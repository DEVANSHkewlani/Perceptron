"""
Memory Query API
================
Unified HTTP interface over all four memory layers.
The reasoning engine queries this instead of hitting DBs directly.
"""
from __future__ import annotations
import os
import uuid
import asyncio
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from fastapi import FastAPI
from .working  import WorkingMemory
from .episodic import EpisodicMemory, _parse_ts
from .semantic import SemanticMemory
from .vector import VectorMemory
from .procedural import ProceduralMemory

working  = WorkingMemory(os.getenv("REDIS_URL",    "redis://localhost:6379"))
episodic = EpisodicMemory(os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/cognitive"))
semantic = SemanticMemory(
    os.getenv("NEO4J_URI",      "bolt://localhost:7687"),
    os.getenv("NEO4J_USER",     "neo4j"),
    os.getenv("NEO4J_PASSWORD", "password123"),
)
vector = VectorMemory(os.getenv("QDRANT_URL", "http://localhost:6333"))
procedural = ProceduralMemory(os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/cognitive"))

@asynccontextmanager
async def lifespan(app):
    await working.connect()
    await episodic.connect()
    await semantic.connect()
    await vector.connect()
    await procedural.connect()
    yield
    await working.disconnect()
    await episodic.disconnect()
    await semantic.disconnect()
    await vector.disconnect()
    await procedural.disconnect()

app = FastAPI(title="Memory API", lifespan=lifespan)

# ── WORKING MEMORY ────────────────────────────────────────────

@app.get("/memory/working/recent")
async def recent_events(limit: int = 50):
    """Get the N most recent HIGH/CRITICAL events from Redis."""
    return await working.get_recent_events(limit)

@app.get("/memory/working/entity/{entity_id:path}")
async def entity_state(entity_id: str):
    """Current state of a specific entity from Redis."""
    state = await working.get_entity_state(entity_id)
    return state or {"status": "not in working memory"}

@app.get("/memory/working/critical")
async def active_critical():
    """All currently active CRITICAL events."""
    return await working.get_active_critical()

# ── EPISODIC MEMORY ───────────────────────────────────────────

@app.get("/memory/episodic/entity/{entity_id:path}")
async def entity_history(entity_id: str, hours: int = 24):
    """Full event history for an entity."""
    rows = await episodic.query_by_entity(entity_id, hours)
    return {"entity_id": entity_id, "count": len(rows), "events": rows}

@app.get("/memory/episodic/type/{event_type}")
async def events_by_type(event_type: str, hours: int = 24):
    return await episodic.query_by_type(event_type, hours)

@app.get("/memory/episodic/severity/{severity}")
async def events_by_severity(severity: str, hours: int = 6):
    return await episodic.query_by_severity(severity, hours)

@app.get("/memory/episodic/summary")
async def event_summary():
    """Event type frequency for today."""
    return await episodic.count_by_type_today()

# ── SEMANTIC MEMORY ───────────────────────────────────────────

@app.get("/memory/graph/neighbors/{entity_id:path}")
async def graph_neighbors(entity_id: str, depth: int = 1):
    return await semantic.get_neighbors(entity_id, depth)

@app.get("/memory/graph/blast-radius/{entity_id:path}")
async def blast_radius(entity_id: str):
    """What else is affected if this entity has a problem?"""
    return await semantic.get_blast_radius(entity_id)

@app.get("/memory/graph/stats")
async def graph_stats():
    count = await semantic.get_entity_count()
    return {"entity_count": count}

# ── EPISODIC VECTOR SEARCH ────────────────────────────────────

@app.get("/memory/episodic/search")
async def search_episodic(query: str | None = None, q: str | None = None, limit: int = 5):
    """Search episodic memory semantically using local embeddings."""
    search_q = q or query
    if not search_q:
        return {"results": []}
    raw_results = await vector.search(search_q, limit)
    
    # If any returned event is a 'reasoning_completed' event, return its payload
    # so that the ContextBuilder can access 'decision' and 'outcome' at the top-level
    results = []
    for exp in raw_results:
        if isinstance(exp, dict) and exp.get("event_type") == "reasoning_completed" and "payload" in exp:
            payload = exp["payload"]
            if isinstance(payload, dict):
                results.append({**payload, "event_type": "reasoning_completed"})
            else:
                results.append(exp)
        else:
            results.append(exp)
            
    return {"results": results}

@app.get("/memory/episodic/similar/{event_id:path}")
async def similar_events(event_id: str, limit: int = 5):
    """Find similar past events to a given event ID."""
    return await vector.search_similar_events(event_id, limit)

@app.post("/memory/episodic")
async def create_episodic_record(record: dict):
    """Store a decision record in episodic memory (TimescaleDB and Qdrant)."""
    event_id = f"evt_{uuid.uuid4().hex[:12]}"
    
    # Extract entity references from the situation summary
    entity_refs = ["agent:reasoning-engine"]
    situation = record.get("situation_summary") or {}
    if isinstance(situation, dict):
        anomalies = situation.get("ranked_anomalies") or []
        for anomaly in anomalies:
            if isinstance(anomaly, dict):
                for key in ("source_id", "entity_id"):
                    val = anomaly.get(key)
                    if val and isinstance(val, str) and ":" in val:
                        entity_refs.append(val)
    entity_refs = list(set(entity_refs))

    event = {
        "event_id": event_id,
        "timestamp": record.get("created_at") or datetime.now(timezone.utc).isoformat(),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source_type": "agent_event",
        "source_id": record.get("agent_id") or "agent:reasoning-engine",
        "agent_id": record.get("agent_id"),
        "event_type": "reasoning_completed",
        "severity": "info",
        "payload": record,
        "entity_refs": entity_refs,
        "confidence": record.get("decision", {}).get("confidence", 1.0) if isinstance(record.get("decision"), dict) else 1.0,
        "tags": ["reasoning", "decision"]
    }
    
    # Store in TimescaleDB and Qdrant in parallel
    await asyncio.gather(
        episodic.store(event),
        vector.store(event)
    )
    return {"status": "success", "event_id": event_id}

# ── PROCEDURAL PLAYBOOKS ──────────────────────────────────────

@app.get("/memory/procedural/playbooks")
async def list_playbooks_filtered(
    trigger_event:      str | None = None,
    trigger_severity:   str | None = None,
    recommended_action: str | None = None,
    min_success_rate:   float = 0.0,
):
    import json
    async with procedural._pool.acquire() as conn:
        filters = ["success_rate >= $1"]; args = [min_success_rate]
        if trigger_event:
            args.append(trigger_event)
            filters.append(f"trigger_event = ${len(args)}")
        if trigger_severity:
            args.append(trigger_severity)
            filters.append(f"trigger_severity = ${len(args)}")
        if recommended_action:
            args.append(recommended_action)
            filters.append(f"recommended_action = ${len(args)}")
        where = "WHERE " + " AND ".join(filters)
        rows  = await conn.fetch(f"SELECT * FROM playbooks {where} ORDER BY success_rate DESC", *args)
    
    res = []
    for r in rows:
        pb = dict(r)
        if isinstance(pb.get("steps"), str):
            pb["steps"] = json.loads(pb["steps"])
        res.append(pb)
    return res

@app.get("/memory/procedural/playbooks/{playbook_id:path}")
async def get_playbook(playbook_id: str):
    """Get a specific playbook."""
    pb = await procedural.get_playbook(playbook_id)
    if not pb:
        return {"error": "Playbook not found"}
    return pb

@app.post("/memory/procedural/playbooks")
async def create_playbook(playbook: dict):
    """Create or update a playbook."""
    await procedural.create_playbook(playbook)
    return {"status": "success", "playbook_id": playbook.get("id")}

@app.patch("/memory/procedural/playbooks/{playbook_id}")
async def update_playbook_stats(playbook_id: str, patch: dict):
    """Updates success/failure stats for a playbook."""
    async with procedural._pool.acquire() as conn:
        await conn.execute("""
            UPDATE playbooks
            SET success_count = $1,
                failure_count = $2,
                success_rate  = $3,
                updated_at    = now()
            WHERE id = $4
        """,
        patch["success_count"],
        patch["failure_count"],
        patch["success_rate"],
        playbook_id,
        )
    return {"updated": playbook_id}

# ── EPISODIC PATCH ───────────────────────────────────────────

@app.patch("/memory/episodic/by-plan/{plan_id}")
async def patch_episodic_outcome(plan_id: str, patch: dict):
    """
    Updates the episodic record created by ReasoningEngine with outcome data.
    """
    import json
    verified_at_str = patch.get("verified_at")
    verified_at = _parse_ts(verified_at_str) if verified_at_str else None
    
    async with episodic._pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE cognitive_events
            SET
              outcome          = $1::text,
              verified_at      = $2::timestamptz,
              anomalies_before = $3::integer,
              anomalies_after  = $4::integer,
              updated_at       = now(),
              payload          = payload || $5::jsonb
            WHERE payload->>'plan_id' = $6
              AND (outcome IS NULL OR outcome = 'pending')
            RETURNING event_id, timestamp, ingested_at, source_type, source_id,
                      event_type, severity, payload, entity_refs, confidence, tags, agent_id,
                      outcome, verified_at, anomalies_before, anomalies_after
        """,
        patch.get("outcome"),
        verified_at,
        patch.get("anomalies_before"),
        patch.get("anomalies_after"),
        json.dumps({k: v for k, v in patch.items()
                    if k not in ("outcome", "verified_at", "anomalies_before", "anomalies_after")}),
        plan_id,
        )

    rows_affected = 0
    if row:
        rows_affected = 1
        updated_event = dict(row)
        # Convert datetime objects to ISO strings for JSON serialization in Qdrant
        for k in ("timestamp", "ingested_at", "verified_at"):
            if updated_event.get(k) and isinstance(updated_event[k], datetime):
                updated_event[k] = updated_event[k].isoformat()
        if isinstance(updated_event.get("payload"), str):
            try:
                updated_event["payload"] = json.loads(updated_event["payload"])
            except Exception:
                pass
        
        # Ensure payload carries the correct outcome for ContextBuilder
        if "payload" in updated_event and isinstance(updated_event["payload"], dict):
            updated_event["payload"]["outcome"] = updated_event["outcome"]
            
        # Write to Qdrant (vector memory)
        await vector.store(updated_event)
        
    return {"patched": plan_id, "rows": rows_affected}


@app.patch("/memory/episodic/by-decision/{decision_id}")
async def patch_episodic_by_decision(decision_id: str, patch: dict):
    """
    Links a plan_id (or other metadata) to the episodic record created by ReasoningEngine.
    """
    import json
    async with episodic._pool.acquire() as conn:
        row = await conn.fetchrow("""
            UPDATE cognitive_events
            SET
              payload          = payload || $1::jsonb,
              updated_at       = now()
            WHERE payload->'decision'->>'decision_id' = $2
            RETURNING event_id, timestamp, ingested_at, source_type, source_id,
                      event_type, severity, payload, entity_refs, confidence, tags, agent_id,
                      outcome, verified_at, anomalies_before, anomalies_after
        """,
        json.dumps(patch),
        decision_id,
        )

    rows_affected = 0
    if row:
        rows_affected = 1
        updated_event = dict(row)
        # Convert datetime objects to ISO strings for JSON serialization in Qdrant
        for k in ("timestamp", "ingested_at", "verified_at"):
            if updated_event.get(k) and isinstance(updated_event[k], datetime):
                updated_event[k] = updated_event[k].isoformat()
        if isinstance(updated_event.get("payload"), str):
            try:
                updated_event["payload"] = json.loads(updated_event["payload"])
            except Exception:
                pass
        
        # Ensure payload carries the correct outcome if it changed
        if "payload" in updated_event and isinstance(updated_event["payload"], dict):
            updated_event["payload"]["outcome"] = updated_event["outcome"]
            
        # Write to Qdrant (vector memory)
        await vector.store(updated_event)
        
    return {"patched_decision": decision_id, "rows": rows_affected}

@app.get("/memory/metrics/{entity_id:path}")
async def get_metric_history(entity_id: str, event_type: str, limit: int = 30):
    """Fetch recent metric observations from TimescaleDB."""
    async with episodic._pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT time, metric_value
            FROM metric_observations
            WHERE entity_id = $1 AND event_type = $2
            ORDER BY time DESC
            LIMIT $3
        """, entity_id, event_type, limit)
        return [{"time": r["time"].isoformat(), "value": r["metric_value"]} for r in reversed(rows)]

# ── HEALTH ────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/memory/working/flush")
async def flush_working_memory():
    """Prune expired event IDs from the working memory sorted set."""
    flushed = await working.flush_expired()
    return {"status": "success", "flushed": flushed}

@app.get("/memory/working/keys")
async def get_working_keys():
    """Fetch list of all raw keys and their metadata from Redis."""
    return await working.get_all_keys()

@app.get("/memory/episodic/recent")
async def episodic_recent(limit: int = 50):
    """Fetch the N most recent events from episodic memory (TimescaleDB)."""
    async with episodic._pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT event_id, timestamp, source_type, source_id, event_type, severity, confidence
            FROM cognitive_events
            ORDER BY timestamp DESC
            LIMIT $1
        """, limit)
        return [dict(r) for r in rows]

@app.get("/memory/stats")
async def get_all_memory_stats():
    """Return keys counts from working, episodic, semantic, and procedural memories."""
    redis_keys = 0
    try:
        if working._r:
            redis_keys = await working._r.dbsize()
    except Exception as e:
        print(f"Error getting working memory dbsize: {e}")

    timescale_records = 0
    try:
        if episodic._pool:
            async with episodic._pool.acquire() as conn:
                timescale_records = await conn.fetchval("SELECT COUNT(*) FROM cognitive_events")
    except Exception as e:
        print(f"Error getting episodic count: {e}")

    neo4j_nodes = 0
    try:
        neo4j_nodes = await semantic.get_entity_count()
    except Exception as e:
        print(f"Error getting semantic count: {e}")

    qdrant_playbooks = 0
    try:
        if procedural._pool:
            async with procedural._pool.acquire() as conn:
                qdrant_playbooks = await conn.fetchval("SELECT COUNT(*) FROM playbooks")
    except Exception as e:
        print(f"Error getting procedural count: {e}")

    return {
        "redis_keys": redis_keys,
        "timescale_records": timescale_records,
        "neo4j_nodes": neo4j_nodes,
        "qdrant_playbooks": qdrant_playbooks
    }


