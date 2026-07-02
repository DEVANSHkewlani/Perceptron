"""
Semantic Memory — Neo4j Knowledge Graph
========================================
Every entity_ref becomes a graph node.
Every event connecting two refs strengthens their edge.
"""
from __future__ import annotations
from neo4j import AsyncGraphDatabase

class SemanticMemory:
    def __init__(self, uri: str, user: str, password: str):
        self._uri = uri
        self._auth = (user, password)
        self._driver = None

    async def connect(self):
        self._driver = AsyncGraphDatabase.driver(self._uri, auth=self._auth)
        await self._driver.verify_connectivity()
        await self._create_indexes()

    async def disconnect(self):
        if self._driver:
            await self._driver.close()

    async def _create_indexes(self):
        async with self._driver.session() as s:
            await s.run("CREATE INDEX entity_id IF NOT EXISTS FOR (e:Entity) ON (e.id)")
            await s.run("CREATE INDEX entity_type IF NOT EXISTS FOR (e:Entity) ON (e.entity_type)")

    async def store(self, event: dict):
        """Create/update entity nodes and strengthen relationship edges."""
        refs = event.get("entity_refs", [])
        event_type = event["event_type"]
        ts = event.get("timestamp", "")
        severity = event.get("severity", "info")

        if not refs:
            return

        async with self._driver.session() as s:
            # Upsert each entity node
            for ref in refs:
                entity_type = ref.split(":")[0] if ":" in ref else "unknown"
                await s.run("""
                    MERGE (e:Entity {id: $id})
                    SET e.entity_type  = $etype,
                        e.last_seen    = $ts,
                        e.last_event   = $event_type,
                        e.last_severity= $severity
                """, id=ref, etype=entity_type,
                    ts=ts, event_type=event_type, severity=severity)

            # Create/strengthen edges between every pair of refs
            ALLOWED_RELATIONSHIPS = {
                "DEPENDS_ON", "COMMUNICATES_WITH", "TRIGGERS", 
                "HISTORICALLY_CORRELATED", "OWNS", "EXECUTES", 
                "CONNECTED_VIA", "PART_OF", "SIMILAR_TO", "RESOLVES"
            }
            for i in range(len(refs)):
                for j in range(i + 1, len(refs)):
                    src_id, rel_type, tgt_id = _get_relationship_edge(refs[i], refs[j], event_type)
                    if rel_type not in ALLOWED_RELATIONSHIPS:
                        rel_type = "CONNECTED_VIA"
                    
                    await s.run(f"""
                        MATCH (a:Entity {{id: $id_a}})
                        MATCH (b:Entity {{id: $id_b}})
                        MERGE (a)-[r:{rel_type}]->(b)
                        SET r.last_event  = $event_type,
                            r.last_seen   = $ts,
                            r.event_count = coalesce(r.event_count, 0) + 1,
                            r.weight      = coalesce(r.weight, 0.0) + 0.1
                    """, id_a=src_id, id_b=tgt_id,
                        event_type=event_type, ts=ts)

    # ── READ / QUERY METHODS ──────────────────────────────────────

    async def get_neighbors(self, entity_id: str, depth: int = 1) -> list[dict]:
        """Get all entities connected to this one."""
        # Sanitize depth to prevent Cypher injection
        d = max(1, min(int(depth), 5))
        async with self._driver.session() as s:
            result = await s.run(f"""
                MATCH (a:Entity {{id: $id}})-[r*1..{d}]-(b:Entity)
                RETURN DISTINCT b.id AS id, b.entity_type AS etype,
                       b.last_event AS last_event, b.last_seen AS last_seen
                LIMIT 50
            """, id=entity_id)
            return [dict(r) async for r in result]

    async def get_blast_radius(self, entity_id: str) -> list[dict]:
        """All entities that could be affected if this entity fails."""
        async with self._driver.session() as s:
            # Query 1: entities that depend on the target node (inbound DEPENDS_ON)
            res1 = await s.run("""
                MATCH (dependent:Entity)-[:DEPENDS_ON*1..3]->(target:Entity {id: $id})
                RETURN DISTINCT
                    dependent.id          AS id,
                    dependent.entity_type AS etype,
                    dependent.status      AS status
                ORDER BY etype
            """, id=entity_id)
            rows1 = [dict(r) async for r in res1]

            # Query 2: direct communicators that may be impacted (inbound COMMUNICATES_WITH)
            res2 = await s.run("""
                MATCH (peer:Entity)-[:COMMUNICATES_WITH]->(target:Entity {id: $id})
                RETURN DISTINCT
                    peer.id          AS id,
                    peer.entity_type AS etype,
                    peer.status      AS status
            """, id=entity_id)
            rows2 = [dict(r) async for r in res2]

            # Combine and deduplicate by id
            seen = set()
            combined = []
            for r in rows1 + rows2:
                if r["id"] not in seen:
                    seen.add(r["id"])
                    combined.append({
                        "id": r["id"],
                        "etype": r["etype"],
                        "status": r.get("status"),
                        "affected_id": r["id"],
                        "entity_type": r["etype"]
                    })
            return combined

    async def get_entity_count(self) -> int:
        async with self._driver.session() as s:
            r = await s.run("MATCH (e:Entity) RETURN count(e) AS cnt")
            rec = await r.single()
            return rec["cnt"] if rec else 0


def _get_relationship_edge(id_a: str, id_b: str, event_type: str) -> tuple[str, str, str]:
    """
    Infers (source_node_id, relationship_type, target_node_id) to represent
    a directed relationship: source -> target.
    """
    type_a = id_a.split(":")[0] if ":" in id_a else "unknown"
    type_b = id_b.split(":")[0] if ":" in id_b else "unknown"
    
    # Check for HISTORICALLY_CORRELATED
    if "correlation" in event_type or event_type == "temporal_correlation":
        return id_a, "HISTORICALLY_CORRELATED", id_b
        
    # Check for TRIGGERS
    if "trigger" in event_type or event_type in ("triggers", "action_triggered", "plan_triggered"):
        return id_a, "TRIGGERS", id_b
        
    # Check for EXECUTES: Agent executes Task
    if type_a == "agent" and type_b == "task":
        return id_a, "EXECUTES", id_b
    if type_b == "agent" and type_a == "task":
        return id_b, "EXECUTES", id_a
        
    # Check for OWNS: User owns Service / Database
    if type_a in ("usr", "user") and type_b in ("svc", "db"):
        return id_a, "OWNS", id_b
    if type_b in ("usr", "user") and type_a in ("svc", "db"):
        return id_b, "OWNS", id_a
        
    # Check for DEPENDS_ON: Service depends on Database / Service
    if type_a == "svc" and type_b == "db":
        return id_a, "DEPENDS_ON", id_b
    if type_b == "svc" and type_a == "db":
        return id_b, "DEPENDS_ON", id_a
        
    # Check for COMMUNICATES_WITH: Service communicates with Service
    if type_a == "svc" and type_b == "svc":
        return id_a, "COMMUNICATES_WITH", id_b
        
    # Default fallback
    return id_a, "CONNECTED_VIA", id_b

