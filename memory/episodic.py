"""
Episodic Memory — TimescaleDB
==============================
Stores every CognitiveEvent. Append-only. Forever.
"""
from __future__ import annotations
import asyncpg
import json
from datetime import datetime, timezone
from dateutil.parser import parse as parse_date

class EpisodicMemory:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool = None

    async def connect(self):
        self._pool = await asyncpg.create_pool(self._dsn, min_size=2, max_size=10)

    async def disconnect(self):
        if self._pool:
            await self._pool.close()

    async def store(self, event: dict):
        """Insert a CognitiveEvent. Idempotent — ON CONFLICT DO NOTHING."""
        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO cognitive_events
                  (event_id, timestamp, ingested_at, source_type, source_id,
                   event_type, severity, payload, entity_refs, confidence, tags, agent_id)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                ON CONFLICT (event_id, timestamp) DO NOTHING
            """,
                event["event_id"],
                _parse_ts(event.get("timestamp")),
                _parse_ts(event.get("ingested_at")),
                event.get("source_type"),
                event.get("source_id"),
                event.get("event_type"),
                event.get("severity"),
                json.dumps(event.get("payload", {})),
                event.get("entity_refs", []),
                event.get("confidence"),
                event.get("tags", []),
                event.get("agent_id"),
            )

    # ── READ METHODS ──────────────────────────────────────────────

    async def query_by_entity(
        self, entity_id: str, hours: int = 24, limit: int = 100
    ) -> list[dict]:
        """All events mentioning an entity in the last N hours."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM cognitive_events
                WHERE $1 = ANY(entity_refs)
                  AND timestamp > NOW() - ($2 || ' hours')::interval
                ORDER BY timestamp DESC LIMIT $3
            """, entity_id, str(hours), limit)
            return [dict(r) for r in rows]

    async def query_by_type(
        self, event_type: str, hours: int = 24, limit: int = 50
    ) -> list[dict]:
        """All events of a specific type in the last N hours."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM cognitive_events
                WHERE event_type = $1
                  AND timestamp > NOW() - ($2 || ' hours')::interval
                ORDER BY timestamp DESC LIMIT $3
            """, event_type, str(hours), limit)
            return [dict(r) for r in rows]

    async def query_by_severity(
        self, severity: str, hours: int = 6, limit: int = 50
    ) -> list[dict]:
        """All events of a specific severity in the last N hours."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM cognitive_events
                WHERE severity = $1
                  AND timestamp > NOW() - ($2 || ' hours')::interval
                ORDER BY timestamp DESC LIMIT $3
            """, severity, str(hours), limit)
            return [dict(r) for r in rows]

    async def count_by_type_today(self) -> list[dict]:
        """Event frequency breakdown for today."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT event_type, severity, COUNT(*) as cnt
                FROM cognitive_events
                WHERE timestamp > NOW() - INTERVAL '24 hours'
                GROUP BY event_type, severity
                ORDER BY cnt DESC
            """)
            return [dict(r) for r in rows]

def _parse_ts(ts_str) -> datetime:
    if not ts_str:
        return datetime.now(timezone.utc)
    if isinstance(ts_str, datetime):
        return ts_str
    dt = parse_date(ts_str)
    return dt.replace(tzinfo=timezone.utc) if not dt.tzinfo else dt
