"""
Procedural Memory — PostgreSQL
==============================
Manages playbooks, strategies, and activation states.
"""
from __future__ import annotations
import asyncpg
import json
import logging
from datetime import datetime, timezone

log = logging.getLogger("memory.procedural")

class ProceduralMemory:
    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool = None

    async def connect(self):
        self._pool = await asyncpg.create_pool(self._dsn, min_size=2, max_size=10)
        log.info("Procedural memory connected to PostgreSQL pool")

    async def disconnect(self):
        if self._pool:
            await self._pool.close()
            log.info("Procedural memory pool closed")

    async def check_and_activate_playbooks(self, event: dict) -> list[dict]:
        """
        Check if the incoming event matches any playbook triggers.
        If yes, update the playbook's last_used_at timestamp and return the triggered playbooks.
        """
        event_type = event.get("event_type")
        severity = event.get("severity")
        if not event_type:
            return []

        async with self._pool.acquire() as conn:
            # Query playbooks where trigger_event matches
            # and trigger_severity is either NULL or matches the event severity.
            rows = await conn.fetch("""
                SELECT * FROM playbooks
                WHERE trigger_event = $1
                  AND (trigger_severity IS NULL OR trigger_severity = $2)
            """, event_type, severity)
            
            if not rows:
                return []
                
            triggered_playbooks = []
            for r in rows:
                pb = dict(r)
                if isinstance(pb.get("steps"), str):
                    pb["steps"] = json.loads(pb["steps"])
                triggered_playbooks.append(pb)
            
            # Update last_used_at for these playbooks
            pb_ids = [pb["id"] for pb in triggered_playbooks]
            await conn.execute("""
                UPDATE playbooks
                SET last_used_at = NOW()
                WHERE id = ANY($1)
            """, pb_ids)
            
            log.info(f"Triggered {len(pb_ids)} playbook(s) for event {event.get('event_id')}: {pb_ids}")
            return triggered_playbooks

    async def get_all_playbooks(self) -> list[dict]:
        """Get all playbooks stored in the database."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM playbooks ORDER BY created_at DESC")
            res = []
            for r in rows:
                pb = dict(r)
                if isinstance(pb.get("steps"), str):
                    pb["steps"] = json.loads(pb["steps"])
                res.append(pb)
            return res

    async def get_playbook(self, playbook_id: str) -> dict | None:
        """Get a specific playbook by its ID."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM playbooks WHERE id = $1", playbook_id)
            if not row:
                return None
            pb = dict(row)
            if isinstance(pb.get("steps"), str):
                pb["steps"] = json.loads(pb["steps"])
            return pb

    async def create_playbook(self, playbook: dict):
        """Create a new playbook in the database."""
        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO playbooks (id, name, trigger_event, trigger_severity, steps, confidence)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (id) DO UPDATE
                SET name = EXCLUDED.name,
                    trigger_event = EXCLUDED.trigger_event,
                    trigger_severity = EXCLUDED.trigger_severity,
                    steps = EXCLUDED.steps,
                    confidence = EXCLUDED.confidence
            """,
                playbook["id"],
                playbook["name"],
                playbook["trigger_event"],
                playbook.get("trigger_severity"),
                json.dumps(playbook["steps"]) if not isinstance(playbook["steps"], str) else playbook["steps"],
                playbook.get("confidence", 0.5)
            )
