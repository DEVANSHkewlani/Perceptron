"""
EntityRegistry
In-memory dict of entity states, backed by Redis for persistence.
Updated on every CognitiveEvent that includes entity_refs.
Read by the WorldModel.get_entity_state() and situation assessor.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis


SEVERITY_SCORE = {"critical": 5, "high": 4, "medium": 3, "low": 2, "info": 1}


@dataclass
class EntityState:
    entity_id:         str
    entity_type:       str      # svc | db | queue | sensor | usr | metric …
    health_status:     str      # healthy | degraded | critical | unknown
    last_seen:         str      # ISO timestamp
    last_event_type:   str      # most recent event_type observed
    last_severity:     str
    severity_score:    int      # 1–5 for sorting
    confidence:        float
    # Temporal fields — written by temporal state poll
    current_value:     float | None = None
    rate_of_change:    float | None = None
    trend_direction:   str = "stable"
    deviation_z:       float | None = None
    # Extra metadata — passed through from payload
    meta:              dict = field(default_factory=dict)


class EntityRegistry:
    def __init__(self, redis_url: str):
        self._redis_url = redis_url
        self._redis: aioredis.Redis | None = None
        self._store: dict[str, EntityState] = {}   # hot in-memory cache
        self._REDIS_KEY = "wm:entity_registry"

    async def connect(self):
        self._redis = await aioredis.from_url(self._redis_url, decode_responses=True)
        await self._load_from_redis()

    async def disconnect(self):
        if self._redis:
            await self._redis.aclose()

    async def _load_from_redis(self):
        """Restore in-memory cache from Redis on startup."""
        raw = await self._redis.hgetall(self._REDIS_KEY)
        for entity_id, blob in raw.items():
            try:
                d = json.loads(blob)
                self._store[entity_id] = EntityState(**d)
            except Exception:
                pass

    async def upsert_from_event(self, event: dict) -> None:
        """
        Called on every CognitiveEvent.
        Updates each entity_ref listed in the event.
        """
        severity    = event.get("severity", "info")
        event_type  = event.get("event_type", "")
        confidence  = event.get("confidence", 0.8)
        ts          = event.get("timestamp", datetime.now(timezone.utc).isoformat())
        entity_refs = event.get("entity_refs", [])

        for entity_id in entity_refs:
            entity_type = entity_id.split(":")[0] if ":" in entity_id else "unknown"
            health = self._severity_to_health(severity, event_type)

            existing = self._store.get(entity_id)
            # Only downgrade health, never silently upgrade without a _resolved event
            if existing and existing.severity_score > SEVERITY_SCORE.get(severity, 1):
                if not event_type.endswith("_resolved") and not event_type.endswith("_restored"):
                    health = existing.health_status

            state = EntityState(
                entity_id       = entity_id,
                entity_type     = entity_type,
                health_status   = health,
                last_seen       = ts,
                last_event_type = event_type,
                last_severity   = severity,
                severity_score  = SEVERITY_SCORE.get(severity, 1),
                confidence      = confidence,
                current_value   = existing.current_value if existing else None,
                rate_of_change  = existing.rate_of_change if existing else None,
                trend_direction = existing.trend_direction if existing else "stable",
                deviation_z     = existing.deviation_z if existing else None,
                meta            = event.get("payload", {}).copy(),
            )
            self._store[entity_id] = state
            await self._redis.hset(
                self._REDIS_KEY, entity_id, json.dumps(asdict(state))
            )

    async def update_temporal_state(self, entity_id: str, temporal: dict) -> None:
        """Write temporal fields from TemporalStateManager.get_state()."""
        entity = self._store.get(entity_id)
        if not entity:
            return
        entity.current_value   = temporal.get("current_value")
        entity.rate_of_change  = temporal.get("rate_of_change")
        entity.trend_direction = temporal.get("trend_direction", "stable")
        entity.deviation_z     = temporal.get("deviation_from_baseline")
        await self._redis.hset(
            self._REDIS_KEY, entity_id, json.dumps(asdict(entity))
        )

    def get(self, entity_id: str) -> EntityState | None:
        return self._store.get(entity_id)

    def get_all(self) -> list[EntityState]:
        return list(self._store.values())

    def get_degraded(self) -> list[EntityState]:
        return sorted(
            [e for e in self._store.values() if e.health_status != "healthy"],
            key=lambda e: e.severity_score,
            reverse=True,
        )

    @staticmethod
    def _severity_to_health(severity: str, event_type: str) -> str:
        if event_type.endswith("_resolved") or event_type.endswith("_restored"):
            return "healthy"
        mapping = {"critical": "critical", "high": "degraded",
                   "medium": "degraded", "low": "healthy", "info": "healthy"}
        return mapping.get(severity, "unknown")
