"""
AnomalyRegistry — lifecycle manager for active anomalies.
An anomaly is opened when a non-info event arrives.
It is resolved when an event_type ending in _resolved/_restored arrives
for the same entity_id, OR when the anomaly has not seen a new event
for STALE_THRESHOLD_S seconds (auto-heal assumption).
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Literal

import redis.asyncio as aioredis

STALE_THRESHOLD_S = 600   # 10 minutes: auto-resolve if no update
SEVERITY_ORDER = ["critical", "high", "medium", "low", "info"]


@dataclass
class Anomaly:
    anomaly_id:   str
    entity_id:    str
    event_type:   str
    severity:     str
    confidence:   float
    opened_at:    str
    last_seen_at: str
    resolved_at:  str | None
    status:       Literal["open", "resolved"]
    details:      dict = field(default_factory=dict)


class AnomalyRegistry:
    def __init__(self, redis_url: str):
        self._redis_url = redis_url
        self._redis: aioredis.Redis | None = None
        self._open: dict[str, Anomaly] = {}   # keyed by entity_id+event_type
        self._REDIS_KEY = "wm:anomalies"

    async def connect(self):
        self._redis = await aioredis.from_url(self._redis_url, decode_responses=True)
        await self._load_from_redis()

    async def disconnect(self):
        if self._redis: await self._redis.aclose()

    async def _load_from_redis(self):
        raw = await self._redis.hgetall(self._REDIS_KEY)
        for key, blob in raw.items():
            try:
                d = json.loads(blob)
                a = Anomaly(**d)
                if a.status == "open":
                    self._open[key] = a
            except Exception:
                pass

    async def process_event(self, event: dict) -> None:
        severity   = event.get("severity", "info")
        event_type = event.get("event_type", "")
        ts         = event.get("timestamp", datetime.now(timezone.utc).isoformat())
        entity_refs = event.get("entity_refs", [])
        confidence = event.get("confidence", 0.8)

        # Resolution event — close all open anomalies for this entity
        if event_type.endswith("_resolved") or event_type.endswith("_restored"):
            for entity_id in entity_refs:
                await self._resolve_entity(entity_id, ts)
            return

        # Non-info events open or refresh an anomaly
        if severity == "info":
            return

        for entity_id in entity_refs:
            key = f"{entity_id}:{event_type}"
            if key in self._open:
                # Refresh
                a = self._open[key]
                a.last_seen_at = ts
                # Escalate severity if new event is worse
                if SEVERITY_ORDER.index(severity) < SEVERITY_ORDER.index(a.severity):
                    a.severity = severity
                a.confidence = confidence
            else:
                # Open new anomaly
                a = Anomaly(
                    anomaly_id   = f"anm_{uuid.uuid4().hex[:8]}",
                    entity_id    = entity_id,
                    event_type   = event_type,
                    severity     = severity,
                    confidence   = confidence,
                    opened_at    = ts,
                    last_seen_at = ts,
                    resolved_at  = None,
                    status       = "open",
                    details      = event.get("payload", {}),
                )
                self._open[key] = a
            await self._persist(key, self._open[key])

    async def expire_stale(self) -> None:
        """Call on a 60-second schedule. Auto-resolve anomalies with no recent update."""
        now = datetime.now(timezone.utc)
        stale_keys = []
        for key, a in self._open.items():
            last = datetime.fromisoformat(a.last_seen_at.replace("Z", "+00:00"))
            if (now - last).total_seconds() > STALE_THRESHOLD_S:
                stale_keys.append(key)
        for key in stale_keys:
            a = self._open.pop(key)
            a.status = "resolved"
            a.resolved_at = now.isoformat()
            await self._persist(key, a)

    async def _resolve_entity(self, entity_id: str, ts: str) -> None:
        keys_to_close = [k for k in self._open if k.startswith(entity_id)]
        for key in keys_to_close:
            a = self._open.pop(key)
            a.status = "resolved"; a.resolved_at = ts
            await self._persist(key, a)

    async def _persist(self, key: str, a: Anomaly) -> None:
        await self._redis.hset(self._REDIS_KEY, key, json.dumps(asdict(a)))

    def get_open(self) -> list[Anomaly]:
        return sorted(
            self._open.values(),
            key=lambda a: SEVERITY_ORDER.index(a.severity),
        )

    def count_by_severity(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for a in self._open.values():
            counts[a.severity] = counts.get(a.severity, 0) + 1
        return counts
