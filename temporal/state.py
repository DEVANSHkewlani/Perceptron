"""
TemporalStateProfile — per-entity temporal state.
Computed on demand from TimescaleDB + Redis window data.
Read by the World Model to construct its entity registry.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

import asyncpg
import numpy as np
import redis.asyncio as aioredis


@dataclass
class EntityTemporalState:
    entity_id:            str
    event_type:           str
    current_value:        float | None
    rate_of_change:       float | None   # value delta per minute
    trend_direction:      Literal["rising", "falling", "stable"]
    deviation_from_baseline: float | None  # z-score vs baseline
    time_since_last_s:    float | None   # seconds since last event
    window_count:         int             # events in last 10 minutes
    computed_at:          datetime = field(default_factory=datetime.utcnow)


class TemporalStateManager:
    def __init__(self, postgres_dsn: str, redis_url: str):
        self.dsn = postgres_dsn
        self._redis_url = redis_url
        self._pool: asyncpg.Pool | None = None
        self._redis: aioredis.Redis | None = None

    async def connect(self):
        self._pool = await asyncpg.create_pool(self.dsn, min_size=2, max_size=6)
        self._redis = await aioredis.from_url(self._redis_url)

    async def disconnect(self):
        if self._pool: await self._pool.close()
        if self._redis: await self._redis.aclose()

    async def get_state(
        self, entity_id: str, event_type: str
    ) -> EntityTemporalState:
        """Compute and return the current temporal state for an entity+metric."""

        # Fetch last 60 minutes of observations
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT time, metric_value
                FROM metric_observations
                WHERE entity_id = $1 AND event_type = $2
                  AND time >= now() - INTERVAL '60 minutes'
                  AND metric_value IS NOT NULL
                ORDER BY time ASC
            """, entity_id, event_type)

        if not rows:
            return EntityTemporalState(
                entity_id=entity_id, event_type=event_type,
                current_value=None, rate_of_change=None,
                trend_direction="stable", deviation_from_baseline=None,
                time_since_last_s=None, window_count=0,
            )

        values = np.array([r["metric_value"] for r in rows if r["metric_value"] is not None])
        times  = [r["time"] for r in rows]

        if len(values) == 0:
            return EntityTemporalState(
                entity_id=entity_id, event_type=event_type,
                current_value=None, rate_of_change=None,
                trend_direction="stable", deviation_from_baseline=None,
                time_since_last_s=None, window_count=0,
            )

        current = float(values[-1])
        mean    = values.mean()
        std     = values.std()

        # Rate of change: delta over last 5 values / 5 minutes
        roc = None
        if len(values) >= 2:
            roc = float((values[-1] - values[-2]))

        # Trend: slope of last 10 points
        trend = "stable"
        if len(values) >= 5:
            slope = np.polyfit(np.arange(len(values[-10:])), values[-10:], 1)[0]
            if   slope >  0.01 * mean: trend = "rising"
            elif slope < -0.01 * mean: trend = "falling"

        # Deviation from baseline
        deviation = (current - mean) / std if std > 0 else 0.0

        # Time since last event
        last_time = times[-1]
        now = datetime.now(last_time.tzinfo)
        time_since = (now - last_time).total_seconds()

        # Window count from Redis
        window_key = f"tw:{entity_id}:{event_type}"
        window_count = await self._redis.zcard(window_key)

        return EntityTemporalState(
            entity_id=entity_id,
            event_type=event_type,
            current_value=current,
            rate_of_change=roc,
            trend_direction=trend,
            deviation_from_baseline=round(deviation, 3),
            time_since_last_s=round(time_since, 1),
            window_count=int(window_count),
        )
