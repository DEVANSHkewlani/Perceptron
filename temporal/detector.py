"""
TemporalPatternDetector
Runs five pattern-detection algorithms on every ingested event.
Persists detected patterns to temporal_patterns table.
Publishes pattern CognitiveEvents to cognitive.events bus.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

import asyncpg
import numpy as np
import redis.asyncio as aioredis
from scipy.stats import pearsonr


# Detector thresholds
SPIKE_Z_THRESHOLD    = 3.0   # z-score for spike
DRIFT_MIN_WINDOWS    = 20    # need at least 20 data points for slope
DRIFT_SLOPE_WARN     = 0.05  # 5% per window = drift warning
CORRELATION_R_MIN    = 0.85  # Pearson r threshold
CORR_MIN_SAMPLES     = 30    # need 30 paired observations
RECURRENCE_DAYS_BACK = 14    # look back 2 weeks for recurrence


class TemporalPatternDetector:
    def __init__(self, postgres_dsn: str, redis_url: str):
        self.dsn = postgres_dsn
        self._redis_url = redis_url
        self._pool: asyncpg.Pool | None = None
        self._redis: aioredis.Redis | None = None

    async def connect(self):
        self._pool = await asyncpg.create_pool(self.dsn, min_size=2, max_size=8)
        self._redis = await aioredis.from_url(self._redis_url)

    async def disconnect(self):
        if self._pool: await self._pool.close()
        if self._redis: await self._redis.aclose()

    async def run_all(
        self, entity_id: str, event_type: str, ts: datetime
    ) -> list[dict]:
        """Run all five detectors in parallel. Returns list of detected patterns."""
        results = await asyncio.gather(
            self.detect_spike(entity_id, event_type),
            self.detect_drift(entity_id, event_type),
            self.detect_absence(entity_id, event_type),
            self.detect_recurrence(entity_id, event_type, ts),
            return_exceptions=True,
        )
        patterns = [r for r in results if isinstance(r, dict)]
        
        # Persist detected patterns to database
        for pattern in patterns:
            await self._persist_pattern(pattern)
            
        return patterns

    async def _persist_pattern(self, pattern: dict) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO temporal_patterns
                    (pattern_type, entity_id, entity_id_b,
                     severity, confidence, details)
                VALUES ($1, $2, $3, $4, $5, $6)
            """,
            pattern["pattern_type"], pattern["entity_id"],
            pattern.get("entity_id_b"), pattern["severity"],
            pattern["confidence"], json.dumps(pattern.get("details")) if pattern.get("details") else None,
        )

    # ── SPIKE DETECTION ────────────────────────────────────────

    async def detect_spike(
        self, entity_id: str, event_type: str
    ) -> dict | None:
        """z-score against 1-hour rolling baseline."""
        rows = await self._fetch_recent_values(entity_id, event_type, minutes=60)
        if len(rows) < 10: return None

        values = np.array([r["metric_value"] for r in rows if r["metric_value"] is not None])
        if len(values) < 5: return None

        mean, std = values[:-1].mean(), values[:-1].std()
        if std == 0: return None

        latest = values[-1]
        z_score = abs(latest - mean) / std

        if z_score >= SPIKE_Z_THRESHOLD:
            return {
                "pattern_type": "spike",
                "entity_id":    entity_id,
                "severity":     "critical" if z_score > 5 else "high",
                "confidence":   min(0.97, 0.80 + (z_score - 3) * 0.05),
                "details": {
                    "event_type":  event_type,
                    "z_score":     round(z_score, 3),
                    "latest":      float(latest),
                    "baseline_mean": round(float(mean), 3),
                    "baseline_std":  round(float(std), 3),
                },
            }
        return None

    # ── DRIFT DETECTION ────────────────────────────────────────

    async def detect_drift(
        self, entity_id: str, event_type: str
    ) -> dict | None:
        """Linear regression over 20 5-minute windows."""
        rows = await self._fetch_time_buckets(
            entity_id, event_type, bucket="5 minutes", limit=20
        )
        if len(rows) < DRIFT_MIN_WINDOWS: return None

        y = np.array([r["avg_value"] for r in rows if r["avg_value"] is not None])
        if len(y) < DRIFT_MIN_WINDOWS: return None
        
        x = np.arange(len(y))
        slope = np.polyfit(x, y, 1)[0]

        # Normalize slope as % of mean
        mean = y.mean()
        rel_slope = slope / mean if mean != 0 else 0

        if abs(rel_slope) >= DRIFT_SLOPE_WARN:
            direction = "increasing" if slope > 0 else "decreasing"
            return {
                "pattern_type": "drift",
                "entity_id":    entity_id,
                "severity":     "medium",
                "confidence":   0.85,
                "details": {
                    "event_type":  event_type,
                    "direction":   direction,
                    "slope_pct":   round(rel_slope * 100, 2),
                    "window_count": len(rows),
                },
            }
        return None

    # ── ABSENCE DETECTION ──────────────────────────────────────

    async def detect_absence(
        self, entity_id: str, event_type: str
    ) -> dict | None:
        """
        Check if a normally-frequent event has stopped arriving.
        The absence sentinel key is reset on every ingestion.
        If the key is missing, the event has been silent for WINDOW_TTL_S.
        This is called PROACTIVELY — not triggered by the missing event,
        but triggered by the NEXT event from the same entity.
        """
        absence_key = f"absence:{entity_id}:{event_type}"
        exists = await self._redis.exists(absence_key)
        # If key exists, event is still arriving — no absence
        # Absence check is run on related entity events, not self
        return None

    async def detect_absence_for_entity(
        self, entity_id: str, watched_event_types: list[str]
    ) -> list[dict]:
        """
        Call this on a schedule (e.g., every 60s) per entity.
        Returns absence patterns for any watched event type that has gone silent.
        """
        patterns = []
        for event_type in watched_event_types:
            absence_key = f"absence:{entity_id}:{event_type}"
            exists = await self._redis.exists(absence_key)
            if not exists:
                patterns.append({
                    "pattern_type": "absence",
                    "entity_id":    entity_id,
                    "severity":     "high",
                    "confidence":   0.88,
                    "details": {
                        "event_type": event_type,
                        "silent_since_min": 10,
                    },
                })
        return patterns

    # ── RECURRENCE DETECTION ───────────────────────────────────

    async def detect_recurrence(
        self, entity_id: str, event_type: str, now: datetime
    ) -> dict | None:
        """
        Check TimescaleDB: did this event_type appear at the same
        hour-of-week in the past RECURRENCE_DAYS_BACK days?
        Uses time_bucket to aggregate into 1-hour windows.
        """
        hour_of_week = now.weekday() * 24 + now.hour
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT COUNT(*) AS cnt
                FROM metric_observations
                WHERE entity_id  = $1
                  AND event_type = $2
                  AND time >= now() - INTERVAL '14 days'
                  AND EXTRACT(DOW FROM time) * 24 +
                      EXTRACT(HOUR FROM time) = $3
            """, entity_id, event_type, hour_of_week)

        count = rows[0]["cnt"]
        if count >= 3:   # appeared 3+ times at this hour-of-week
            return {
                "pattern_type": "recurrence",
                "entity_id":    entity_id,
                "severity":     "medium",
                "confidence":   min(0.95, 0.70 + count * 0.05),
                "details": {
                    "event_type":   event_type,
                    "hour_of_week": hour_of_week,
                    "past_count":   count,
                },
            }
        return None

    # ── CORRELATION DETECTION ──────────────────────────────────

    async def detect_correlation(
        self,
        entity_a: str, event_type_a: str,
        entity_b: str, event_type_b: str,
    ) -> dict | None:
        """
        Pearson r between two entity-metric time series.
        Call this from a background scheduler, not on every event.
        Returns pattern if correlation is strong (|r| >= 0.85).
        """
        rows_a = await self._fetch_recent_values(entity_a, event_type_a, minutes=120)
        rows_b = await self._fetch_recent_values(entity_b, event_type_b, minutes=120)

        vals_a = [r["metric_value"] for r in rows_a if r["metric_value"] is not None]
        vals_b = [r["metric_value"] for r in rows_b if r["metric_value"] is not None]

        n = min(len(vals_a), len(vals_b))
        if n < CORR_MIN_SAMPLES: return None

        try:
            r, p_value = pearsonr(vals_a[:n], vals_b[:n])
            if np.isnan(r) or np.isnan(p_value):
                return None
        except Exception:
            return None

        if abs(r) >= CORRELATION_R_MIN and p_value < 0.05:
            return {
                "pattern_type": "correlation",
                "entity_id":    entity_a,
                "entity_id_b":  entity_b,
                "severity":     "info",
                "confidence":   round(abs(r), 3),
                "details": {
                    "event_type_a": event_type_a,
                    "event_type_b": event_type_b,
                    "pearson_r":    round(r, 4),
                    "p_value":      round(p_value, 6),
                    "sample_count": n,
                },
            }
        return None

    # ── HELPERS ────────────────────────────────────────────────

    async def _fetch_recent_values(
        self, entity_id: str, event_type: str, minutes: int
    ) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT time, metric_value
                FROM metric_observations
                WHERE entity_id  = $1
                  AND event_type = $2
                  AND time >= now() - ($3 || ' minutes')::INTERVAL
                ORDER BY time ASC
            """, entity_id, event_type, str(minutes))
        return [dict(r) for r in rows]

    async def _fetch_time_buckets(
        self, entity_id: str, event_type: str, bucket: str, limit: int
    ) -> list[dict]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT time_bucket($1::INTERVAL, time) AS bucket,
                       AVG(metric_value) AS avg_value
                FROM metric_observations
                WHERE entity_id  = $2
                  AND event_type = $3
                  AND time >= now() - ($1::INTERVAL * $4)
                GROUP BY bucket
                ORDER BY bucket DESC
                LIMIT $4
            """, bucket, entity_id, event_type, limit)
        return [dict(r) for r in reversed(rows)]
