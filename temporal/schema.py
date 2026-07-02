"""
TimescaleDB Schema for Temporal Engine
Hypertables: metric_observations, temporal_patterns, temporal_baselines
Run once on startup via TemporalSchemaManager.initialize()
"""
from __future__ import annotations
import asyncpg

TIMESCALE_EXTENSION = """
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
"""

# Raw metric observations — one row per CognitiveEvent with a numeric value
METRIC_OBSERVATIONS_DDL = """
    CREATE TABLE IF NOT EXISTS metric_observations (
        time           TIMESTAMPTZ         NOT NULL,
        entity_id      TEXT                NOT NULL,
        event_type     TEXT                NOT NULL,
        source_type    TEXT                NOT NULL,
        metric_value   DOUBLE PRECISION,
        severity       TEXT,
        confidence     DOUBLE PRECISION,
        event_id       TEXT,
        tags           TEXT[]
    );

    SELECT create_hypertable(
        'metric_observations', 'time',
        if_not_exists => TRUE,
        chunk_time_interval => INTERVAL '1 day'
    );

    CREATE INDEX IF NOT EXISTS idx_metric_entity_time
        ON metric_observations (entity_id, time DESC);

    CREATE INDEX IF NOT EXISTS idx_metric_event_type_time
        ON metric_observations (event_type, time DESC);
"""

# Detected temporal patterns — written by TemporalPatternDetector
TEMPORAL_PATTERNS_DDL = """
    CREATE TABLE IF NOT EXISTS temporal_patterns (
        detected_at    TIMESTAMPTZ         NOT NULL DEFAULT now(),
        pattern_type   TEXT                NOT NULL,
        entity_id      TEXT                NOT NULL,
        entity_id_b    TEXT,
        severity       TEXT                NOT NULL,
        confidence     DOUBLE PRECISION    NOT NULL,
        details        JSONB,
        resolved_at    TIMESTAMPTZ,
        is_active      BOOLEAN             DEFAULT TRUE
    );

    SELECT create_hypertable(
        'temporal_patterns', 'detected_at',
        if_not_exists => TRUE,
        chunk_time_interval => INTERVAL '7 days'
    );

    CREATE INDEX IF NOT EXISTS idx_patterns_entity
        ON temporal_patterns (entity_id, detected_at DESC);
"""

# Rolling baselines — materialized per entity+event_type per hour
TEMPORAL_BASELINES_DDL = """
    CREATE TABLE IF NOT EXISTS temporal_baselines (
        entity_id        TEXT              NOT NULL,
        event_type       TEXT              NOT NULL,
        hour_of_week     SMALLINT          NOT NULL,
        mean             DOUBLE PRECISION,
        stddev           DOUBLE PRECISION,
        p50              DOUBLE PRECISION,
        p95              DOUBLE PRECISION,
        sample_count     INTEGER,
        last_computed_at TIMESTAMPTZ       DEFAULT now(),
        PRIMARY KEY (entity_id, event_type, hour_of_week)
    );
"""


class TemporalSchemaManager:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self):
        self._pool = await asyncpg.create_pool(self.dsn, min_size=2, max_size=10)

    async def disconnect(self):
        if self._pool:
            await self._pool.close()

    async def initialize(self):
        """Create extension and all tables. Safe to call on every startup."""
        async with self._pool.acquire() as conn:
            await conn.execute(TIMESCALE_EXTENSION)
            await conn.execute(METRIC_OBSERVATIONS_DDL)
            await conn.execute(TEMPORAL_PATTERNS_DDL)
            await conn.execute(TEMPORAL_BASELINES_DDL)

    async def insert_observation(self, obs: dict) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO metric_observations
                    (time, entity_id, event_type, source_type,
                     metric_value, severity, confidence, event_id, tags)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """,
            obs["time"], obs["entity_id"], obs["event_type"],
            obs["source_type"], obs.get("metric_value"),
            obs.get("severity"), obs.get("confidence"),
            obs.get("event_id"), obs.get("tags", []),
        )

    async def insert_pattern(self, pattern: dict) -> None:
        import json
        async with self._pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO temporal_patterns
                    (pattern_type, entity_id, entity_id_b,
                     severity, confidence, details)
                VALUES ($1,$2,$3,$4,$5,$6)
            """,
            pattern["pattern_type"], pattern["entity_id"],
            pattern.get("entity_id_b"), pattern["severity"],
            pattern["confidence"], json.dumps(pattern.get("details")) if pattern.get("details") else None,
        )
