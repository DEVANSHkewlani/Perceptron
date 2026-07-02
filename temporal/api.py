"""
FastAPI REST API for Temporal Engine
Exposes endpoints for querying state profiles, patterns, predictions, and baselines.
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query

from .state import TemporalStateManager
from .predictor import TemporalPredictor
from .schema import TemporalSchemaManager

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/cognitive")
REDIS_URL    = os.getenv("REDIS_URL", "redis://localhost:6379")

state_mgr   = TemporalStateManager(POSTGRES_DSN, REDIS_URL)
schema_mgr  = TemporalSchemaManager(POSTGRES_DSN)
predictor   = TemporalPredictor()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await state_mgr.connect()
    await schema_mgr.connect()
    yield
    await state_mgr.disconnect()
    await schema_mgr.disconnect()

app = FastAPI(title="Temporal Engine API", lifespan=lifespan)


@app.get("/temporal/state/{entity_id:path}")
async def get_temporal_state(
    entity_id: str,
    event_type: str = Query(...),
):
    """Current temporal state for a specific entity + event_type pair."""
    state = await state_mgr.get_state(entity_id, event_type)
    return state.__dict__


@app.get("/temporal/patterns/{entity_id:path}")
async def get_patterns(
    entity_id: str,
    limit: int = Query(20, ge=1, le=100),
):
    """Recent detected patterns for an entity."""
    async with schema_mgr._pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT pattern_type, severity, confidence, details, detected_at
            FROM temporal_patterns
            WHERE entity_id = $1
            ORDER BY detected_at DESC LIMIT $2
        """, entity_id, limit)
    return [dict(r) for r in rows]


@app.get("/temporal/predict/{entity_id:path}")
async def get_prediction(
    entity_id: str,
    event_type: str = Query(...),
):
    """5, 15, 60 minute forecasts for the requested metric."""
    async with schema_mgr._pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT metric_value FROM metric_observations
            WHERE entity_id = $1 AND event_type = $2
              AND metric_value IS NOT NULL
              AND time >= now() - INTERVAL '2 hours'
            ORDER BY time ASC
        """, entity_id, event_type)
    values = [r["metric_value"] for r in rows]
    preds = predictor.predict(entity_id, event_type, values)
    return [p.__dict__ for p in preds]


@app.get("/temporal/baselines/{entity_id:path}")
async def get_baseline(entity_id: str, event_type: str = Query(...)):
    """Hour-of-week baseline statistics for an entity metric."""
    async with schema_mgr._pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT hour_of_week, mean, stddev, p50, p95, sample_count
            FROM temporal_baselines
            WHERE entity_id = $1 AND event_type = $2
            ORDER BY hour_of_week
        """, entity_id, event_type)
    return [dict(r) for r in rows]


@app.get("/health")
async def health():
    return {"status": "ok"}
