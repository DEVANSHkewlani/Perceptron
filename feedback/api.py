"""
Feedback Loop API — port 8096
Provides observability into the feedback cycle:
  GET  /feedback/metrics         — outcome counts, success rates by action
  GET  /feedback/resolves        — query Neo4j for learned RESOLVES edges
  POST /feedback/trigger/{plan_id} — manually trigger feedback for a plan (testing)
  GET  /health
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks
import httpx

from .consumer import FeedbackConsumer, FeedbackConfig

cfg      = FeedbackConfig()
consumer = FeedbackConsumer(cfg)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the Kafka consumer loop as a background task
    bg = asyncio.create_task(consumer.run())
    yield
    bg.cancel()
    try:
        await bg
    except asyncio.CancelledError:
        pass


app = FastAPI(title="Feedback Loop API", lifespan=lifespan)


@app.get("/feedback/metrics")
async def get_metrics():
    """Outcome statistics and per-action success rates from procedural memory."""
    playbook_rates = []
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{cfg.memory_url}/memory/procedural/playbooks")
            if r.status_code == 200:
                playbook_rates = [
                    {
                        "action": pb.get("recommended_action"),
                        "success_rate": pb.get("success_rate"),
                        "success_count": pb.get("success_count", 0),
                        "failure_count": pb.get("failure_count", 0),
                    }
                    for pb in r.json()
                    if pb.get("recommended_action")
                ]
    except Exception:
        pass
    return {**consumer.metrics, "action_rates": playbook_rates}


@app.get("/feedback/resolves")
async def get_resolves_edges():
    """Query Neo4j for all RESOLVES edges — shows learned causal knowledge."""
    if not consumer.graph_updater._driver:
        return []
    try:
        async with consumer.graph_updater._driver.session() as session:
            result = await session.run("""
                MATCH (a:Action)-[r:RESOLVES]->(c:Concept)
                RETURN a.name AS action, c.name AS anomaly_type,
                       r.confidence AS confidence,
                       r.success_count AS wins, r.failure_count AS losses
                ORDER BY r.confidence DESC
            """)
            rows = await result.data()
            return rows
    except Exception as e:
        return {"error": f"Failed to query Neo4j: {e}"}


@app.post("/feedback/trigger/{plan_id}", status_code=202)
async def trigger_feedback(plan_id: str, bg: BackgroundTasks):
    """Manually trigger a feedback cycle — useful for testing."""
    fake_event = {
        "event_type": "action_completed",
        "payload": {"plan_id": plan_id, "action": "manual_trigger"},
    }
    bg.add_task(consumer._process_safe, fake_event)
    return {"triggered": plan_id}


@app.get("/health")
async def health():
    return {"status": "ok"}
