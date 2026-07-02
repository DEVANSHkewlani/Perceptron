"""
World Model FastAPI Server — port 8092
Five query endpoints + health + situation brief stream.
The Reasoning Engine (Phase 7) calls only these endpoints.
"""
from __future__ import annotations

from contextlib import asynccontextmanager
import asyncio

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from .core import WorldModel, WorldModelConfig
from .situation_assessor import SituationAssessor

cfg = WorldModelConfig()   # reads from env vars in production
wm: WorldModel | None = None
assessor: SituationAssessor | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global wm, assessor
    wm = WorldModel(cfg)
    await wm.start()
    assessor = SituationAssessor(wm)
    # Run the Kafka loop as a background task so the API stays responsive
    bg_task = asyncio.create_task(wm.run())
    yield
    bg_task.cancel()
    try:
        await bg_task
    except asyncio.CancelledError:
        pass
    await wm.stop()


app = FastAPI(
    title="World Model API",
    description="Single source of truth for the cognitive architecture.",
    lifespan=lifespan,
)


@app.get("/world/situation")
async def get_current_situation(top_n: int = Query(5, ge=1, le=20)):
    """
    PRIMARY ENDPOINT for the Reasoning Engine.
    Returns ranked situation brief — the LLM-ready context object.
    """
    return await assessor.assess(top_n=top_n)


@app.get("/world/entity/{entity_id}")
async def get_entity_state(entity_id: str):
    state = wm.get_entity_state(entity_id)
    if not state:
        raise HTTPException(status_code=404, detail=f"Entity '{entity_id}' not found")
    return state


@app.get("/world/blast-radius/{entity_id}")
async def get_blast_radius(entity_id: str, max_hops: int = Query(3, ge=1, le=5)):
    return await wm.get_blast_radius(entity_id)


@app.get("/world/causal-chain/{entity_id}")
async def get_causal_chain(entity_id: str):
    return await wm.get_causal_chain(entity_id)


@app.get("/world/predict/{entity_id}")
async def get_prediction(entity_id: str, event_type: str = Query(...)):
    return await wm.get_prediction(entity_id, event_type)


@app.get("/world/anomalies")
async def list_anomalies():
    from dataclasses import asdict
    return [asdict(a) for a in wm.anomalies.get_open()]


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "entities_tracked": len(wm.entities.get_all()),
        "open_anomalies": len(wm.anomalies.get_open()),
    }


@app.post("/world/tasks", status_code=201)
async def create_task(task: dict):
    """Create a task delegation node and registry entry."""
    return await wm.create_task(task)


@app.get("/world/tasks/{agent_id}")
async def get_agent_tasks(agent_id: str, status: str = "pending"):
    """Get active tasks assigned to an agent."""
    return await wm.get_agent_tasks(agent_id, status)


@app.patch("/world/tasks/{task_id}")
async def update_task_status(task_id: str, body: dict):
    """Update task status (e.g. complete or cancel)."""
    await wm.complete_task(task_id, body)
    return {"status": "success", "task_id": task_id}


@app.get("/world/conflicts")
async def get_active_conflicts():
    """Detect and return conflicting tasks in the Neo4j graph."""
    return await wm.causal.detect_all_conflicts()
