from __future__ import annotations
from contextlib import asynccontextmanager
from fastapi import FastAPI, BackgroundTasks
from .engine import ReasoningEngine, ReasoningConfig
import asyncio

engine: ReasoningEngine | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine
    engine = ReasoningEngine()
    bg = asyncio.create_task(engine.run_loop())
    yield
    bg.cancel()

app = FastAPI(title="Reasoning Engine API", lifespan=lifespan)


@app.post("/reasoning/trigger", status_code=202)
async def trigger_reasoning(bg: BackgroundTasks):
    """Force an immediate reasoning cycle (for testing or manual triggers)."""
    bg.add_task(engine.reason)
    return {"status": "reasoning cycle triggered"}


@app.post("/reasoning/reason")
async def reason_now(body: dict | None = None):
    """Synchronous reasoning — waits for decision (for testing)."""
    agent_id = "agent:reasoning-engine"
    domain = "general"
    if body:
        agent_id = body.get("agent_id", agent_id)
        domain = body.get("domain", domain)
    engine.agent_id = agent_id
    engine.domain = domain
    decision = await engine.reason()
    if not decision:
        return {"status": "no_action_needed"}
    return decision.model_dump()


@app.get("/health")
async def health():
    return {"status": "ok"}
