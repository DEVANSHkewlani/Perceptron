"""
Execution Layer API — FastAPI service running on port 8095.
"""
from __future__ import annotations
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, BackgroundTasks
from aiokafka import AIOKafkaProducer
from planning.store import PlanStore
from planning.monitor import PlanMonitor
from planning.schema import Plan
from .runner import PlanRunner
from .action_registry import ActionRegistry

# Config from environment variables
KAFKA_URL = os.getenv("KAFKA_URL", "localhost:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
PLANNING_URL = os.getenv("PLANNING_URL", "http://localhost:8094")
WORLD_MODEL_URL = os.getenv("WORLD_MODEL_URL", "http://localhost:8092")

producer: AIOKafkaProducer | None = None
runner:   PlanRunner | None       = None
plan_store: PlanStore | None      = None
plan_monitor: PlanMonitor | None  = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, runner, plan_store, plan_monitor
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_URL)
    await producer.start()
    
    plan_store = PlanStore(REDIS_URL)
    await plan_store.connect()
    
    plan_monitor = PlanMonitor(KAFKA_URL, WORLD_MODEL_URL)
    await plan_monitor.start()
    
    registry = ActionRegistry(producer, REDIS_URL, PLANNING_URL)
    runner = PlanRunner(plan_store, plan_monitor, registry)
    yield
    await producer.stop()
    await plan_store.disconnect()
    await plan_monitor.stop()

app = FastAPI(title="Execution Layer API", lifespan=lifespan)


@app.post("/execution/execute", status_code=202)
async def execute_plan(plan_data: dict, bg: BackgroundTasks):
    """Accept a Plan dict and execute it in the background."""
    if not runner:
        raise HTTPException(status_code=503, detail="Runner not initialized")
    plan = Plan.model_validate(plan_data)
    bg.add_task(runner.execute, plan)
    return {"plan_id": plan.plan_id, "status": "executing"}


@app.get("/health")
async def health():
    return {"status": "ok"}
