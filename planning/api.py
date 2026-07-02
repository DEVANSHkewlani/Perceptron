"""
Planning System API — FastAPI service running on port 8094.
"""
from __future__ import annotations
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from .generator import PlanGenerator
from .template_store import PlanTemplateStore
from .store import PlanStore
from .monitor import PlanMonitor
from .schema import StepStatus, PlanStatus, PlanStep

# Configure Redis and Kafka URLs from environment variables if present
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
KAFKA_URL = os.getenv("KAFKA_URL", "localhost:9092")
WORLD_MODEL_URL = os.getenv("WORLD_MODEL_URL", "http://localhost:8092")
MEMORY_API_URL = os.getenv("MEMORY_API_URL", "http://localhost:8090")

plan_store   = PlanStore(REDIS_URL)
plan_monitor = PlanMonitor(KAFKA_URL, WORLD_MODEL_URL)
templates    = PlanTemplateStore("plan_templates.yaml")
generator    = PlanGenerator(templates)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Retrieve template path from environment or default to local directory
    templates.load()
    await plan_store.connect()
    await plan_monitor.start()
    yield
    await plan_store.disconnect()
    await plan_monitor.stop()

app = FastAPI(title="Planning System API", lifespan=lifespan)


@app.post("/planning/generate", status_code=201)
async def generate_plan(decision: dict):
    """Generate a Plan from a DecisionObject. Called by Reasoning Engine."""
    plan = await generator.generate(decision)
    await plan_store.save(plan)

    # Link plan_id to episodic memory via decision_id
    decision_id = decision.get("decision_id")
    if decision_id:
        try:
            import httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                r = await client.patch(
                    f"{MEMORY_API_URL}/memory/episodic/by-decision/{decision_id}",
                    json={"plan_id": plan.plan_id}
                )
                if r.status_code != 200:
                    print(f"[Planning API] Warning: Failed to link plan_id to decision_id: status code {r.status_code}")
        except Exception as e:
            import traceback
            print(f"[Planning API] Warning: Error linking plan_id to episodic memory: {type(e)} {e}")
            traceback.print_exc()

    return plan.model_dump()


@app.get("/planning/plans/{plan_id}")
async def get_plan(plan_id: str):
    plan = await plan_store.get(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    return plan.model_dump()


@app.get("/planning/plans")
async def list_plans():
    return [p.model_dump() for p in await plan_store.list_active()]


@app.post("/planning/plans/{plan_id}/approve")
async def approve_plan(plan_id: str, body: dict | None = None):
    """Human operator approves a waiting plan. Execution Layer polls this."""
    plan = await plan_store.get(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    
    # Mark approval-gate steps as approved (SUCCEEDED), save
    for step in plan.steps:
        status_str = step.status.value if hasattr(step.status, "value") else str(step.status)
        if step.is_approval_gate and status_str == "waiting_approval":
            step.status = StepStatus.SUCCEEDED
            
    # Reset plan status to RUNNING if it was awaiting approval
    status_str = plan.status.value if hasattr(plan.status, "value") else str(plan.status)
    if status_str == "awaiting_approval":
        plan.status = PlanStatus.RUNNING

    await plan_store.save(plan)
    return {"approved": plan_id}


@app.delete("/planning/plans/{plan_id}")
async def delete_plan(plan_id: str):
    """Mark a plan as aborted/cancelled."""
    plan = await plan_store.get(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    plan.status = PlanStatus.ABORTED
    await plan_store.save(plan)
    return {"deleted": plan_id}


@app.post("/planning/plans/{plan_id}/append")
async def append_plan_step(plan_id: str, body: dict):
    """Append a step from another plan (conflict resolution merge)."""
    plan = await plan_store.get(plan_id)
    if not plan:
        raise HTTPException(status_code=404, detail="Plan not found")
    
    from_plan_id = body.get("from_plan")
    from_plan = await plan_store.get(from_plan_id)
    if from_plan:
        for step in from_plan.steps:
            if step.action == body.get("action"):
                # Copy step and append
                new_step = step.model_copy()
                new_step.step_id = f"s{len(plan.steps)}"
                new_step.depends_on = [plan.steps[-1].step_id] if plan.steps else []
                plan.steps.append(new_step)
                break
    else:
        new_step = PlanStep(
            step_id=f"s{len(plan.steps)}",
            action=body.get("action"),
            description=f"Appended: {body.get('action')}",
            depends_on=[plan.steps[-1].step_id] if plan.steps else []
        )
        plan.steps.append(new_step)
        
    await plan_store.save(plan)
    return plan.model_dump()


@app.get("/health")
async def health():
    return {"status": "ok"}
