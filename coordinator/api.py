from contextlib import asynccontextmanager
import asyncio
from fastapi import FastAPI
from .coordinator import AgentCoordinator

coordinator = AgentCoordinator()


@asynccontextmanager
async def lifespan(app: FastAPI):
    bg = asyncio.create_task(coordinator.run())
    yield
    bg.cancel()

app = FastAPI(title="Agent Coordinator API", lifespan=lifespan)


@app.get("/coordinator/agents")
async def list_agents(): return coordinator.get_registry()


@app.get("/coordinator/agents/{agent_id}/tasks")
async def get_agent_tasks(agent_id: str):
    import httpx
    async with httpx.AsyncClient() as c:
        r = await c.get(f"http://localhost:8092/world/tasks/{agent_id}")
    return r.json() if r.status_code == 200 else []


@app.get("/coordinator/conflicts")
async def get_active_conflicts():
    import httpx
    async with httpx.AsyncClient() as c:
        r = await c.get("http://localhost:8092/world/conflicts")
    return r.json() if r.status_code == 200 else []


@app.get("/health")
async def health():
    reg = coordinator.get_registry()
    online = sum(1 for v in reg.values() if v["status"] == "running")
    return {"status": "ok", "agents_online": online, "total": len(reg)}
