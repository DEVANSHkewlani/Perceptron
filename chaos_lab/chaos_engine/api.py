from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from controller import ChaosController, ScenarioState, DockerChaos
from scenarios import build_scenarios
from injectors.database_chaos import DatabaseChaos
from injectors.queue_chaos import QueueChaos
from injectors.network_chaos import NetworkChaos
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response
from typing import Any
from datetime import datetime, timezone
import uvicorn, os, logging, asyncio, httpx
import redis.asyncio as aioredis

logging.basicConfig(format="%(message)s", level=logging.INFO)
logger = logging.getLogger("chaos-api")

ACTIVATIONS = Counter("chaos_activations_total", "Total scenarios activated", ["scenario"])
DETECTION_TIME = Histogram("chaos_detection_seconds", "Time from activate to DCA detection", ["scenario"])
REMEDIATION_TIME = Histogram("chaos_remediation_seconds", "Time from activate to full remediation", ["scenario"])

DEFAULT_GAUNTLET_STEPS = [
    {"scenario": "product_service_crash", "status": "pending"},
    {"scenario": "order_service_crash", "status": "pending"},
    {"scenario": "db_pool_exhaustion", "status": "pending"},
    {"scenario": "redis_memory_full", "status": "pending"},
    {"scenario": "slow_database_query", "status": "pending"},
    {"scenario": "gateway_latency_spike", "status": "pending"},
    {"scenario": "kafka_consumer_lag", "status": "pending"},
    {"scenario": "gateway_error_rate", "status": "pending"},
    {"scenario": "replication_lag", "status": "pending"},
    {"scenario": "memory_pressure", "status": "pending"},
    {"scenario": "network_packet_loss", "status": "pending"},
    {"scenario": "lock_contention", "status": "pending"},
    {"scenario": "idle_in_transaction", "status": "pending"},
    {"scenario": "cpu_spike", "status": "pending"},
    {"scenario": "config_file_tamper", "status": "pending"},
    {"scenario": "ssl_cert_near_expiry", "status": "pending"},
    {"scenario": "order_processing_delay", "status": "pending"},
    {"scenario": "circuit_breaker_open", "status": "pending"}
]

# Shared objects
controller: ChaosController | None = None
gauntlet_task: Any = None
gauntlet_active: bool = False
gauntlet_steps: list[dict] = [dict(s) for s in DEFAULT_GAUNTLET_STEPS]
gauntlet_current_step: int = 0

@asynccontextmanager
async def lifespan(app: FastAPI):
    global controller
    d = DockerChaos()
    db = DatabaseChaos(dsn=os.getenv("DB_DSN", "postgresql://shopcore:shopcore@postgres:5432/shopcore"))
    q = QueueChaos(bootstrap=os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092"))
    net = NetworkChaos()
    
    scenarios = build_scenarios(d, db, q, net)
    controller = ChaosController(scenarios)
    logger.info("Chaos Engine Controller initialized successfully")
    yield

app = FastAPI(title="ShopCore Chaos Engine API", version="13.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class ActivateRequest(BaseModel):
    params: dict = {}

@app.get("/scenarios")
async def list_scenarios():
    return controller.get_status()

@app.post("/scenarios/{name}/activate")
async def activate(name: str, body: ActivateRequest = ActivateRequest()):
    if name not in controller.scenarios:
        raise HTTPException(404, f"Unknown scenario: {name}")
    success = await controller.activate(name, body.params)
    if not success:
         raise HTTPException(400, f"Failed to activate scenario: {name} (already active or in recovery)")
    ACTIVATIONS.labels(scenario=name).inc()
    return {"status": "activated", "scenario": name}

@app.post("/scenarios/{name}/deactivate")
async def deactivate(name: str):
    if name not in controller.scenarios:
        raise HTTPException(404, f"Unknown scenario: {name}")
    await controller.deactivate(name)
    return {"status": "deactivated", "scenario": name}

@app.post("/scenarios/reset")
@app.post("/reset")
async def reset():
    global gauntlet_active, gauntlet_steps, gauntlet_current_step
    gauntlet_active = False
    gauntlet_steps = [dict(s) for s in DEFAULT_GAUNTLET_STEPS]
    gauntlet_current_step = 0
    await controller.deactivate_all()
    return {"status": "reset", "detail": "all scenarios deactivated and gauntlet status reset"}

@app.post("/scenarios/{name}/mark_detected")
async def mark_detected(name: str):
    s = controller.scenarios.get(name)
    if s and s.state == ScenarioState.ACTIVE and not s.detected_at:
        await controller.mark_detected(name)
        if s.detected_at and s.activated_at:
            dt = (s.detected_at - s.activated_at).total_seconds()
            DETECTION_TIME.labels(scenario=name).observe(dt)
    return {"status": "ok"}

@app.post("/scenarios/{name}/mark_remediated")
async def mark_remediated(name: str):
    s = controller.scenarios.get(name)
    if s and s.state == ScenarioState.ACTIVE and not s.remediated_at:
        await controller.mark_remediated(name)
        if s.remediated_at and s.activated_at:
            dt = (s.remediated_at - s.activated_at).total_seconds()
            REMEDIATION_TIME.labels(scenario=name).observe(dt)
    return {"status": "ok"}

# Gauntlet runner logic in-memory mapping
async def gauntlet_runner():
    global gauntlet_active, gauntlet_steps, gauntlet_current_step
    logger.info("[gauntlet] Starting automatic test run")
    gauntlet_steps = [dict(s) for s in DEFAULT_GAUNTLET_STEPS]
    gauntlet_current_step = 0
    
    for i, step in enumerate(gauntlet_steps):
        if not gauntlet_active:
            break
        gauntlet_current_step = i
        name = step["scenario"]
        step["status"] = "active"
        
        logger.info(f"[gauntlet] Step {i+1}: Activating {name}")
        await controller.activate(name)
        
        # Wait for DCA detection and remediation (up to 120s)
        for _ in range(60):
            if not gauntlet_active:
                break
            await asyncio.sleep(2)
            s = controller.scenarios[name]
            if s.remediated_at:
                step["status"] = "healed"
                break
        else:
            step["status"] = "failed"
            
        logger.info(f"[gauntlet] Step {i+1}: Deactivating {name}")
        await controller.deactivate(name)
        
        # Cool down
        if gauntlet_active:
            await asyncio.sleep(15)
            
    gauntlet_active = False
    logger.info("[gauntlet] Automatic test run completed")

@app.post("/gauntlet/start")
async def start_gauntlet(bt: BackgroundTasks):
    global gauntlet_active
    if gauntlet_active:
        return {"status": "running", "detail": "Gauntlet already in progress"}
    gauntlet_active = True
    bt.add_task(gauntlet_runner)
    return {"status": "started"}

@app.get("/gauntlet/status")
async def get_gauntlet_status():
    return {
        "active": gauntlet_active,
        "current_step": gauntlet_current_step,
        "steps": gauntlet_steps
    }

@app.get("/health")
async def health():
    return {"status": "ok", "service": "chaos-engine"}

@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/metrics/live")
async def live_metrics():
    # 1. Redis memory load
    redis_load = 12
    try:
        r = await aioredis.from_url(os.getenv("REDIS_URL", "redis://redis:6379"))
        info = await r.info("memory")
        await r.close()
        used = info.get("used_memory", 0)
        max_mem = info.get("maxmemory", 0) or (256 * 1024 * 1024)
        redis_load = min(100, int((used / max_mem) * 100))
    except Exception as e:
        logger.error(f"Failed to fetch Redis memory: {e}")

    # 2. Kafka lag
    kafka_lag = 0
    try:
        lag = 0
        s = controller.scenarios.get("kafka_consumer_lag")
        if s and s.state == ScenarioState.ACTIVE:
            # Generate simulated lag growing by 2 per second since activated
            lag = int((datetime.now(timezone.utc) - s.activated_at).total_seconds() * 2)
        kafka_lag = lag
    except Exception as e:
        logger.error(f"Failed to fetch Kafka lag: {e}")

    # 3. Error rate & Availability from API Gateway
    error_rate = 0.0
    availability = 100.00
    try:
        # Fetch from Prometheus gateway metrics
        async with httpx.AsyncClient(timeout=1.0) as client:
            resp = await client.get("http://api-gateway:8010/metrics")
            if resp.status_code == 200:
                text = resp.text
                total = 0
                errors = 0
                for line in text.split("\n"):
                    if "shopcore_gateway_requests_total" in line and not line.startswith("#"):
                        try:
                            val = int(float(line.split()[-1]))
                            total += val
                            if 'status="5' in line:
                                errors += val
                        except:
                            pass
                if total > 0:
                    error_rate = round((errors / total) * 100, 2)
                    availability = round(((total - errors) / total) * 100, 2)
    except Exception as e:
        s = controller.scenarios.get("gateway_error_rate")
        if s and s.state == ScenarioState.ACTIVE:
            error_rate = s.params.get("fail_rate", 0.3) * 100
            availability = 100.0 - error_rate
        else:
            # If gateway is unreachable, availability is 0
            availability = 0.0
            error_rate = 100.0

    return {
        "redis_memory_load": redis_load,
        "kafka_consumer_lag": kafka_lag,
        "error_rate": error_rate,
        "availability": availability
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9091)
