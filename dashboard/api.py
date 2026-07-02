from __future__ import annotations
import os
import json
import asyncio
import logging
import socket
import time
import httpx
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from aiokafka import AIOKafkaConsumer

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard-api")

app = FastAPI(title="Cognitive Architecture Dashboard Aggregator")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Active WebSocket connections
active_connections: Set[WebSocket] = set()

# Global state cache
last_status: Dict = {}
connection_requested = False
connection_errors: list[str] = []

# Global metrics tracking
kafka_message_counter = 0
last_poll_time = datetime.now()
adapter_message_counts = {
    "log": 0,
    "api": 0,
    "database": 0,
    "redis": 0,
    "queue": 0,
    "file": 0,
    "metric": 0,
    "user": 0,
    "agent": 0
}
last_adapter_rates = {
    "log": "0 msg/min",
    "api": "0 poll/min",
    "database": "0 poll/min",
    "redis": "0 poll/min",
    "queue": "0 poll/min",
    "file": "0 events/min",
    "metric": "0 alerts/min",
    "user": "0 events/min",
    "agent": "0 events/min"
}


async def broadcast(message: dict):
    """Broadcast JSON message to all active WebSocket clients."""
    if not active_connections:
        return
    payload = json.dumps(message)
    disconnected = set()
    for websocket in active_connections:
        try:
            await websocket.send_text(payload)
        except Exception:
            disconnected.add(websocket)
    for ws in disconnected:
        active_connections.remove(ws)

def _tcp_check(host: str, port: int, timeout: float = 0.25) -> tuple[bool, str]:
    try:
        t0 = time.monotonic()
        with socket.create_connection((host, port), timeout=timeout):
            latency = int((time.monotonic() - t0) * 1000)
        return True, f"{latency}ms"
    except Exception:
        return False, "--"


def _dashboard_connected() -> bool:
    return connection_requested and is_shopcore_connected()


async def get_readiness() -> dict:
    checks = {
        "shopcore_gateway": ("localhost", 8010, True),
        "dca_kafka": ("localhost", 9092, True),
        "dca_redis": ("localhost", 6379, True),
        "dca_postgres": ("localhost", 5432, True),
        "dca_neo4j": ("localhost", 7687, True),
        "dca_qdrant": ("localhost", 6333, True),
        "shopcore_kafka": ("localhost", 9094, False),
        "shopcore_postgres": ("localhost", 5433, False),
        "shopcore_redis": ("localhost", 6380, False),
    }
    results = {}
    errors = []
    for name, (host, port, required) in checks.items():
        ok, latency = _tcp_check(host, port)
        results[name] = {
            "ok": ok,
            "required": required,
            "target": f"{host}:{port}",
            "latency": latency,
        }
        if required and not ok:
            errors.append(f"{name} unreachable at {host}:{port}")
    return {
        "ready": not errors,
        "connected": connection_requested and not errors,
        "errors": errors,
        "checks": results,
    }


async def get_adapters_status() -> list:
    global last_adapter_rates
    adapters = [
        {"type": "LogAdapter", "source": "svc:api-gateway/product/order/user logs", "port": 8080, "rate": last_adapter_rates.get("log", "0 msg/min"), "lag": "0 msgs", "schema": "Pydantic.LogEventSchema"},
        {"type": "MetricWebhook", "source": "metric:prometheus", "port": 9090, "rate": last_adapter_rates.get("metric", "0 alerts/min"), "lag": "0 msgs", "schema": "Pydantic.PromAlertSchema"},
        {"type": "APIAdapter", "source": "ShopCore health endpoints", "port": 8010, "rate": last_adapter_rates.get("api", "0 poll/min"), "lag": "0 msgs", "schema": "Pydantic.ApiHealthSchema"},
        {"type": "DatabaseAdapter", "source": "db:shopcore-postgres", "port": 5433, "rate": last_adapter_rates.get("database", "0 poll/min"), "lag": "0 msgs", "schema": "Pydantic.DatabaseMetricsSchema"},
        {"type": "QueueAdapter", "source": "queue:order-events", "port": 9094, "rate": last_adapter_rates.get("queue", "0 poll/min"), "lag": "0 msgs", "schema": "Pydantic.QueueMetricsSchema"},
        {"type": "FileAdapter", "source": "file:nginx-config,file:ssl-certs", "port": None, "rate": last_adapter_rates.get("file", "0 events/min"), "lag": "0 msgs", "schema": "Pydantic.FileEventSchema"},
        {"type": "RedisAdapter", "source": "cache:shopcore-redis", "port": 6380, "rate": last_adapter_rates.get("redis", "0 poll/min"), "lag": "0 msgs", "schema": "Pydantic.RedisMetricsSchema"},
        {"type": "UserEventsAdapter", "source": "perception push endpoint", "port": 8080, "rate": last_adapter_rates.get("user", "0 events/min"), "lag": "0 msgs", "schema": "Pydantic.UserBehaviorSchema"},
        {"type": "AgentEventsAdapter", "source": "perception push endpoint", "port": 8080, "rate": last_adapter_rates.get("agent", "0 events/min"), "lag": "0 msgs", "schema": "Pydantic.AgentEventSchema"},
    ]

    for adapter in adapters:
        port = adapter.pop("port")
        if port is None:
            ok = os.path.exists("./files/nginx/conf.d")
            adapter["status"] = "ACTIVE" if ok and _dashboard_connected() else "DOWN"
            adapter["latency"] = "--"
        else:
            ok, latency = _tcp_check("localhost", port)
            adapter["status"] = "ACTIVE" if ok and _dashboard_connected() else "DOWN"
            adapter["latency"] = latency
    return adapters

def is_shopcore_connected() -> bool:
    ok, _ = _tcp_check("localhost", 8010)
    return ok

# Periodic services polling task
async def poll_services_loop():
    logger.info("Starting background services polling task...")
    ports = {
        "perception": 8080,
        "memory": 8090,
        "temporal": 8091,
        "world_model": 8092,
        "reasoning": 8093,
        "planning": 8094,
        "execution": 8095,
        "feedback": 8096,
        "coordinator": 8097
    }
    
    while True:
        global kafka_message_counter, last_poll_time, adapter_message_counts, last_adapter_rates
        now = datetime.now()
        elapsed = (now - last_poll_time).total_seconds() or 2.0
        calculated_throughput = round(kafka_message_counter / elapsed, 2)
        kafka_message_counter = 0
        last_poll_time = now

        # Calculate rates for each adapter type
        for stype, count in list(adapter_message_counts.items()):
            rate_per_min = round((count / elapsed) * 60, 1)
            adapter_message_counts[stype] = 0
            if stype in ("api", "database", "redis", "queue"):
                last_adapter_rates[stype] = f"{rate_per_min} poll/min"
            elif stype == "metric":
                last_adapter_rates[stype] = f"{rate_per_min} alerts/min"
            else:
                last_adapter_rates[stype] = f"{rate_per_min} msg/min"


        status = {
            "connected": _dashboard_connected(),
            "health": "OFFLINE",
            "throughput": calculated_throughput if _dashboard_connected() else 0.0,
            "agents": {},
            "anomalies": [],
            "active_plans": [],
            "services": {name: "offline" for name in ports},
            "memory_stats": {
                "redis_keys": 0,
                "timescale_records": 0,
                "neo4j_nodes": 0,
                "qdrant_playbooks": 0
            },
            "conflicts": [],
            "feedback_metrics": {},
            "adapters": [],
            "readiness": await get_readiness(),
        }
        
        if not _dashboard_connected():
            async with httpx.AsyncClient(timeout=1.0) as client:
                for name, port in ports.items():
                    try:
                        r = await client.get(f"http://localhost:{port}/health")
                        if r.status_code == 200:
                            status["services"][name] = "online"
                    except Exception:
                        pass
                try:
                    status["adapters"] = await get_adapters_status()
                except Exception:
                    pass
        else:
            status["health"] = "NOMINAL"
            async with httpx.AsyncClient(timeout=1.0) as client:
                # Check service health endpoints
                for name, port in ports.items():
                    try:
                        r = await client.get(f"http://localhost:{port}/health")
                        if r.status_code == 200:
                            status["services"][name] = "online"
                    except Exception:
                        pass

                # Fetch Coordinator agents list
                if status["services"]["coordinator"] == "online":
                    try:
                        r = await client.get("http://localhost:8097/coordinator/agents")
                        if r.status_code == 200:
                            status["agents"] = r.json()
                        r_c = await client.get("http://localhost:8097/coordinator/conflicts")
                        if r_c.status_code == 200:
                            status["conflicts"] = r_c.json()
                    except Exception:
                        pass

                # Fetch World Model anomalies
                if status["services"]["world_model"] == "online":
                    try:
                        r = await client.get("http://localhost:8092/world/anomalies")
                        if r.status_code == 200:
                            status["anomalies"] = r.json()
                            if len(status["anomalies"]) > 0:
                                status["health"] = "INCIDENT"
                            
                            # Query dynamic forecasts for each active anomaly
                            for anom in status["anomalies"]:
                                try:
                                    pred_r = await client.get(
                                        f"http://localhost:8092/world/predict/{anom['entity_id']}",
                                        params={"event_type": anom["event_type"]}
                                    )
                                    if pred_r.status_code == 200:
                                        preds = pred_r.json()
                                        anom["predictions"] = {}
                                        for p in preds:
                                            horizon = f"t{p['horizon_min']}"
                                            anom["predictions"][horizon] = {
                                                "value": p["predicted"],
                                                "confidence": p["confidence"]
                                            }
                                except Exception as e:
                                    logger.warning(f"Error fetching predictions for anomaly: {e}")
                    except Exception:
                        pass

                # Fetch Planning active plans
                if status["services"]["planning"] == "online":
                    try:
                        r = await client.get("http://localhost:8094/planning/plans")
                        if r.status_code == 200:
                            status["active_plans"] = r.json()
                    except Exception:
                        pass

                # Fetch Memory layer stats
                if status["services"]["memory"] == "online":
                    try:
                        r_stats = await client.get("http://localhost:8090/memory/stats")
                        if r_stats.status_code == 200:
                            status["memory_stats"] = r_stats.json()
                    except Exception:
                        pass

                # Fetch Feedback Loop metrics
                if status["services"]["feedback"] == "online":
                    try:
                        r_fb = await client.get("http://localhost:8096/feedback/metrics")
                        if r_fb.status_code == 200:
                            status["feedback_metrics"] = r_fb.json()
                    except Exception:
                        pass
                
                # Fetch dynamic perception adapters
                try:
                    status["adapters"] = await get_adapters_status()
                except Exception:
                    pass

        # Update cache and broadcast
        global last_status
        last_status = status
        if _dashboard_connected():
            await broadcast({"type": "status_update", "data": status})
        await asyncio.sleep(2)


# Background Kafka consumer task
async def kafka_consumer_loop():
    global kafka_message_counter, adapter_message_counts
    kafka_url = os.getenv("KAFKA_URL", "localhost:9092")
    logger.info(f"Connecting to Kafka at {kafka_url}...")
    
    while True:
        try:
            consumer = AIOKafkaConsumer(
                "cognitive.events",
                "cognitive.perception_failures",
                bootstrap_servers=kafka_url,
                group_id="dashboard-aggregator-v12",
                auto_offset_reset="latest",
                value_deserializer=lambda b: json.loads(b.decode())
            )
            await consumer.start()
            logger.info("Successfully connected to Kafka. Ingesting feeds...")
            
            async for msg in consumer:
                kafka_message_counter += 1
                event = msg.value
                if isinstance(event, dict):
                    stype = event.get("source_type")
                    if not stype:
                        source_id = event.get("source_id") or ""
                        if source_id.startswith("svc:"):
                            stype = "api"
                        elif source_id.startswith("db:"):
                            stype = "database"
                        elif source_id.startswith("cache:"):
                            stype = "redis"
                        elif source_id.startswith("queue:"):
                            stype = "queue"
                        elif source_id.startswith("file:"):
                            stype = "file"
                        elif source_id.startswith("metric:"):
                            stype = "metric"
                        elif source_id.startswith("agent:"):
                            stype = "agent"
                        elif source_id.startswith("usr:"):
                            stype = "user"
                        else:
                            stype = "log"
                    
                    if stype in adapter_message_counts:
                        adapter_message_counts[stype] += 1

                if _dashboard_connected():
                    await broadcast({
                        "type": "event",
                        "data": msg.value
                    })
        except Exception as e:
            logger.warning(f"Kafka unavailable at {kafka_url}: {e}. Retrying connection in 5s...")
            await asyncio.sleep(5)

# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background polling and Kafka simulation loops
    polling_task = asyncio.create_task(poll_services_loop())
    kafka_task = asyncio.create_task(kafka_consumer_loop())
    yield
    polling_task.cancel()
    kafka_task.cancel()
    try:
        await asyncio.gather(polling_task, kafka_task, return_exceptions=True)
    except Exception:
        pass

# Register lifespan
app.router.lifespan_context = lifespan

# Static endpoints
@app.get("/")
async def read_index():
    with open("dashboard/static/index.html") as f:
        return HTMLResponse(content=f.read(), status_code=200)

# Websocket endpoint
@app.websocket("/ws/dashboard")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.add(websocket)
    logger.info(f"WebSocket client connected. Total clients: {len(active_connections)}")
    
    # Send current status immediately upon connection
    if last_status and _dashboard_connected():
        try:
            await websocket.send_text(json.dumps({"type": "status_update", "data": last_status}))
        except Exception:
            pass

    try:
        while True:
            # Keep socket alive and receive client queries/messages
            data = await websocket.receive_text()
            logger.info(f"Received WS message: {data}")
    except WebSocketDisconnect:
        active_connections.remove(websocket)
        logger.info(f"WebSocket client disconnected. Remaining: {len(active_connections)}")

# REST APIs

@app.post("/api/connect")
async def connect_dashboard():
    global connection_requested, connection_errors
    readiness = await get_readiness()
    if not readiness["ready"]:
        connection_requested = False
        connection_errors = readiness["errors"]
        return JSONResponse(status_code=503, content=readiness)
    connection_requested = True
    connection_errors = []
    readiness = await get_readiness()
    await broadcast({"type": "connection", "data": readiness})
    return readiness


@app.post("/api/disconnect")
async def disconnect_dashboard():
    global connection_requested, connection_errors, last_status, kafka_message_counter
    connection_requested = False
    connection_errors = []
    kafka_message_counter = 0
    last_status = {}
    payload = {"ready": False, "connected": False, "errors": [], "checks": {}}
    await broadcast({"type": "connection", "data": payload})
    return payload


@app.get("/api/connection/status")
async def connection_status():
    readiness = await get_readiness()
    return {
        **readiness,
        "connected": _dashboard_connected(),
        "requested": connection_requested,
        "errors": readiness["errors"] or connection_errors,
    }

@app.post("/api/plans/{plan_id}/approve")
async def approve_plan(plan_id: str):
    """Proxy plan approval request to the Planning service."""
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(f"http://localhost:8094/planning/plans/{plan_id}/approve")
            return JSONResponse(status_code=r.status_code, content=r.json())
        except Exception as e:
            logger.warning(f"Failed to connect to Planning API to approve plan: {e}")
            raise HTTPException(status_code=503, detail="Planning API offline")

@app.post("/api/plans/{plan_id}/reject")
async def reject_plan(plan_id: str):
    """Proxy plan rejection (delete) request to the Planning service."""
    async with httpx.AsyncClient() as client:
        try:
            r = await client.delete(f"http://localhost:8094/planning/plans/{plan_id}")
            return JSONResponse(status_code=r.status_code, content=r.json())
        except Exception as e:
            logger.warning(f"Failed to connect to Planning API to reject plan: {e}")
            raise HTTPException(status_code=503, detail="Planning API offline")

@app.get("/api/plans")
async def get_dashboard_plans():
    """Proxy active plans from Planning service."""
    if not _dashboard_connected():
        return []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get("http://localhost:8094/planning/plans")
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.warning(f"Failed to fetch plans: {e}")
    return []


@app.get("/api/anomalies")
async def get_dashboard_anomalies():
    """Proxy anomalies from World Model, enriched with temporal predictions."""
    if not _dashboard_connected():
        return []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get("http://localhost:8092/world/anomalies")
            if r.status_code == 200:
                anoms = r.json()
                for anom in anoms:
                    try:
                        pred_r = await client.get(
                            f"http://localhost:8092/world/predict/{anom['entity_id']}",
                            params={"event_type": anom["event_type"]}
                        )
                        if pred_r.status_code == 200:
                            preds = pred_r.json()
                            anom["predictions"] = {}
                            for p in preds:
                                horizon = f"t{p['horizon_min']}"
                                anom["predictions"][horizon] = {
                                    "value": p["predicted"],
                                    "confidence": p["confidence"]
                                }
                    except Exception as e:
                        logger.warning(f"Error fetching predictions for anomaly in API: {e}")
                return anoms
        except Exception as e:
            logger.warning(f"Failed to fetch anomalies: {e}")
    return []


@app.get("/api/coordinator/agents/{agent_id}/tasks")
async def get_dashboard_agent_tasks(agent_id: str):
    """Proxy agent tasks from Coordinator service."""
    if not _dashboard_connected():
        return []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(f"http://localhost:8097/coordinator/agents/{agent_id}/tasks")
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.warning(f"Failed to fetch agent tasks: {e}")
    return []


@app.get("/api/graph/d3")
async def get_d3_graph():
    """Return nodes and links compatible with D3 Graph visualizer from Neo4j or empty fallback."""
    if not _dashboard_connected():
        return {"nodes": [], "links": []}
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "password123")
    
    nodes_map = {}
    links = []
    
    try:
        from neo4j import AsyncGraphDatabase
        async with AsyncGraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password)) as driver:
            await driver.verify_connectivity()
            async with driver.session() as session:
                # Query nodes
                result_nodes = await session.run("MATCH (n) RETURN id(n) as id, labels(n)[0] as label, n.id as name LIMIT 50")
                async for r in result_nodes:
                    node_id = r["name"] or f"node_{r['id']}"
                    label = r["label"] or "Entity"
                    color = "#ef4444" if "Database" in label or "db" in node_id \
                        else "#fbbf24" if "Service" in label or "svc" in node_id \
                        else "#3b82f6" if "UserFlow" in label or "usr" in node_id \
                        else "#10b981" if "Agent" in label or "agent" in node_id \
                        else "#8b5cf6" # Action
                    nodes_map[node_id] = {
                        "id": node_id,
                        "type": label,
                        "color": color
                    }
                
                # Query links
                result_links = await session.run("MATCH (n)-[r]->(m) RETURN n.id as source, m.id as target, type(r) as type LIMIT 50")
                async for r in result_links:
                    source = r["source"]
                    target = r["target"]
                    if source and target and source in nodes_map and target in nodes_map:
                        links.append({
                            "source": source,
                            "target": target,
                            "type": r["type"]
                        })
    except Exception as e:
        logger.warning(f"Neo4j is offline: {e}. Returning empty graph.")
        
    return {
        "nodes": list(nodes_map.values()),
        "links": links
    }

@app.post("/api/graph/query")
async def run_cypher_query(body: dict):
    """Run Cypher query against Neo4j database or return error if offline."""
    cypher = body.get("cypher")
    if not cypher:
        raise HTTPException(status_code=400, detail="Missing cypher query parameter")
    
    neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    neo4j_user = os.getenv("NEO4J_USER", "neo4j")
    neo4j_password = os.getenv("NEO4J_PASSWORD", "password123")
    
    try:
        from neo4j import AsyncGraphDatabase
        async with AsyncGraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password)) as driver:
            await driver.verify_connectivity()
            async with driver.session() as session:
                result = await session.run(cypher)
                records = [dict(r) async for r in result]
                return {"status": "success", "data": records}
    except Exception as e:
        logger.warning(f"Neo4j connection failed: {e}. Returning empty.")
        return {
            "status": "error",
            "data": [],
            "message": f"Neo4j is offline ({str(e)})."
        }

def _extract_decision(payload) -> dict | None:
    """Normalize reasoning_completed payload to a DecisionObject dict."""
    if not payload:
        return None
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return None
    if not isinstance(payload, dict):
        return None
    if payload.get("recommended_action"):
        return payload
    nested = payload.get("decision")
    if isinstance(nested, dict) and nested.get("recommended_action"):
        return nested
    return None


@app.get("/api/decision/latest")
async def get_latest_decision():
    """Get the latest reasoning decision from working or episodic memory."""
    if not _dashboard_connected():
        return None
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get("http://localhost:8090/memory/working/recent", params={"limit": 20})
            if r.status_code == 200:
                for event in r.json():
                    if event.get("event_type") == "reasoning_completed":
                        decision = _extract_decision(event.get("payload"))
                        if decision:
                            return decision
        except Exception as e:
            logger.warning(f"Error checking working memory for decision: {e}")
        try:
            r = await client.get(
                "http://localhost:8090/memory/episodic/type/reasoning_completed",
                params={"hours": 24, "limit": 5},
            )
            if r.status_code == 200:
                for latest_event in r.json():
                    decision = _extract_decision(latest_event.get("payload"))
                    if decision:
                        return decision
        except Exception as e:
            logger.warning(f"Error checking episodic memory for decision: {e}")
    return None


@app.get("/api/events")
async def get_events(limit: int = 50, severity: str | None = None):
    """Fetch recent events from working memory, falling back to episodic."""
    if not _dashboard_connected():
        return []
    async with httpx.AsyncClient() as client:
        events: list = []
        try:
            r = await client.get("http://localhost:8090/memory/working/recent", params={"limit": limit})
            if r.status_code == 200:
                events = r.json()
        except Exception:
            pass
        if not events:
            try:
                r = await client.get(
                    "http://localhost:8090/memory/episodic/recent",
                    params={"limit": limit},
                )
                if r.status_code == 200:
                    events = r.json()
            except Exception:
                pass
        if severity:
            events = [e for e in events if e.get("severity") == severity]
        return events

@app.get("/api/feedback/recent")
async def get_feedback_recent():
    """Fetch feedback loop statistics or return empty counters."""
    if not _dashboard_connected():
        return {
            "total_processed": 0,
            "successes": 0,
            "failures": 0,
            "partials": 0,
            "action_rates": []
        }
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get("http://localhost:8096/feedback/metrics")
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
    return {
        "total_processed": 0,
        "successes": 0,
        "failures": 0,
        "partials": 0,
        "action_rates": []
    }

@app.get("/api/memory/episodic/summary")
async def get_episodic_summary():
    """Fetch episodic memory summary statistics."""
    if not _dashboard_connected():
        return {}
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get("http://localhost:8090/memory/episodic/summary")
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.warning(f"Failed to fetch episodic summary: {e}")
    return {}

@app.get("/api/memory/procedural/playbooks")
async def get_procedural_playbooks():
    """Fetch procedural playbooks from memory API."""
    if not _dashboard_connected():
        return []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get("http://localhost:8090/memory/procedural/playbooks")
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.warning(f"Failed to fetch playbooks: {e}")
    return []

@app.get("/api/memory/working/keys")
async def get_dashboard_working_keys():
    """Fetch working memory keys from memory API."""
    if not _dashboard_connected():
        return []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get("http://localhost:8090/memory/working/keys")
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.warning(f"Failed to fetch working keys: {e}")
    return []

@app.get("/api/memory/episodic/recent")
async def get_dashboard_episodic_recent(limit: int = 50):
    """Fetch recent episodic events from memory API."""
    if not _dashboard_connected():
        return []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get("http://localhost:8090/memory/episodic/recent", params={"limit": limit})
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.warning(f"Failed to fetch episodic recent events: {e}")
    return []

@app.get("/api/chaos/status")
async def get_chaos_status():
    """Chaos dashboard is intentionally separate from the DCA dashboard."""
    raise HTTPException(status_code=404, detail="Chaos dashboard is separate from DCA dashboard")

@app.get("/api/chaos/history")
async def get_chaos_history():
    """Chaos dashboard is intentionally separate from the DCA dashboard."""
    raise HTTPException(status_code=404, detail="Chaos dashboard is separate from DCA dashboard")

@app.get("/api/metrics/history")
async def get_metrics_history(entity_id: str, event_type: str, limit: int = 30):
    """Fetch metric history from memory API."""
    if not _dashboard_connected():
        return []
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(
                f"http://localhost:8090/memory/metrics/{entity_id}",
                params={"event_type": event_type, "limit": limit}
            )
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            logger.warning(f"Failed to fetch metric history: {e}")
    return []

# Try mounting static files, create directory structure first
try:
    os.makedirs("dashboard/static", exist_ok=True)
    app.mount("/static", StaticFiles(directory="dashboard/static"), name="static")
except Exception as e:
    logger.warning(f"Could not mount static directory: {e}")
