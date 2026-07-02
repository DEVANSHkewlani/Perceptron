"""
Perception API
==============
FastAPI endpoints for PUSH-based signal sources.
Sources that push to us (rather than us polling them):
  - User events (from browser JS SDK)
  - Browser environment events (JS errors, Web Vitals)
  - Security events (WAF webhooks, Cloudflare, Fail2ban)
  - Agent events (from cognitive sub-agents)
  - Deployment webhooks (GitHub, GitLab, ArgoCD)
  - Third-party status page webhooks

All routes normalize their specific input format and publish
a validated CognitiveEvent to Kafka (or PerceptionFailure to the failure topic).

Run with:
    uvicorn cognitive_perception.perception_api.main:app --host 0.0.0.0 --port 8080
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, Request, BackgroundTasks
from pydantic import BaseModel

from ..schema.event import CognitiveEvent, PerceptionFailure, Severity, SourceType
from ..normalizers.metric_normalizer import MetricNormalizer
from ..normalizers.user_normalizer import UserBehaviorNormalizer
from ..normalizers.browser_normalizer import BrowserEventNormalizer
from ..normalizers.security_normalizer import SecurityEventNormalizer


# ─────────────────────────────────────────────
# CONFIG & APP SETUP
# ─────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC     = "cognitive.events"
FAILURE_TOPIC   = "cognitive.perception_failures"

producer: AIOKafkaProducer | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            enable_idempotence=True,
            acks="all",
        )
        await producer.start()
    except Exception as e:
        print(f"[PerceptionAPI] Failed to start Kafka producer on {KAFKA_BOOTSTRAP}: {e}")
    yield
    if producer:
        await producer.stop()


app = FastAPI(
    title="Cognitive Perception API",
    description="Push endpoint for all signal sources that send events to us.",
    lifespan=lifespan,
)


async def _publish(event: CognitiveEvent | PerceptionFailure | Any):
    """Publish a normalized event or failure to Kafka with publish lag monitoring."""
    if not producer:
        print(f"[PerceptionAPI] Producer offline. Dropped event: {event.event_type if hasattr(event, 'event_type') else type(event)}")
        return
    import time
    try:
        payload = event.model_dump_json().encode("utf-8")
        topic = FAILURE_TOPIC if isinstance(event, PerceptionFailure) else KAFKA_TOPIC
        
        t0 = time.monotonic()
        await producer.send_and_wait(topic, payload)
        kafka_lag_ms = (time.monotonic() - t0) * 1000
        
        if kafka_lag_ms > 500:
            print(f"[WARNING] [PerceptionAPI] Kafka publish lag high: {kafka_lag_ms:.1f}ms "
                  f"for {event.event_type if hasattr(event, 'event_type') else 'failure'}")
    except Exception as e:
        print(f"[PerceptionAPI] Failed to publish event: {e}")


# ─────────────────────────────────────────────
# USER EVENTS  (from frontend JS SDK)
# ─────────────────────────────────────────────

class UserEventBatch(BaseModel):
    """
    The frontend SDK sends events in batches every few seconds.
    Each item is one user action.
    """
    events: list[dict[str, Any]]
    session_id: str
    user_id: str | None = None


@app.post("/perception/user-events", status_code=202)
async def receive_user_events(batch: UserEventBatch, bg: BackgroundTasks):
    """
    Receives batched user interaction events from the browser SDK.
    Normalizes each one and publishes to Kafka.
    """
    bg.add_task(_process_user_batch, batch)
    return {"accepted": len(batch.events)}


async def _process_user_batch(batch: UserEventBatch):
    user_id   = batch.user_id or f"anon:{batch.session_id}"
    source_id = f"usr:{user_id}"
    normalizer = UserBehaviorNormalizer()

    for raw in batch.events:
        raw_copy = dict(raw)
        raw_copy.setdefault("user_id", batch.user_id)
        raw_copy.setdefault("session_id", batch.session_id)
        
        result = normalizer.normalize(raw_copy, source_id)
        await _publish(result)


# ─────────────────────────────────────────────
# BROWSER ENVIRONMENT EVENTS  (JS errors, Web Vitals)
# ─────────────────────────────────────────────

@app.post("/perception/browser-events", status_code=202)
async def receive_browser_events(request: Request, bg: BackgroundTasks):
    """
    Receives browser environment signals:
    - window.onerror and unhandledrejection events
    - PerformanceObserver entries (Core Web Vitals)
    - Hydration failures (Next.js / React)
    """
    body = await request.json()
    bg.add_task(_process_browser_event, body)
    return {"accepted": True}


async def _process_browser_event(raw: dict):
    session_id = raw.get("session_id", "unknown")
    source_id = f"browser:{session_id}"
    normalizer = BrowserEventNormalizer()
    
    result = normalizer.normalize(raw, source_id)
    await _publish(result)


# ─────────────────────────────────────────────
# PROMETHEUS ALERTMANAGER WEBHOOK
# ─────────────────────────────────────────────

@app.post("/perception/prometheus-alerts", status_code=202)
async def receive_prometheus_alerts(request: Request, bg: BackgroundTasks):
    """
    Alertmanager webhook receiver.
    Alertmanager sends one POST per alert group.
    """
    body = await request.json()
    bg.add_task(_process_prometheus_payload, body)
    return {"accepted": len(body.get("alerts", []))}


async def _process_prometheus_payload(body: dict):
    normalizer = MetricNormalizer()
    alerts     = body.get("alerts", [])

    for alert in alerts:
        labels    = alert.get("labels", {})
        source_id = f"metric:{labels.get('job', labels.get('instance', 'unknown'))}"
        result    = normalizer.normalize(alert, source_id)
        await _publish(result)


# ─────────────────────────────────────────────
# SECURITY EVENTS  (WAF, Cloudflare, Fail2ban)
# ─────────────────────────────────────────────

@app.post("/perception/security-events", status_code=202)
async def receive_security_events(request: Request, bg: BackgroundTasks):
    """
    Security signal webhook endpoint.
    """
    body   = await request.json()
    source = request.headers.get("X-Security-Source", "unknown")
    bg.add_task(_process_security_event, body, source)
    return {"accepted": True}


async def _process_security_event(raw: dict, source: str):
    source_id = f"security:{source}"
    normalizer = SecurityEventNormalizer()
    
    raw_copy = dict(raw)
    raw_copy.setdefault("source_system", source)
    
    result = normalizer.normalize(raw_copy, source_id)
    await _publish(result)


# ─────────────────────────────────────────────
# AGENT EVENTS  (from cognitive sub-agents)
# ─────────────────────────────────────────────

@app.post("/perception/agent-events", status_code=202)
async def receive_agent_events(request: Request, bg: BackgroundTasks):
    """
    Receives events from cognitive sub-agents via REST.
    """
    body = await request.json()
    bg.add_task(_process_agent_event, body)
    return {"accepted": True}


async def _process_agent_event(raw: dict):
    agent_id  = raw.get("agent_id", "agent:unknown")
    source_id = agent_id if agent_id.startswith("agent:") else f"agent:{agent_id}"

    # Agent events are already fully structured CognitiveEvents or can be built directly
    event = CognitiveEvent(
        timestamp=datetime.now(timezone.utc),
        source_type=SourceType.AGENT_EVENT,
        source_id=source_id,
        event_type=raw.get("event_type", "action_completed"),
        severity=Severity(raw.get("severity", "info")),
        payload=raw.get("payload", raw),
        entity_refs=raw.get("entity_refs", [source_id]),
        confidence=1.0,
        tags=["agent_event", agent_id],
    )
    await _publish(event)

# ─────────────────────────────────────────────
# DEPLOYMENT WEBHOOKS  (GitHub, GitLab, ArgoCD)
# ─────────────────────────────────────────────

@app.post("/perception/deployment-events", status_code=202)
async def receive_deployment_events(request: Request, bg: BackgroundTasks):
    """GitHub/GitLab/ArgoCD deployment webhooks."""
    body       = await request.json()
    event_type = request.headers.get("X-GitHub-Event", request.headers.get("X-GitLab-Event", "deployment"))
    bg.add_task(_process_deployment_event, body, event_type)
    return {"accepted": True}


async def _process_deployment_event(raw: dict, github_event: str):
    deployment = raw.get("deployment", raw)
    service    = raw.get("repository", {}).get("name", "unknown")
    env        = deployment.get("environment", "production")

    type_map = {
        "deployment":  "deployment_started",
        "push":        "deployment_started",
        "release":     "deployment_completed",
    }
    evt_type = type_map.get(github_event, "deployment_started")

    event = CognitiveEvent(
        timestamp=datetime.now(timezone.utc),
        source_type=SourceType.LOG,  # deployments are normalized into operational logs
        source_id=f"svc:{service}",
        event_type=evt_type,
        severity=Severity.INFO,
        payload={"service": service, "environment": env, "raw": deployment},
        entity_refs=[f"svc:{service}"],
        confidence=0.99,
        tags=["deployment", env, service],
    )
    await _publish(event)


# ─────────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────────

@app.get("/health")
async def health():
    return {
        "status": "ok",
        "kafka": "connected" if producer and producer._sender else "disconnected",
    }
