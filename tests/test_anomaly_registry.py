import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock
from world_model.anomaly_registry import AnomalyRegistry


@pytest.fixture
def registry():
    r = AnomalyRegistry.__new__(AnomalyRegistry)
    r._redis = AsyncMock()
    r._redis.hgetall = AsyncMock(return_value={})
    r._redis.hset    = AsyncMock()
    r._open = {}
    r._REDIS_KEY = "wm:anomalies"
    return r


@pytest.mark.asyncio
async def test_anomaly_opened_on_high_severity_event(registry):
    await registry.process_event({
        "severity": "high", "event_type": "api_latency_spike",
        "confidence": 0.9, "timestamp": "2024-01-15T14:00:00Z",
        "entity_refs": ["svc:api"], "payload": {},
    })
    open_list = registry.get_open()
    assert len(open_list) == 1
    assert open_list[0].event_type == "api_latency_spike"
    assert open_list[0].status == "open"


@pytest.mark.asyncio
async def test_info_event_does_not_open_anomaly(registry):
    await registry.process_event({
        "severity": "info", "event_type": "deployment_completed",
        "confidence": 0.99, "timestamp": "2024-01-15T14:00:00Z",
        "entity_refs": ["svc:api"], "payload": {},
    })
    assert len(registry.get_open()) == 0


@pytest.mark.asyncio
async def test_resolved_event_closes_anomaly(registry):
    await registry.process_event({
        "severity": "critical", "event_type": "service_unreachable",
        "confidence": 0.97, "timestamp": "2024-01-15T14:00:00Z",
        "entity_refs": ["svc:api"], "payload": {},
    })
    assert len(registry.get_open()) == 1
    await registry.process_event({
        "severity": "info", "event_type": "service_health_restored",
        "confidence": 0.97, "timestamp": "2024-01-15T14:05:00Z",
        "entity_refs": ["svc:api"], "payload": {},
    })
    assert len(registry.get_open()) == 0


@pytest.mark.asyncio
async def test_severity_escalation_on_repeat_event(registry):
    # medium first, then critical for same entity+type
    for severity in ["medium", "critical"]:
        await registry.process_event({
            "severity": severity, "event_type": "cpu_spike",
            "confidence": 0.9, "timestamp": "2024-01-15T14:00:00Z",
            "entity_refs": ["metric:node-01"], "payload": {},
        })
    open_list = registry.get_open()
    assert len(open_list) == 1   # still one anomaly, not two
    assert open_list[0].severity == "critical"   # escalated


@pytest.mark.asyncio
async def test_stale_anomaly_auto_resolved(registry):
    from world_model.anomaly_registry import Anomaly
    old_ts = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
    anomaly = Anomaly(
        anomaly_id="anm_test1", entity_id="svc:old",
        event_type="api_latency_spike", severity="high",
        confidence=0.9, opened_at=old_ts, last_seen_at=old_ts,
        resolved_at=None, status="open",
    )
    registry._open["svc:old:api_latency_spike"] = anomaly
    await registry.expire_stale()
    assert len(registry.get_open()) == 0
