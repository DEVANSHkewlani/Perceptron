import pytest
from unittest.mock import AsyncMock, MagicMock
from world_model.entity_registry import EntityRegistry


@pytest.fixture
def registry():
    r = EntityRegistry.__new__(EntityRegistry)
    r._redis = AsyncMock()
    r._redis.hgetall = AsyncMock(return_value={})
    r._redis.hset    = AsyncMock()
    r._store = {}
    r._REDIS_KEY = "wm:entity_registry"
    return r


@pytest.mark.asyncio
async def test_upsert_creates_new_entity(registry):
    event = {
        "severity":    "high",
        "event_type":  "api_latency_spike",
        "confidence":  0.95,
        "timestamp":   "2024-01-15T14:00:00Z",
        "entity_refs": ["svc:auth-service"],
        "payload":     {"latency_ms": 3500},
    }
    await registry.upsert_from_event(event)
    entity = registry.get("svc:auth-service")
    assert entity is not None
    assert entity.health_status == "degraded"
    assert entity.last_event_type == "api_latency_spike"
    assert entity.severity_score == 4


@pytest.mark.asyncio
async def test_resolved_event_restores_healthy_status(registry):
    # First inject a degrading event
    await registry.upsert_from_event({
        "severity": "high", "event_type": "service_health_degraded",
        "confidence": 0.9, "timestamp": "2024-01-15T14:00:00Z",
        "entity_refs": ["svc:api-gateway"], "payload": {},
    })
    assert registry.get("svc:api-gateway").health_status == "degraded"
    # Then inject a resolution event
    await registry.upsert_from_event({
        "severity": "info", "event_type": "service_health_restored",
        "confidence": 0.97, "timestamp": "2024-01-15T14:05:00Z",
        "entity_refs": ["svc:api-gateway"], "payload": {},
    })
    assert registry.get("svc:api-gateway").health_status == "healthy"


@pytest.mark.asyncio
async def test_multiple_entity_refs_all_updated(registry):
    event = {
        "severity": "critical", "event_type": "database_connection_timeout",
        "confidence": 0.82, "timestamp": "2024-01-15T14:00:00Z",
        "entity_refs": ["svc:auth-service", "db:postgres-primary"],
        "payload": {},
    }
    await registry.upsert_from_event(event)
    assert registry.get("svc:auth-service").health_status == "critical"
    assert registry.get("db:postgres-primary").health_status == "critical"


@pytest.mark.asyncio
async def test_temporal_state_update_writes_fields(registry):
    await registry.upsert_from_event({
        "severity": "medium", "event_type": "cpu_spike",
        "confidence": 0.9, "timestamp": "2024-01-15T14:00:00Z",
        "entity_refs": ["metric:node-01"], "payload": {},
    })
    await registry.update_temporal_state("metric:node-01", {
        "current_value": 87.5, "rate_of_change": 2.1,
        "trend_direction": "rising", "deviation_from_baseline": 3.2,
    })
    e = registry.get("metric:node-01")
    assert e.current_value == 87.5
    assert e.trend_direction == "rising"
    assert e.deviation_z == 3.2


@pytest.mark.asyncio
async def test_get_degraded_sorts_by_severity(registry):
    for entity_id, severity in [
        ("svc:a", "medium"), ("svc:b", "critical"), ("svc:c", "high")
    ]:
        await registry.upsert_from_event({
            "severity": severity, "event_type": "service_health_degraded",
            "confidence": 0.9, "timestamp": "2024-01-15T14:00:00Z",
            "entity_refs": [entity_id], "payload": {},
        })
    degraded = registry.get_degraded()
    assert degraded[0].entity_id == "svc:b"   # critical first
