import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone
from world_model.core import WorldModel, WorldModelConfig
from world_model.entity_registry import EntityState
from world_model.anomaly_registry import Anomaly


@pytest.fixture
def config():
    return WorldModelConfig(
        kafka_bootstrap="localhost:9092",
        redis_url="redis://localhost:6379",
        neo4j_uri="bolt://localhost:7687",
    )


@pytest.fixture
def model(config):
    # Create the model using new and mock out its connections
    m = WorldModel(config)
    m.entities = MagicMock()
    m.entities.connect = AsyncMock()
    m.entities.disconnect = AsyncMock()
    m.entities.get_all = MagicMock(return_value=[])
    m.entities.get_degraded = MagicMock(return_value=[])
    m.entities.get = MagicMock(return_value=None)
    m.entities.upsert_from_event = AsyncMock()
    m.entities.update_temporal_state = AsyncMock()

    m.anomalies = MagicMock()
    m.anomalies.connect = AsyncMock()
    m.anomalies.disconnect = AsyncMock()
    m.anomalies.process_event = AsyncMock()
    m.anomalies.get_open = MagicMock(return_value=[])
    m.anomalies.count_by_severity = MagicMock(return_value={})

    m.causal = MagicMock()
    m.causal.connect = AsyncMock()
    m.causal.disconnect = AsyncMock()
    m.causal.get_blast_radius = AsyncMock(return_value=[])
    m.causal.get_causal_chain = AsyncMock(return_value=[])
    m.causal.get_correlation_partners = AsyncMock(return_value=[])
    return m


@pytest.mark.asyncio
async def test_world_model_lifecycle(model):
    await model.start()
    assert model._running is True
    assert model._http is not None
    await model.stop()
    assert model._running is False
    model.entities.connect.assert_called_once()
    model.anomalies.connect.assert_called_once()
    model.causal.connect.assert_called_once()
    model.entities.disconnect.assert_called_once()
    model.anomalies.disconnect.assert_called_once()
    model.causal.disconnect.assert_called_once()


@pytest.mark.asyncio
async def test_process_event_calls_registries(model):
    event = {"event_type": "cpu_spike", "severity": "high"}
    await model._process_event(event)
    model.entities.upsert_from_event.assert_called_once_with(event)
    model.anomalies.process_event.assert_called_once_with(event)


@pytest.mark.asyncio
async def test_get_current_situation(model):
    now_str = datetime.now(timezone.utc).isoformat()
    anomaly = Anomaly(
        anomaly_id="anm_123", entity_id="svc:auth", event_type="cpu_spike",
        severity="high", confidence=0.9, opened_at=now_str, last_seen_at=now_str,
        resolved_at=None, status="open"
    )
    entity = EntityState(
        entity_id="svc:auth", entity_type="svc", health_status="degraded",
        last_seen=now_str, last_event_type="cpu_spike", last_severity="high",
        severity_score=4, confidence=0.9
    )
    model.anomalies.get_open.return_value = [anomaly]
    model.anomalies.count_by_severity.return_value = {"high": 1}
    model.entities.get_degraded.return_value = [entity]

    situation = model.get_current_situation(top_n=3)
    assert situation["anomaly_count"] == {"high": 1}
    assert len(situation["critical_entities"]) == 1
    assert situation["critical_entities"][0]["id"] == "svc:auth"
    assert len(situation["top_anomalies"]) == 1
    assert situation["top_anomalies"][0]["id"] == "anm_123"


@pytest.mark.asyncio
async def test_get_entity_state(model):
    now_str = datetime.now(timezone.utc).isoformat()
    entity = EntityState(
        entity_id="svc:auth", entity_type="svc", health_status="healthy",
        last_seen=now_str, last_event_type="cpu_spike", last_severity="high",
        severity_score=4, confidence=0.9
    )
    model.entities.get.return_value = entity
    anomaly = Anomaly(
        anomaly_id="anm_123", entity_id="svc:auth", event_type="cpu_spike",
        severity="high", confidence=0.9, opened_at=now_str, last_seen_at=now_str,
        resolved_at=None, status="open"
    )
    model.anomalies.get_open.return_value = [anomaly]

    state = model.get_entity_state("svc:auth")
    assert state is not None
    assert state["entity_id"] == "svc:auth"
    assert "cpu_spike" in state["open_anomalies"]


@pytest.mark.asyncio
async def test_get_blast_radius(model):
    from world_model.causal_engine import BlastRadiusResult
    result = BlastRadiusResult(affected_entity_id="svc:auth", entity_type="svc", relationship_type="DEPENDS_ON", hop_distance=1)
    model.causal.get_blast_radius.return_value = [result]
    entity = EntityState(
        entity_id="svc:auth", entity_type="svc", health_status="degraded",
        last_seen="", last_event_type="", last_severity="", severity_score=1, confidence=1.0
    )
    model.entities.get.return_value = entity

    blast = await model.get_blast_radius("db:postgres")
    assert len(blast) == 1
    assert blast[0]["entity_id"] == "svc:auth"
    assert blast[0]["current_health"] == "degraded"
