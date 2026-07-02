import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from temporal.graph_bridge import GraphTemporalBridge


@pytest.fixture
def bridge():
    b = GraphTemporalBridge.__new__(GraphTemporalBridge)
    b._driver = MagicMock()
    mock_session = AsyncMock()
    mock_session.run = MagicMock()  # AsyncMock isn't strictly necessary for session.run if mocked correctly, but using AsyncMock is safer. Let's make mock_session.run an AsyncMock
    mock_session.run = AsyncMock()
    b._driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    b._driver.session.return_value.__aexit__  = AsyncMock(return_value=False)
    b._mock_session = mock_session
    return b


@pytest.mark.asyncio
async def test_correlation_pattern_calls_session_run(bridge):
    """A correlation pattern must call Neo4j session.run()."""
    pattern = {
        "pattern_type": "correlation",
        "entity_id":    "svc:auth",
        "entity_id_b":  "db:postgres",
        "severity":     "info",
        "confidence":   0.92,
        "details": {
            "event_type_a": "api_latency_spike",
            "event_type_b": "slow_query_detected",
            "pearson_r": 0.92, "p_value": 0.001, "sample_count": 40,
        },
    }
    await bridge.apply_pattern(pattern)
    bridge._mock_session.run.assert_called_once()
    cypher = bridge._mock_session.run.call_args[0][0]
    assert "HISTORICALLY_CORRELATED" in cypher


@pytest.mark.asyncio
async def test_recurrence_pattern_creates_concept_node(bridge):
    pattern = {
        "pattern_type": "recurrence",
        "entity_id":    "svc:api-gateway",
        "severity":     "medium",
        "confidence":   0.85,
        "details": {
            "event_type": "deployment_started",
            "hour_of_week": 14,
            "past_count": 5,
        },
    }
    await bridge.apply_pattern(pattern)
    bridge._mock_session.run.assert_called_once()
    cypher = bridge._mock_session.run.call_args[0][0]
    assert "Concept" in cypher
    assert "TRIGGERS" in cypher


@pytest.mark.asyncio
async def test_spike_pattern_updates_entity_status(bridge):
    pattern = {
        "pattern_type": "spike",
        "entity_id":    "svc:auth-service",
        "severity":     "high",
        "confidence":   0.95,
        "details": {
            "event_type": "api_latency_spike",
            "z_score": 3.5,
        },
    }
    await bridge.apply_pattern(pattern)
    bridge._mock_session.run.assert_called_once()
    cypher = bridge._mock_session.run.call_args[0][0]
    assert "last_anomaly_type" in cypher
    assert "anomaly_severity" in cypher
