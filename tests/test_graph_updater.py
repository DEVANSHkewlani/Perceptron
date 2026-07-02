import pytest
from unittest.mock import AsyncMock, MagicMock
from feedback.graph_updater import GraphUpdater

CONFIG = {
    "defaults": {
        "confidence_increment": 0.04,
        "confidence_decrement": 0.05,
        "confidence_min": 0.10,
        "confidence_max": 0.99
    }
}


@pytest.fixture
def updater():
    u = GraphUpdater.__new__(GraphUpdater)
    u._inc = 0.04
    u._dec = 0.05
    u._min = 0.10
    u._max = 0.99
    mock_session = AsyncMock()
    mock_session.run = AsyncMock()
    driver = MagicMock()
    driver.session.return_value.__aenter__ = AsyncMock(return_value=mock_session)
    driver.session.return_value.__aexit__  = AsyncMock(return_value=False)
    u._driver = driver
    u._mock_session = mock_session
    return u


@pytest.mark.asyncio
async def test_success_calls_merge_with_increment(updater):
    await updater.update(
        "scale_consumer_group",
        {"anomaly_type": "consumer_lag_critical"},
        "success"
    )
    updater._mock_session.run.assert_called_once()
    cypher = updater._mock_session.run.call_args[0][0]
    assert "MERGE"         in cypher
    assert "RESOLVES"      in cypher
    assert "success_count" in cypher
    kwargs = updater._mock_session.run.call_args.kwargs
    assert kwargs["inc"] == 0.04
    assert kwargs["max"] == 0.99


@pytest.mark.asyncio
async def test_failure_calls_match_with_decrement(updater):
    await updater.update(
        "restart_service",
        {"event_type": "service_health_degraded"},
        "failure"
    )
    cypher = updater._mock_session.run.call_args[0][0]
    assert "MATCH"          in cypher
    assert "failure_count"  in cypher
    kwargs = updater._mock_session.run.call_args.kwargs
    assert kwargs["dec"] == 0.05
    assert kwargs["min"] == 0.10


@pytest.mark.asyncio
async def test_anomaly_type_falls_back_to_event_type(updater):
    await updater.update(
        "monitor_and_wait",
        {"event_type": "cpu_spike"},
        "success"
    )
    kwargs = updater._mock_session.run.call_args.kwargs
    assert "cpu_spike" in kwargs["concept_id"]


@pytest.mark.asyncio
async def test_unknown_anomaly_type_uses_fallback(updater):
    await updater.update("send_alert", {}, "success")
    kwargs = updater._mock_session.run.call_args.kwargs
    assert "unknown_anomaly" in kwargs["concept_id"]
