import pytest
from unittest.mock import AsyncMock, patch
from feedback.verifier import OutcomeVerifier

CONFIG = {
    "defaults": {"verification_delay_s": 0},
    "action_overrides": {"send_alert": {"verification_delay_s": 0}}
}


@pytest.fixture
def verifier():
    return OutcomeVerifier("http://localhost:8092", CONFIG)


def make_event(action="scale_consumer_group"):
    return {"payload": {"action": action, "entity_refs": ["queue:orders"]}}


@pytest.mark.asyncio
async def test_success_when_anomaly_count_drops(verifier):
    with patch.object(OutcomeVerifier, "_count_anomalies", AsyncMock(side_effect=[4, 1])), \
         patch.object(OutcomeVerifier, "_get_entity_health", AsyncMock(side_effect=["critical", "healthy"])):
        r = await verifier.verify("plan_1", "scale_consumer_group", make_event())
    assert r.outcome == "success"
    assert r.anomalies_before == 4
    assert r.anomalies_after  == 1


@pytest.mark.asyncio
async def test_partial_when_anomaly_count_unchanged(verifier):
    with patch.object(OutcomeVerifier, "_count_anomalies", AsyncMock(side_effect=[3, 3])), \
         patch.object(OutcomeVerifier, "_get_entity_health", AsyncMock(side_effect=["degraded", "degraded"])):
        r = await verifier.verify("plan_2", "restart_connection_pool", make_event())
    assert r.outcome == "partial"


@pytest.mark.asyncio
async def test_failure_when_anomaly_count_rises(verifier):
    with patch.object(OutcomeVerifier, "_count_anomalies", AsyncMock(side_effect=[1, 5])), \
         patch.object(OutcomeVerifier, "_get_entity_health", AsyncMock(return_value=None)):
        r = await verifier.verify("plan_3", "restart_service", make_event())
    assert r.outcome == "failure"
    assert r.anomalies_after > r.anomalies_before


@pytest.mark.asyncio
async def test_entity_health_upgrade_counts_as_success(verifier):
    with patch.object(OutcomeVerifier, "_count_anomalies", AsyncMock(side_effect=[2, 2])), \
         patch.object(OutcomeVerifier, "_get_entity_health", AsyncMock(side_effect=["degraded", "healthy"])):
        r = await verifier.verify("plan_4", "scale_read_replicas", make_event())
    assert r.outcome == "success"


@pytest.mark.asyncio
async def test_config_delay_applied_per_action(verifier):
    assert verifier._get_delay("send_alert") == 0
    assert verifier._get_delay("unknown_action") == 0


@pytest.mark.asyncio
async def test_result_contains_all_required_fields(verifier):
    with patch.object(OutcomeVerifier, "_count_anomalies", AsyncMock(side_effect=[2, 0])), \
         patch.object(OutcomeVerifier, "_get_entity_health", AsyncMock(return_value="healthy")):
        r = await verifier.verify("plan_5", "scale_consumer_group", make_event())
    assert r.plan_id == "plan_5"
    assert r.verified_at is not None
    assert "→" in r.notes
