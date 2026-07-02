import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone
from world_model.anomaly_registry import Anomaly
from world_model.situation_assessor import SituationAssessor


def make_anomaly(severity="high", entity_id="svc:auth"):
    ts = datetime.now(timezone.utc).isoformat()
    return Anomaly(
        anomaly_id="anm_test", entity_id=entity_id,
        event_type="api_latency_spike", severity=severity,
        confidence=0.92, opened_at=ts, last_seen_at=ts,
        resolved_at=None, status="open",
    )


@pytest.fixture
def mock_wm():
    wm = MagicMock()
    wm.anomalies.get_open     = MagicMock(return_value=[make_anomaly()])
    wm.anomalies.count_by_severity = MagicMock(return_value={"high": 1})
    wm.entities.get_all       = MagicMock(return_value=[])
    wm.entities.get_degraded  = MagicMock(return_value=[])
    wm.get_blast_radius       = AsyncMock(return_value=[])
    wm.get_causal_chain       = AsyncMock(return_value={
        "dependency_chain": [], "correlated_entities": []
    })
    wm.get_prediction         = AsyncMock(return_value=[])
    return wm


@pytest.mark.asyncio
async def test_assess_returns_required_keys(mock_wm):
    assessor = SituationAssessor(mock_wm)
    result = await assessor.assess(top_n=3)
    for key in [
        "situation_summary", "ranked_anomalies", "system_health",
        "causal_insights", "predictions", "uncertainty_notes", "assessed_at",
    ]:
        assert key in result, f"Missing key: {key}"


@pytest.mark.asyncio
async def test_critical_anomaly_ranks_above_medium(mock_wm):
    crit = make_anomaly(severity="critical", entity_id="svc:a")
    med  = make_anomaly(severity="medium",   entity_id="svc:b")
    mock_wm.anomalies.get_open.return_value = [med, crit]   # wrong order
    assessor = SituationAssessor(mock_wm)
    result = await assessor.assess()
    # Critical should be ranked first regardless of input order
    assert result["ranked_anomalies"][0]["severity"] == "critical"


@pytest.mark.asyncio
async def test_blast_radius_inflates_score(mock_wm):
    # Anomaly with large blast radius should outscore same-severity with small one
    mock_wm.anomalies.get_open.return_value = [
        make_anomaly("high", "svc:a"),
        make_anomaly("high", "svc:b"),
    ]
    # svc:a has 10 affected entities, svc:b has 0
    mock_wm.get_blast_radius = AsyncMock(side_effect=[
        [{"entity_id": f"svc:{i}", "hop_distance": 1, "current_health": "healthy", "entity_type":"svc", "relationship":"DEPENDS_ON"} for i in range(10)],
        [],
    ])
    assessor = SituationAssessor(mock_wm)
    result = await assessor.assess()
    # svc:a with blast_radius=10 should rank higher
    assert result["ranked_anomalies"][0]["entity_id"] == "svc:a"


@pytest.mark.asyncio
async def test_empty_world_returns_no_anomalies_summary(mock_wm):
    mock_wm.anomalies.get_open.return_value = []
    mock_wm.anomalies.count_by_severity.return_value = {}
    assessor = SituationAssessor(mock_wm)
    result = await assessor.assess()
    assert "No active anomalies" in result["situation_summary"]
    assert result["ranked_anomalies"] == []
