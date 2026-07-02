import pytest
from reasoning.fast_path import RuleEngine


@pytest.fixture
def engine():
    return RuleEngine()


def make_situation(event_type: str, severity: str, blast: int = 0) -> dict:
    return {
        "ranked_anomalies": [{
            "event_type": event_type, "severity": severity,
            "entity_id": "svc:test", "blast_radius_count": blast,
        }],
        "system_health": {"by_severity": {severity: 1}},
    }


def test_no_anomalies_returns_monitor_and_wait(engine):
    decision = engine.evaluate({"ranked_anomalies": []})
    assert decision is not None
    assert decision.recommended_action == "monitor_and_wait"


def test_consumer_lag_critical_fires_scale(engine):
    decision = engine.evaluate(make_situation("consumer_lag_critical", "critical"))
    assert decision.recommended_action == "scale_consumer_group"
    assert decision.confidence >= 0.90


def test_connection_pool_exhausted_fires_restart(engine):
    decision = engine.evaluate(
        make_situation("connection_pool_exhausted", "critical")
    )
    assert decision.recommended_action == "restart_connection_pool"


def test_disk_full_always_escalates(engine):
    decision = engine.evaluate(make_situation("disk_full", "critical"))
    assert decision.recommended_action == "escalate_to_human"
    assert decision.requires_human_approval is True


def test_novel_situation_returns_none(engine):
    """Unknown event type should return None — needs LLM."""
    decision = engine.evaluate(make_situation("some_novel_anomaly_xyz", "high"))
    assert decision is None


def test_fast_path_records_reasoning_trace(engine):
    decision = engine.evaluate(make_situation("consumer_lag_critical", "critical"))
    assert "FAST PATH" in decision.reasoning_trace
    assert "LLM call skipped" in decision.reasoning_trace
