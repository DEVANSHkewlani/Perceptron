import pytest
from pydantic import ValidationError
from reasoning.schema import DecisionObject, RootCauseHypothesis


def valid_decision(**overrides) -> dict:
    base = {
        "situation_assessment":    "Auth service latency spike detected.",
        "root_cause_hypothesis":   {"hypothesis": "Slow queries", "confidence": 0.88, "evidence": []},
        "recommended_action":      "restart_connection_pool",
        "action_parameters":       {"service_id": "svc:auth-service"},
        "confidence":              0.88,
        "requires_human_approval": False,
        "alternative_actions":     [],
        "reasoning_trace":         "Step 1: identified slow queries...",
    }
    base.update(overrides)
    return base


def test_valid_decision_parses():
    d = DecisionObject(**valid_decision())
    assert d.recommended_action == "restart_connection_pool"
    assert d.confidence == 0.88


def test_empty_action_rejected():
    with pytest.raises(ValidationError):
        DecisionObject(**valid_decision(recommended_action=""))


def test_confidence_out_of_range_rejected():
    with pytest.raises(ValidationError):
        DecisionObject(**valid_decision(confidence=1.5))


def test_low_confidence_auto_requires_approval():
    """Confidence below 0.65 must force requires_human_approval = True."""
    d = DecisionObject(**valid_decision(confidence=0.5, requires_human_approval=False))
    assert d.requires_human_approval is True


def test_to_episodic_record_shape():
    d = DecisionObject(**valid_decision())
    record = d.to_episodic_record({"situation_summary": "test"})
    assert record["event_type"] == "reasoning_completed"
    assert "decision" in record
    assert record["outcome"] is None
