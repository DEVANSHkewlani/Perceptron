import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from reasoning.engine import ReasoningEngine, ReasoningConfig
from reasoning.schema import DecisionObject, RootCauseHypothesis


def make_situation(event_type="consumer_lag_critical", severity="critical") -> dict:
    return {
        "situation_summary": "1 CRITICAL anomaly.",
        "ranked_anomalies": [{
            "event_type": event_type, "severity": severity,
            "entity_id": "queue:order-events", "confidence": 0.97,
            "blast_radius_count": 2,
        }],
        "system_health": {"by_severity": {severity: 1}, "total_entities": 5},
    }


@pytest.fixture
def mock_engine(tmp_path):
    # Write a minimal actions.yaml for the test
    actions_file = tmp_path / "actions.yaml"
    actions_file.write_text("""
actions:
  scale_consumer_group:
    description: "Scale consumer group"
    risk: low
    reversible: true
    requires_approval: false
    parameters:
      queue_id: {type: string, required: true}
      consumer_group: {type: string, required: true}
      instance_delta: {type: integer, required: true}
  escalate_to_human:
    description: "Escalate to human"
    risk: low
    reversible: true
    requires_approval: false
    parameters:
      reason: {type: string, required: true}
      urgency: {type: string, required: true}
  monitor_and_wait:
    description: "Monitor and wait"
    risk: low
    reversible: true
    requires_approval: false
    parameters:
      duration_m: {type: integer, required: true}
""")
    cfg = ReasoningConfig(actions_yaml_path=str(actions_file))
    engine = ReasoningEngine.__new__(ReasoningEngine)
    engine.cfg         = cfg
    engine.agent_id    = "agent:reasoning-engine"
    engine.domain      = "general"
    engine.rule_engine = MagicMock()
    engine.ctx_builder = MagicMock()
    engine.llm_client  = AsyncMock()
    engine.prompt_bld  = MagicMock()
    engine._http       = AsyncMock()
    engine._actions    = {
        "scale_consumer_group": {"requires_approval": False, "risk": "low"},
        "escalate_to_human":    {"requires_approval": False, "risk": "low"},
        "monitor_and_wait":     {"requires_approval": False, "risk": "low"},
    }
    from reasoning.actions import ActionRegistry
    engine._action_reg = ActionRegistry(engine._actions)
    return engine


@pytest.mark.asyncio
async def test_fast_path_decision_bypasses_llm(mock_engine):
    """When fast path matches, LLM should not be called."""
    fast_decision = DecisionObject(
        situation_assessment="Fast path.",
        root_cause_hypothesis=RootCauseHypothesis(hypothesis="rule", confidence=0.95, evidence=[]),
        recommended_action="scale_consumer_group",
        action_parameters={"queue_id": "queue:order-events", "consumer_group": "default", "instance_delta": 3},
        confidence=0.95, requires_human_approval=False,
        reasoning_trace="[FAST PATH]",
    )
    mock_engine.rule_engine.evaluate.return_value = fast_decision

    # Mock _fetch_situation
    mock_http_resp = AsyncMock()
    mock_http_resp.status_code = 200
    mock_http_resp.json = MagicMock(return_value=make_situation())
    mock_engine._http.get = AsyncMock(return_value=mock_http_resp)
    mock_engine._http.post = AsyncMock()

    decision = await mock_engine.reason()

    mock_engine.llm_client.call_with_schema_correction.assert_not_called()
    assert decision.recommended_action == "scale_consumer_group"


@pytest.mark.asyncio
async def test_llm_called_when_no_fast_path(mock_engine):
    """When fast path returns None, LLM client must be called."""
    mock_engine.rule_engine.evaluate.return_value = None

    mock_http_resp = AsyncMock()
    mock_http_resp.status_code = 200
    mock_http_resp.json = MagicMock(return_value=make_situation("novel_anomaly_xyz", "high"))
    mock_engine._http.get  = AsyncMock(return_value=mock_http_resp)
    mock_engine._http.post = AsyncMock()

    mock_engine.ctx_builder.get_past_experiences   = AsyncMock(return_value=[])
    mock_engine.ctx_builder.get_activated_playbooks= AsyncMock(return_value=[])
    mock_engine.ctx_builder.format_experiences      = MagicMock(return_value="none")
    mock_engine.prompt_bld.build                    = MagicMock(return_value="test prompt")

    llm_decision = DecisionObject(
        situation_assessment="LLM decision.",
        root_cause_hypothesis=RootCauseHypothesis(hypothesis="novel", confidence=0.8, evidence=[]),
        recommended_action="monitor_and_wait",
        action_parameters={"duration_m": 10},
        confidence=0.80, requires_human_approval=False,
        reasoning_trace="LLM reasoning trace.",
    )
    mock_engine.llm_client.call_with_schema_correction = AsyncMock(return_value=llm_decision)

    decision = await mock_engine.reason()

    mock_engine.llm_client.call_with_schema_correction.assert_called_once()
    assert decision.recommended_action == "monitor_and_wait"


@pytest.mark.asyncio
async def test_invalid_llm_action_replaced_with_escalate(mock_engine):
    """If LLM returns an action not in registry, replace with escalate_to_human."""
    mock_engine.rule_engine.evaluate.return_value = None
    mock_http_resp = AsyncMock()
    mock_http_resp.status_code = 200
    mock_http_resp.json = MagicMock(return_value=make_situation("mystery_event", "high"))
    mock_engine._http.get  = AsyncMock(return_value=mock_http_resp)
    mock_engine._http.post = AsyncMock()
    mock_engine.ctx_builder.get_past_experiences   = AsyncMock(return_value=[])
    mock_engine.ctx_builder.get_activated_playbooks= AsyncMock(return_value=[])
    mock_engine.ctx_builder.format_experiences      = MagicMock(return_value="none")
    mock_engine.prompt_bld.build                    = MagicMock(return_value="test")

    bad_decision = DecisionObject(
        situation_assessment="test",
        root_cause_hypothesis=RootCauseHypothesis(hypothesis="test", confidence=0.8, evidence=[]),
        recommended_action="invented_action_that_does_not_exist",
        action_parameters={}, confidence=0.8, requires_human_approval=False,
        reasoning_trace="test",
    )
    mock_engine.llm_client.call_with_schema_correction = AsyncMock(return_value=bad_decision)

    decision = await mock_engine.reason()
    assert decision.recommended_action == "escalate_to_human"
    assert decision.requires_human_approval is True


@pytest.mark.asyncio
async def test_returns_none_when_no_significant_anomalies(mock_engine):
    mock_http_resp = AsyncMock()
    mock_http_resp.status_code = 200
    mock_http_resp.json = MagicMock(return_value={
        "ranked_anomalies": [],
        "system_health": {"by_severity": {"info": 2}},
    })
    mock_engine._http.get = AsyncMock(return_value=mock_http_resp)
    decision = await mock_engine.reason()
    assert decision is None
