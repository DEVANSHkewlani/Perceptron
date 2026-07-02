import pytest
from unittest.mock import MagicMock
from planning.generator import PlanGenerator
from planning.schema import PlanStatus


def make_generator(has_template=True):
    store = MagicMock()
    store.get.return_value = {
        "goal": "Reduce lag on {queue_id}",
        "preconditions": [],
        "steps": [
            {"id": "s1", "action": "scale_consumer_group",
             "description": "Scale {queue_id}", "timeout_s": 60, "on_failure": "rollback"},
        ],
        "success_criteria": [{"type": "event_received", "check": "action_completed"}],
        "rollback": [], "approval_gates": [],
    } if has_template else None
    return PlanGenerator(store)


@pytest.mark.asyncio
async def test_template_strategy_used_when_template_exists():
    gen = make_generator(has_template=True)
    decision = {"recommended_action": "scale_consumer_group",
                "action_parameters": {"queue_id": "queue:orders", "instance_delta": 3, "consumer_group": "g1"},
                "requires_human_approval": False}
    plan = await gen.generate(decision)
    assert plan.strategy == "template_based"
    assert "queue:orders" in plan.goal


@pytest.mark.asyncio
async def test_rule_based_fallback_creates_single_step_plan():
    gen = make_generator(has_template=False)
    decision = {"recommended_action": "some_new_action",
                "action_parameters": {}, "requires_human_approval": False,
                "situation_assessment": "test"}
    plan = await gen.generate(decision)
    assert plan.strategy == "rule_based"
    assert any(s.action == "some_new_action" for s in plan.steps)


@pytest.mark.asyncio
async def test_approval_gate_added_when_required():
    gen = make_generator(has_template=False)
    decision = {"recommended_action": "restart_service",
                "action_parameters": {"service_id": "svc:auth"},
                "requires_human_approval": True,
                "situation_assessment": "auth down"}
    plan = await gen.generate(decision)
    assert len(plan.approval_gates) > 0
    assert any(s.is_approval_gate for s in plan.steps)


@pytest.mark.asyncio
async def test_parameters_interpolated_in_goal():
    gen = make_generator(has_template=True)
    decision = {"recommended_action": "scale_consumer_group",
                "action_parameters": {"queue_id": "queue:payments",
                                       "consumer_group": "payment-processor",
                                       "instance_delta": 5},
                "requires_human_approval": False}
    plan = await gen.generate(decision)
    assert "queue:payments" in plan.goal
    assert "{queue_id}" not in plan.goal   # template vars resolved


@pytest.mark.asyncio
async def test_parameters_interpolated_in_step_description():
    gen = make_generator(has_template=True)
    decision = {"recommended_action": "scale_consumer_group",
                "action_parameters": {"queue_id": "queue:metrics",
                                       "consumer_group": "metrics-processor",
                                       "instance_delta": 2},
                "requires_human_approval": False}
    plan = await gen.generate(decision)
    step = next(s for s in plan.steps if s.step_id == "s1")
    assert "queue:metrics" in step.description
    assert "{queue_id}" not in step.description

