import pytest
from planning.schema import Plan, PlanStep, PlanStatus, StepStatus


def make_plan(**kwargs) -> Plan:
    steps = [
        PlanStep(step_id="s1", action="scale_consumer_group",
                 description="Scale", parameters={"queue_id": "q:test"}),
        PlanStep(step_id="s2", action="send_alert",
                 description="Notify", depends_on=["s1"]),
    ]
    defaults = dict(decision_id="dec_test", goal="Test goal", steps=steps)
    defaults.update(kwargs)
    return Plan(**defaults)


def test_plan_created_with_defaults():
    plan = make_plan()
    assert plan.status == PlanStatus.CREATED
    assert plan.plan_id.startswith("plan_")
    assert len(plan.steps) == 2


def test_ready_steps_returns_pending_with_no_deps():
    plan = make_plan()
    ready = plan.ready_steps()
    assert len(ready) == 1      # only s1 — s2 depends on s1
    assert ready[0].step_id == "s1"


def test_ready_steps_after_s1_succeeds():
    plan = make_plan()
    plan.steps[0].status = StepStatus.SUCCEEDED
    ready = plan.ready_steps()
    assert len(ready) == 1
    assert ready[0].step_id == "s2"


def test_is_complete_when_all_steps_succeeded():
    plan = make_plan()
    for s in plan.steps:
        s.status = StepStatus.SUCCEEDED
    assert plan.is_complete() is True


def test_is_not_complete_when_step_pending():
    plan = make_plan()
    plan.steps[0].status = StepStatus.SUCCEEDED
    assert plan.is_complete() is False


def test_is_complete_when_steps_skipped():
    plan = make_plan()
    plan.steps[0].status = StepStatus.SUCCEEDED
    plan.steps[1].status = StepStatus.SKIPPED
    assert plan.is_complete() is True

