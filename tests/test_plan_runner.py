import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from planning.schema import Plan, PlanStep, PlanStatus, StepStatus
from execution.runner import PlanRunner
from execution.handlers.base import ExecutionResult


def make_plan(*step_ids: str) -> Plan:
    steps = [
        PlanStep(step_id=sid, action="send_alert",
                 description=f"Step {sid}", parameters={"message": "test"})
        for sid in step_ids
    ]
    return Plan(decision_id="dec_test", goal="test", steps=steps)


def make_runner(success: bool = True, delay: float = 0.0) -> PlanRunner:
    mock_handler = AsyncMock()
    
    async def mock_run(*args, **kwargs):
        if delay > 0:
            await asyncio.sleep(delay)
        return ExecutionResult(
            success=success, output={"sent": True},
            error=None if success else "handler error",
        )
        
    mock_handler.run = AsyncMock(side_effect=mock_run)
    store    = AsyncMock()
    
    async def mock_on_step_completed(plan, step):
        if step.status == StepStatus.FAILED:
            if step.on_failure == "rollback":
                plan.status = PlanStatus.ROLLED_BACK
            elif step.on_failure == "abort":
                plan.status = PlanStatus.ABORTED
        elif plan.is_complete():
            plan.status = PlanStatus.SUCCEEDED

    monitor  = AsyncMock()
    monitor.on_step_completed = AsyncMock(side_effect=mock_on_step_completed)
    registry = MagicMock()
    registry.get.return_value = mock_handler
    return PlanRunner(store, monitor, registry)



@pytest.mark.asyncio
async def test_single_step_plan_succeeds():
    runner = make_runner(success=True)
    plan   = make_plan("s1")
    result = await runner.execute(plan)
    assert result.status == PlanStatus.SUCCEEDED
    assert plan.steps[0].status == StepStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_failed_step_marks_plan_failed():
    runner = make_runner(success=False)
    plan   = make_plan("s1")
    plan.steps[0].on_failure = "abort"
    result = await runner.execute(plan)
    assert result.status == PlanStatus.ABORTED


@pytest.mark.asyncio
async def test_parallel_steps_execute_concurrently():
    # s1 and s2 have no dependencies — both should execute
    runner = make_runner(success=True)
    plan = Plan(decision_id="dec", goal="test", steps=[
        PlanStep(step_id="s1", action="send_alert", description="A", parameters={"message": "m"}),
        PlanStep(step_id="s2", action="send_alert", description="B", parameters={"message": "m"}),
    ])
    result = await runner.execute(plan)
    assert result.status == PlanStatus.SUCCEEDED
    assert all(s.status == StepStatus.SUCCEEDED for s in result.steps)


@pytest.mark.asyncio
async def test_dependent_step_not_run_if_dependency_failed():
    runner = make_runner(success=False)
    plan = Plan(decision_id="dec", goal="test", steps=[
        PlanStep(step_id="s1", action="send_alert", description="A",
                 parameters={"message": "m"}, on_failure="abort"),
        PlanStep(step_id="s2", action="send_alert", description="B",
                 parameters={"message": "m"}, depends_on=["s1"]),
    ])
    result = await runner.execute(plan)
    # s2 should remain PENDING because s1 failed and plan aborted
    assert plan.steps[1].status == StepStatus.PENDING


@pytest.mark.asyncio
async def test_monitor_called_after_each_step():
    runner = make_runner(success=True)
    plan   = make_plan("s1", "s2")
    plan.steps[1].depends_on = ["s1"]
    await runner.execute(plan)
    assert runner.monitor.on_step_completed.call_count == 2


@pytest.mark.asyncio
async def test_runner_ignores_nonexistent_handler():
    runner = make_runner(success=True)
    runner.registry.get.return_value = None # No handler registered
    plan = make_plan("s1")
    result = await runner.execute(plan)
    assert plan.steps[0].status == StepStatus.FAILED
    assert "No handler registered" in plan.steps[0].error
    assert runner.monitor.on_step_completed.called


@pytest.mark.asyncio
async def test_step_timeout_causes_failure():
    # Handler takes 2 seconds, timeout is 1 second
    runner = make_runner(success=True, delay=2.0)
    plan = Plan(decision_id="dec", goal="test", steps=[
        PlanStep(step_id="s1", action="send_alert", description="Slow", parameters={}, timeout_s=1)
    ])
    await runner.execute(plan)
    assert plan.steps[0].status == StepStatus.FAILED
    assert "timed out after" in plan.steps[0].error


@pytest.mark.asyncio
async def test_failed_step_triggers_rollback():
    runner = make_runner(success=False)
    plan = Plan(decision_id="dec", goal="test", steps=[
        PlanStep(step_id="s1", action="scale", description="A", parameters={}, on_failure="rollback")
    ])
    await runner.execute(plan)
    # Runner should stop loop and monitor should receive the event
    assert plan.steps[0].status == StepStatus.FAILED
    assert runner.monitor.on_step_completed.called
