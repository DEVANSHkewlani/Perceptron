import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timezone
from planning.schema import Plan, PlanStep, PlanStatus, StepStatus, SuccessCriterion, RollbackStep
from planning.monitor import PlanMonitor


@pytest.fixture
def mock_producer():
    p = AsyncMock()
    p.send_and_wait = AsyncMock()
    return p


@pytest.fixture
def monitor(mock_producer):
    m = PlanMonitor(kafka_bootstrap="localhost:9092", world_model_url="http://localhost:8092")
    m._producer = mock_producer
    return m


@pytest.mark.asyncio
async def test_on_step_completed_succeeded_check_criteria_fail(monitor):
    plan = Plan(
        decision_id="dec_123",
        goal="Test goal",
        success_criteria=[SuccessCriterion(type="world_model_query", check="anomaly:consumer_lag_critical resolved")]
    )
    step = PlanStep(step_id="s1", action="scale", description="scale", status=StepStatus.SUCCEEDED)
    
    mock_resp = MagicMock()
    mock_resp.json.return_value = [{"event_type": "consumer_lag_critical"}]
    monitor._http.get = AsyncMock(return_value=mock_resp)
    
    await monitor.on_step_completed(plan, step)
    
    assert plan.status == PlanStatus.CREATED
    assert monitor._producer.send_and_wait.call_count == 1
    args, kwargs = monitor._producer.send_and_wait.call_args
    assert args[0] == "cognitive.events"


@pytest.mark.asyncio
async def test_on_step_completed_succeeded_check_criteria_success(monitor):
    plan = Plan(
        decision_id="dec_123",
        goal="Test goal",
        success_criteria=[SuccessCriterion(type="world_model_query", check="anomaly:consumer_lag_critical resolved")]
    )
    step = PlanStep(step_id="s1", action="scale", description="scale", status=StepStatus.SUCCEEDED)
    
    mock_resp = MagicMock()
    mock_resp.json.return_value = []
    monitor._http.get = AsyncMock(return_value=mock_resp)
    
    await monitor.on_step_completed(plan, step)
    
    assert plan.status == PlanStatus.SUCCEEDED
    assert plan.completed_at is not None
    assert monitor._producer.send_and_wait.call_count == 2


@pytest.mark.asyncio
async def test_on_step_completed_failed_rollback(monitor):
    plan = Plan(
        decision_id="dec_123",
        goal="Test goal",
        rollback_plan=[RollbackStep(action="scale_down", description="Rollback")]
    )
    step = PlanStep(step_id="s1", action="scale", description="scale", status=StepStatus.FAILED, on_failure="rollback")
    
    await monitor.on_step_completed(plan, step)
    
    assert plan.status == PlanStatus.ROLLED_BACK
    assert monitor._producer.send_and_wait.call_count == 3


@pytest.mark.asyncio
async def test_on_step_completed_failed_abort(monitor):
    plan = Plan(
        decision_id="dec_123",
        goal="Test goal"
    )
    step = PlanStep(step_id="s1", action="scale", description="scale", status=StepStatus.FAILED, on_failure="abort")
    
    await monitor.on_step_completed(plan, step)
    
    assert plan.status == PlanStatus.ABORTED
    assert monitor._producer.send_and_wait.call_count == 2


@pytest.mark.asyncio
async def test_on_step_completed_failed_continue(monitor):
    plan = Plan(
        decision_id="dec_123",
        goal="Test goal"
    )
    step = PlanStep(step_id="s1", action="scale", description="scale", status=StepStatus.FAILED, on_failure="continue")
    
    await monitor.on_step_completed(plan, step)
    
    assert plan.status == PlanStatus.CREATED
    assert monitor._producer.send_and_wait.call_count == 1

