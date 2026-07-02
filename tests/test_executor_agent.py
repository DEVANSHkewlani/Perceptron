import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from agents.base_agent import AgentConfig
from agents.executor_agent import ExecutorAgent


@pytest.fixture
def executor_agent():
    cfg = AgentConfig(agent_id="agent:executor-db", agent_type="executor", domain="database")
    a = ExecutorAgent(cfg, poll_interval_s=1)
    a._producer = AsyncMock()
    a._producer.send_and_wait = AsyncMock()
    a._http = AsyncMock()
    a._running = True
    return a


@pytest.mark.asyncio
async def test_executor_polls_and_runs_tasks(executor_agent):
    # Mock task polling
    tasks_response = MagicMock()
    tasks_response.status_code = 200
    tasks_response.json.return_value = [{
        "id": "task_123",
        "agent_id": "agent:executor-db",
        "plan_id": "plan_abc",
        "action": "restart_connection_pool"
    }]
    executor_agent._http.get = AsyncMock(return_value=tasks_response)

    # Mock execute_task process
    executor_agent._execute_task = AsyncMock()

    # Run loop briefly and cancel
    task = asyncio.create_task(executor_agent._task_loop())
    await asyncio.sleep(0.1)
    task.cancel()

    executor_agent._execute_task.assert_called_once()


@pytest.mark.asyncio
async def test_executor_ignores_failed_plan_fetches(executor_agent):
    task = {"id": "task_123", "plan_id": "plan_failed"}
    # Plan fetch fails (status 404)
    plan_response = MagicMock()
    plan_response.status_code = 404
    executor_agent._http.get = AsyncMock(return_value=plan_response)
    executor_agent._http.post = AsyncMock()
    
    await executor_agent._execute_task(task)
    # Exec API post should not be called since plan fetch failed
    executor_agent._http.post.assert_not_called()


@pytest.mark.asyncio
async def test_executor_marks_tasks_failed_on_runner_exception(executor_agent):
    task = {"id": "task_123", "plan_id": "plan_abc"}
    plan_response = MagicMock(); plan_response.status_code = 200; plan_response.json.return_value = {}
    # Exec runner post returns 500 error
    exec_response = MagicMock(); exec_response.status_code = 500
    
    executor_agent._http.get = AsyncMock(return_value=plan_response)
    executor_agent._http.post = AsyncMock(return_value=exec_response)
    executor_agent._http.patch = AsyncMock()
    executor_agent.emit_event = AsyncMock()
    
    await executor_agent._execute_task(task)
    
    # Task marked as failed
    patch_args = executor_agent._http.patch.call_args[1]["json"]
    assert patch_args["status"] == "failed"
    assert "task_failed" in [args[0][0] for args in executor_agent.emit_event.call_args_list]


@pytest.mark.asyncio
async def test_executor_emits_task_completed_event(executor_agent):
    task = {"id": "task_123", "plan_id": "plan_abc"}
    plan_response = MagicMock(); plan_response.status_code = 200; plan_response.json.return_value = {}
    exec_response = MagicMock(); exec_response.status_code = 200
    
    executor_agent._http.get = AsyncMock(return_value=plan_response)
    planner_mock_post = AsyncMock(side_effect=[exec_response, MagicMock()])
    executor_agent._http.post = planner_mock_post
    executor_agent._http.patch = AsyncMock()
    
    # Spy emit_event
    executor_agent.emit_event = AsyncMock()
    
    await executor_agent._execute_task(task)
    
    # Verifies task_completed event was emitted
    executor_agent.emit_event.assert_called_once()
    assert executor_agent.emit_event.call_args[0][0] == "task_completed"
