import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from agents.base_agent import AgentConfig
from agents.planner_agent import PlannerAgent


@pytest.fixture
def planner_agent():
    cfg = AgentConfig(
        agent_id="agent:planner-db",
        agent_type="planner",
        domain="database",
        action_vocab=["restart_connection_pool"]
    )
    a = PlannerAgent(cfg, executor_agent_id="agent:executor-db")
    a._producer = AsyncMock()
    a._producer.send_and_wait = AsyncMock()
    a._http = AsyncMock()
    return a


@pytest.mark.asyncio
async def test_planner_ignores_unrelated_domain(planner_agent):
    # Situation has queue anomaly - db planner should ignore
    payload = {
        "situation": {
            "ranked_anomalies": [{
                "anomaly_id": "anm_queue",
                "entity_id": "queue:events",
                "event_type": "consumer_lag_critical",
                "severity": "critical",
                "confidence": 1.0
            }]
        }
    }
    planner_agent._http.post = AsyncMock()
    await planner_agent._handle_situation(payload)
    planner_agent._http.post.assert_not_called()


@pytest.mark.asyncio
async def test_planner_triggers_planning_on_matching_domain(planner_agent):
    payload = {
        "situation": {
            "ranked_anomalies": [{
                "anomaly_id": "anm_db",
                "entity_id": "db:postgres",
                "event_type": "database_connection_timeout",
                "severity": "critical",
                "confidence": 1.0
            }]
        }
    }

    # Mock reasoning response
    reason_response = MagicMock()
    reason_response.status_code = 200
    reason_response.json.return_value = {
        "decision_id": "dec_123",
        "recommended_action": "restart_connection_pool",
        "action_parameters": {"service": "db"},
        "confidence": 0.95
    }

    # Mock planning response
    plan_response = MagicMock()
    plan_response.status_code = 201
    plan_response.json.return_value = {"plan_id": "plan_999"}

    # Mock task registration response
    task_response = MagicMock()
    task_response.status_code = 201

    planner_agent._http.post = AsyncMock(side_effect=[reason_response, plan_response, task_response])

    await planner_agent._handle_situation(payload)

    # 3 calls: reasoning trigger, plan generate, create task
    assert planner_agent._http.post.call_count == 3
    assert planner_agent._producer.send_and_wait.called


@pytest.mark.asyncio
async def test_planner_ignores_non_delegated_events(planner_agent):
    # Simulate non-delegated event payload
    event = {
        "event_type": "cpu_spike",
        "payload": {"signal": "situation_detected"}
    }
    # Create a mock consumer that returns this event
    consumer = MagicMock()
    # To mock async iterator:
    class AsyncIter:
        def __init__(self, val):
            self.val = val
            self.done = False
        def __aiter__(self):
            return self
        async def __anext__(self):
            if self.done:
                raise StopAsyncIteration
            self.done = True
            mock_msg = MagicMock()
            mock_msg.value = self.val
            return mock_msg

    planner_agent._handle_situation_safe = AsyncMock()
    task = asyncio.create_task(planner_agent._consume_situations(AsyncIter(event)))
    await asyncio.sleep(0.05)
    task.cancel()
    planner_agent._handle_situation_safe.assert_not_called()


@pytest.mark.asyncio
async def test_planner_no_action_needed_handling(planner_agent):
    payload = {
        "situation": {
            "ranked_anomalies": [{
                "anomaly_id": "anm_db",
                "entity_id": "db:postgres",
                "event_type": "database_connection_timeout",
                "severity": "critical",
                "confidence": 1.0
            }]
        }
    }
    reason_response = MagicMock()
    reason_response.status_code = 200
    reason_response.json.return_value = {"status": "no_action_needed"}
    planner_agent._http.post = AsyncMock(return_value=reason_response)
    await planner_agent._handle_situation(payload)
    # reasoning endpoint is called, but plan generate is skipped (only 1 post call)
    assert planner_agent._http.post.call_count == 1


@pytest.mark.asyncio
async def test_planner_graceful_shutdown(planner_agent):
    planner_agent._running = True
    await planner_agent.stop()
    assert planner_agent._running is False
    assert planner_agent._producer.send_and_wait.called
