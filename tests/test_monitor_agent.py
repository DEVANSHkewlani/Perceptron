import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from agents.base_agent import AgentConfig
from agents.monitor_agent import MonitorAgent


@pytest.fixture
def monitor_agent():
    cfg = AgentConfig(agent_id="agent:monitor-global", agent_type="monitor")
    a = MonitorAgent(cfg, poll_interval_s=1, trigger_severity="medium")
    a._producer = AsyncMock()
    a._producer.send_and_wait = AsyncMock()
    a._http = AsyncMock()
    a._running = True
    return a


@pytest.mark.asyncio
async def test_monitor_loop_fetches_and_evaluates(monitor_agent):
    # Mock get_situation to return an anomaly
    monitor_agent.get_situation = AsyncMock(return_value={
        "ranked_anomalies": [{
            "anomaly_id": "anm_test",
            "entity_id": "svc:web",
            "event_type": "latency_spike",
            "severity": "critical",
            "confidence": 1.0
        }]
    })

    # Start loop in background, run briefly, and cancel
    task = asyncio.create_task(monitor_agent._monitor_loop())
    await asyncio.sleep(0.1)
    task.cancel()

    monitor_agent.get_situation.assert_called()
    assert monitor_agent._producer.send_and_wait.called


@pytest.mark.asyncio
async def test_monitor_publishes_event_on_high_severity(monitor_agent):
    situation = {
        "ranked_anomalies": [{
            "anomaly_id": "anm_high",
            "entity_id": "svc:auth",
            "event_type": "cpu_spike",
            "severity": "high",
            "confidence": 0.95
        }]
    }
    await monitor_agent._evaluate_and_signal(situation)
    assert monitor_agent._producer.send_and_wait.called
    call_args = monitor_agent._producer.send_and_wait.call_args[0]
    import json
    payload = json.loads(call_args[1])
    assert payload["payload"]["anomaly_id"] == "anm_high"


@pytest.mark.asyncio
async def test_monitor_heartbeat(monitor_agent):
    monitor_agent.cfg.heartbeat_s = 1
    task = asyncio.create_task(monitor_agent.heartbeat_loop())
    await asyncio.sleep(1.1)
    task.cancel()
    assert monitor_agent._producer.send_and_wait.called


@pytest.mark.asyncio
async def test_monitor_graceful_shutdown(monitor_agent):
    await monitor_agent.stop()
    assert monitor_agent._running is False
    assert monitor_agent._producer.send_and_wait.called
