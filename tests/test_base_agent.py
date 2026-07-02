import pytest
from unittest.mock import AsyncMock, MagicMock
from agents.base_agent import AgentConfig
from agents.monitor_agent import MonitorAgent


@pytest.fixture
def agent():
    cfg = AgentConfig(agent_id="agent:test-monitor", agent_type="monitor")
    a   = MonitorAgent.__new__(MonitorAgent)
    a.cfg       = cfg
    a.agent_id  = cfg.agent_id
    a._running  = True
    a._producer = AsyncMock(); a._producer.send_and_wait = AsyncMock()
    a._http     = AsyncMock()
    a._log      = MagicMock()
    a._poll_s   = 999   # prevent actual polling in tests
    a._min_sev  = 3
    a._last_sig = {}
    return a


@pytest.mark.asyncio
async def test_emit_event_publishes_to_kafka(agent):
    await agent.emit_event("action_completed", "info", {"test": True})
    agent._producer.send_and_wait.assert_called_once()
    call_args = agent._producer.send_and_wait.call_args[0]
    assert call_args[0] == "cognitive.events"
    import json
    payload = json.loads(call_args[1])
    assert payload["agent_id"] == "agent:test-monitor"
    assert payload["source_type"] == "agent_event"


@pytest.mark.asyncio
async def test_domain_filter_applied_when_specialised(agent):
    # Set domain to database — should filter out queue events
    agent.cfg.domain       = "database"
    agent.cfg.action_vocab = ["restart_connection_pool"]

    anomalies = [
        {"event_type": "consumer_lag_critical",    "severity": "critical"},
        {"event_type": "database_connection_timeout", "severity": "critical"},
    ]
    relevant = [a for a in anomalies if agent._is_domain_relevant(a)]
    assert len(relevant) == 1
    assert relevant[0]["event_type"] == "database_connection_timeout"


@pytest.mark.asyncio
async def test_monitor_deduplicates_signals(agent):
    # Same anomaly emitted twice should only signal once
    situation = {"ranked_anomalies": [{
        "anomaly_id": "anm_123", "entity_id": "svc:auth",
        "event_type": "cpu_spike", "severity": "high", "confidence": 0.9,
    }]}
    await agent._evaluate_and_signal(situation)
    await agent._evaluate_and_signal(situation)   # second call = same anomaly_id
    assert agent._producer.send_and_wait.call_count == 1   # deduplicated


@pytest.mark.asyncio
async def test_monitor_below_min_severity_not_signalled(agent):
    situation = {"ranked_anomalies": [{
        "anomaly_id": "anm_456", "entity_id": "svc:api",
        "event_type": "cache_miss_spike", "severity": "low", "confidence": 0.6,
    }]}
    await agent._evaluate_and_signal(situation)
    agent._producer.send_and_wait.assert_not_called()
