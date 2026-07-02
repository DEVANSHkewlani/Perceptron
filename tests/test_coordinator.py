import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from coordinator.coordinator import AgentCoordinator


@pytest.fixture
def coordinator():
    c = AgentCoordinator()
    c._http = AsyncMock()
    # Mock agent start/run so they don't actually boot live clients
    for agent in c.agents:
        agent.run = AsyncMock()
    return c


@pytest.mark.asyncio
async def test_coordinator_starts_all_agents(coordinator):
    task = asyncio.create_task(coordinator.run())
    await asyncio.sleep(0.1)
    task.cancel()

    # Registry populated
    reg = coordinator.get_registry()
    assert len(reg) == len(coordinator.agents)
    assert reg["agent:monitor-global"]["status"] == "running"
    assert reg["agent:planner-01"]["status"] == "running"


@pytest.mark.asyncio
async def test_coordinator_crashed_agent_handling(coordinator):
    # Mock one agent's run to raise an exception
    agent = coordinator.agents[0]
    agent.run = AsyncMock(side_effect=ValueError("Simulated crash"))
    
    # Initialize registry entry for the agent
    coordinator._registry[agent.agent_id] = {
        "type": agent.cfg.agent_type,
        "domain": agent.cfg.domain,
        "status": "running"
    }
    
    # Run _run_agent_safe on it
    await coordinator._run_agent_safe(agent)
    
    # Check that registry reports status as "crashed"
    reg = coordinator.get_registry()
    assert reg[agent.agent_id]["status"] == "crashed"


@pytest.mark.asyncio
async def test_coordinator_conflict_loop_detects_overlaps(coordinator):
    # Mock HTTP response containing conflicts
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {
            "entity_id": "svc:auth",
            "tasks": [
                {"task_id": "t1", "agent_id": "agent:planner-db", "plan_id": "p1", "action": "restart_connection_pool", "risk": "low", "confidence": 0.9},
                {"task_id": "t2", "agent_id": "agent:planner-service", "plan_id": "p2", "action": "restart_service", "risk": "low", "confidence": 0.8}
            ]
        }
    ]
    coordinator._http.get = AsyncMock(return_value=mock_response)
    coordinator.resolver.resolve = AsyncMock()
    
    # Mock asyncio.sleep to raise CancelledError after first invocation
    # This prevents the while True loop from spinning forever
    sleep_calls = 0
    async def mock_sleep_impl(sec):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls > 1:
            raise asyncio.CancelledError()
        # first call: do nothing, let loop proceed
        
    with patch("coordinator.coordinator.asyncio.sleep", mock_sleep_impl):
        try:
            await coordinator._conflict_detection_loop()
        except asyncio.CancelledError:
            pass
            
    # Verify that conflict resolver was called with the conflict structure
    coordinator.resolver.resolve.assert_called_once()
    assert coordinator.resolver.resolve.call_args[0][0]["entity_id"] == "svc:auth"


@pytest.mark.asyncio
async def test_coordinator_health_endpoint():
    from fastapi.testclient import TestClient
    from coordinator.api import app, coordinator as api_coordinator
    
    # Set mock registry data on the global api_coordinator
    api_coordinator._registry = {
        "agent:test-1": {"status": "running", "type": "monitor", "domain": "database"},
        "agent:test-2": {"status": "crashed", "type": "planner", "domain": "database"},
    }
    
    with patch.object(api_coordinator, "run", new_callable=AsyncMock):
        client = TestClient(app)
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["agents_online"] == 1
        assert data["total"] == 2


@pytest.mark.asyncio
async def test_coordinator_heartbeat_watchdog(coordinator):
    from agents.registry import AgentRegistry
    # Verify AgentRegistry status changes
    reg = AgentRegistry()
    reg.register("agent:test-monitor", "monitor", "database")
    assert reg.get("agent:test-monitor")["status"] == "running"
    reg.update_status("agent:test-monitor", "crashed")
    assert reg.get("agent:test-monitor")["status"] == "crashed"

    # Also check base agent heartbeat event emission
    agent = coordinator.agents[0]
    agent._running = True
    agent.cfg.heartbeat_s = 1
    agent.emit_event = AsyncMock()
    
    task = asyncio.create_task(agent.heartbeat_loop())
    await asyncio.sleep(1.1)
    task.cancel()
    
    agent.emit_event.assert_called()
    call_args = agent.emit_event.call_args[0]
    assert call_args[0] == "action_completed"
    assert call_args[1] == "info"
    assert call_args[2]["heartbeat"] is True


@pytest.mark.asyncio
async def test_coordinator_shutdown_cancels_all(coordinator):
    # Run the coordinator in a background task
    task = asyncio.create_task(coordinator.run())
    await asyncio.sleep(0.05)
    
    # Cancel the coordinator run task
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
        
    # Verify that all agent tasks in coordinator._tasks were cancelled or completed
    assert len(coordinator._tasks) > 0
    for t in coordinator._tasks:
        assert t.cancelled() or t.done()
