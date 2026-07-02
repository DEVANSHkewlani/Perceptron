import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from coordinator.conflict_resolver import ConflictResolver


@pytest.fixture
def resolver():
    return ConflictResolver("http://localhost:8092", "http://localhost:8094")


def make_conflict(a_risk="low", a_conf=0.90, b_risk="high", b_conf=0.75) -> dict:
    return {
        "entity_id": "svc:auth-service",
        "tasks": [
            {"task_id":"t1","agent_id":"agent:planner-db",
             "plan_id":"plan_a","action":"restart_connection_pool",
             "risk":a_risk,"confidence":a_conf},
            {"task_id":"t2","agent_id":"agent:planner-service",
             "plan_id":"plan_b","action":"restart_service",
             "risk":b_risk,"confidence":b_conf},
        ]
    }


@pytest.mark.asyncio
async def test_high_confidence_low_risk_wins(resolver):
    """Higher confidence/lower risk action should win priority resolution."""
    cancelled = []
    with patch("httpx.AsyncClient") as MockClient:
        mc = AsyncMock()
        mc.__aenter__ = AsyncMock(return_value=mc)
        mc.__aexit__  = AsyncMock(return_value=False)
        mc.delete = AsyncMock(side_effect=lambda url: cancelled.append(url) or AsyncMock(status_code=200)())
        mc.patch  = AsyncMock()
        MockClient.return_value = mc
        await resolver.resolve(make_conflict(a_risk="low", a_conf=0.90,
                                             b_risk="high", b_conf=0.75))
    assert any("plan_b" in c for c in cancelled)


@pytest.mark.asyncio
async def test_both_low_risk_triggers_merge(resolver):
    """Two low-risk actions should be merged, not one cancelled."""
    appended = []
    with patch("httpx.AsyncClient") as MockClient:
        mc = AsyncMock()
        mc.__aenter__ = AsyncMock(return_value=mc)
        mc.__aexit__  = AsyncMock(return_value=False)
        mc.post   = AsyncMock(side_effect=lambda url, **kw: appended.append(url) or AsyncMock()())
        mc.delete = AsyncMock()
        MockClient.return_value = mc
        await resolver.resolve(make_conflict(a_risk="low", a_conf=0.88,
                                             b_risk="low", b_conf=0.80))
    assert any("append" in url for url in appended)


@pytest.mark.asyncio
async def test_single_task_no_resolution_needed(resolver):
    """No conflict if only one task targeting entity."""
    conflict = {"entity_id": "svc:api", "tasks": [
        {"task_id":"t1","agent_id":"agent:planner-db",
         "plan_id":"plan_a","action":"scale_consumer_group",
         "risk":"low","confidence":0.95}
    ]}
    await resolver.resolve(conflict)   # should return without doing anything
