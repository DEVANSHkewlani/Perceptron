import pytest
from unittest.mock import AsyncMock, patch
from planning.schema import Plan, PlanStatus
from planning.store import PlanStore


@pytest.mark.asyncio
async def test_save_plan():
    store = PlanStore()
    store._redis = AsyncMock()
    
    plan = Plan(decision_id="dec_123", goal="Test goal")
    await store.save(plan)
    
    store._redis.setex.assert_called_once()
    args, kwargs = store._redis.setex.call_args
    assert args[0] == f"plan:{plan.plan_id}"
    assert args[1] == 86400


@pytest.mark.asyncio
async def test_get_plan_exists():
    store = PlanStore()
    store._redis = AsyncMock()
    
    plan = Plan(decision_id="dec_123", goal="Test goal")
    store._redis.get.return_value = plan.model_dump_json()
    
    retrieved = await store.get(plan.plan_id)
    assert retrieved is not None
    assert retrieved.plan_id == plan.plan_id
    assert retrieved.goal == plan.goal


@pytest.mark.asyncio
async def test_get_plan_not_exists():
    store = PlanStore()
    store._redis = AsyncMock()
    store._redis.get.return_value = None
    
    retrieved = await store.get("nonexistent")
    assert retrieved is None


@pytest.mark.asyncio
async def test_list_active_plans():
    store = PlanStore()
    store._redis = AsyncMock()
    
    p1 = Plan(decision_id="dec_1", goal="Goal 1", status=PlanStatus.CREATED)
    p2 = Plan(decision_id="dec_2", goal="Goal 2", status=PlanStatus.RUNNING)
    p3 = Plan(decision_id="dec_3", goal="Goal 3", status=PlanStatus.SUCCEEDED)
    
    store._redis.keys.return_value = ["plan:1", "plan:2", "plan:3"]
    
    async def mock_get(key):
        if key == "plan:1":
            return p1.model_dump_json()
        elif key == "plan:2":
            return p2.model_dump_json()
        elif key == "plan:3":
            return p3.model_dump_json()
        return None
    
    store._redis.get.side_effect = mock_get
    
    active = await store.list_active()
    assert len(active) == 2
    ids = [p.decision_id for p in active]
    assert "dec_1" in ids
    assert "dec_2" in ids
    assert "dec_3" not in ids


@pytest.mark.asyncio
async def test_store_operations_raise_runtime_error_when_not_connected():
    store = PlanStore()
    with pytest.raises(RuntimeError):
        await store.save(Plan(decision_id="dec", goal="goal"))
    with pytest.raises(RuntimeError):
        await store.get("plan_id")
    with pytest.raises(RuntimeError):
        await store.list_active()

