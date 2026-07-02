import pytest
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from temporal.state import TemporalStateManager, EntityTemporalState


@pytest.fixture
def state_manager():
    sm = TemporalStateManager.__new__(TemporalStateManager)
    sm._pool = None
    sm._redis = None
    return sm


@pytest.mark.asyncio
async def test_get_state_returns_empty_when_no_data(state_manager):
    """When no historical observations exist, should return empty/default state profile."""
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=[])
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__  = AsyncMock(return_value=False)
    state_manager._pool = mock_pool

    state = await state_manager.get_state("svc:auth", "cpu_spike")
    assert state.current_value is None
    assert state.rate_of_change is None
    assert state.trend_direction == "stable"
    assert state.window_count == 0


@pytest.mark.asyncio
async def test_get_state_calculates_correct_metrics(state_manager):
    """With historical data, state profile should compute stats properly."""
    now_ts = datetime.now(timezone.utc)
    # 10 observations, step of 1 minute, increasing from 10 to 19
    rows = [
        {
            "time": now_ts - timedelta(minutes=10 - i),
            "metric_value": float(10 + i)
        }
        for i in range(10)
    ]
    
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=rows)
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__  = AsyncMock(return_value=False)
    state_manager._pool = mock_pool

    mock_redis = AsyncMock()
    mock_redis.zcard = AsyncMock(return_value=5)
    state_manager._redis = mock_redis

    state = await state_manager.get_state("svc:auth", "cpu_spike")
    assert state.current_value == 19.0
    assert state.rate_of_change == 1.0  # 19 - 18
    assert state.trend_direction == "rising"
    assert state.window_count == 5
