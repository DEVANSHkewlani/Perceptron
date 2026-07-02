"""
Tests for TemporalPatternDetector.
Uses an in-memory asyncpg connection (test TimescaleDB) and fakeredis.
"""
import pytest
import asyncio
import numpy as np
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from temporal.detector import TemporalPatternDetector


@pytest.fixture
def detector():
    d = TemporalPatternDetector.__new__(TemporalPatternDetector)
    d._pool  = None
    d._redis = None
    return d


# ── SPIKE ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_spike_detected_above_3sigma(detector):
    """A value 4σ above the mean should produce a spike pattern."""
    baseline = [90.0, 110.0] * 10
    spike_val = 100.0 + 4.0 * 10.0   # mean=100, std=10 → z=4
    values = baseline + [spike_val]
    values_as_rows = [{"metric_value": v} for v in values]

    with patch.object(detector, "_fetch_recent_values",
                       new_callable=AsyncMock, return_value=values_as_rows):
        result = await detector.detect_spike("svc:auth", "api_latency_spike")

    assert result is not None
    assert result["pattern_type"] == "spike"
    assert result["details"]["z_score"] >= 3.0
    assert result["severity"] in ("high", "critical")


@pytest.mark.asyncio
async def test_spike_not_detected_within_baseline(detector):
    """A value within 2σ should NOT trigger a spike."""
    values = [{"metric_value": 100.0 + np.random.uniform(-5, 5)} for _ in range(20)]
    with patch.object(detector, "_fetch_recent_values",
                       new_callable=AsyncMock, return_value=values):
        result = await detector.detect_spike("svc:auth", "api_latency_spike")
    assert result is None


@pytest.mark.asyncio
async def test_spike_returns_none_on_insufficient_data(detector):
    """Fewer than 10 rows should always return None (no false positives)."""
    values = [{"metric_value": 999.0} for _ in range(5)]
    with patch.object(detector, "_fetch_recent_values",
                       new_callable=AsyncMock, return_value=values):
        result = await detector.detect_spike("svc:auth", "cpu_spike")
    assert result is None


# ── DRIFT ──────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_drift_detected_rising(detector):
    """A steadily increasing series should produce a drift pattern."""
    buckets = [
        {"avg_value": 10.0 + i * 1.5}
        for i in range(20)
    ]
    with patch.object(detector, "_fetch_time_buckets",
                       new_callable=AsyncMock, return_value=buckets):
        result = await detector.detect_drift("db:postgres", "slow_query_detected")
    assert result is not None
    assert result["pattern_type"] == "drift"
    assert result["details"]["direction"] == "increasing"


@pytest.mark.asyncio
async def test_drift_not_detected_on_stable_series(detector):
    buckets = [{"avg_value": 50.0} for _ in range(20)]
    with patch.object(detector, "_fetch_time_buckets",
                       new_callable=AsyncMock, return_value=buckets):
        result = await detector.detect_drift("db:postgres", "slow_query_detected")
    assert result is None


# ── ABSENCE ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_absence_detected_when_key_missing(detector):
    """If the absence sentinel key does not exist, pattern should be returned."""
    mock_redis = AsyncMock()
    mock_redis.exists = AsyncMock(return_value=0)
    detector._redis = mock_redis

    results = await detector.detect_absence_for_entity(
        "svc:api-gateway", ["service_health_degraded"]
    )
    assert len(results) == 1
    assert results[0]["pattern_type"] == "absence"
    assert results[0]["severity"] == "high"


@pytest.mark.asyncio
async def test_absence_not_triggered_when_key_present(detector):
    mock_redis = AsyncMock()
    mock_redis.exists = AsyncMock(return_value=1)
    detector._redis = mock_redis

    results = await detector.detect_absence_for_entity(
        "svc:api-gateway", ["service_health_degraded"]
    )
    assert results == []


# ── CORRELATION ────────────────────────────────────────────

@pytest.mark.asyncio
async def test_correlation_detected_high_r(detector):
    """Two perfectly correlated series (r=1.0) should produce a correlation pattern."""
    n = 40
    base = [float(i) for i in range(n)]
    rows_a = [{"metric_value": v} for v in base]
    rows_b = [{"metric_value": v * 2} for v in base]

    with patch.object(detector, "_fetch_recent_values",
                       new_callable=AsyncMock, side_effect=[rows_a, rows_b]):
        result = await detector.detect_correlation(
            "svc:auth", "api_latency_spike",
            "db:postgres", "slow_query_detected",
        )
    assert result is not None
    assert result["pattern_type"] == "correlation"
    assert result["confidence"] >= 0.85


# ── RECURRENCE ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_recurrence_detected_count_ge_3(detector):
    """Appearing 5 times at the same hour-of-week should fire recurrence."""
    mock_pool = MagicMock()
    mock_conn = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=[{"cnt": 5}])
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__  = AsyncMock(return_value=False)
    detector._pool = mock_pool

    ts = datetime(2024, 1, 15, 14, 30, tzinfo=timezone.utc)
    result = await detector.detect_recurrence("svc:api", "deployment_started", ts)
    assert result is not None
    assert result["pattern_type"] == "recurrence"
