import pytest
from temporal.predictor import TemporalPredictor


@pytest.fixture
def predictor():
    return TemporalPredictor()


def test_predict_returns_three_horizons(predictor):
    """Should return predictions for 5, 15, 60 min horizons."""
    values = [50.0 + i * 0.5 for i in range(30)]
    preds = predictor.predict("svc:api", "api_latency_spike", values)
    assert len(preds) == 3
    assert [p.horizon_min for p in preds] == [5, 15, 60]


def test_predict_confidence_decreases_with_horizon(predictor):
    """Longer horizons should have lower confidence."""
    values = [100.0] * 30
    preds = predictor.predict("svc:api", "cpu_spike", values)
    if len(preds) >= 3:
        assert preds[0].confidence >= preds[-1].confidence


def test_predict_returns_empty_on_insufficient_data(predictor):
    """Fewer than 10 values must return empty list (no phantom predictions)."""
    preds = predictor.predict("svc:api", "cpu_spike", [50.0] * 5)
    assert preds == []


def test_predict_bounds_span_point_estimate(predictor):
    """Lower bound must be below and upper bound above the point estimate."""
    values = [float(i % 20) for i in range(40)]
    preds = predictor.predict("svc:api", "api_latency_spike", values)
    for p in preds:
        assert p.lower_bound <= p.predicted <= p.upper_bound
