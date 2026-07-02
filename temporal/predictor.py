"""
TemporalPredictor
Uses statsmodels ExponentialSmoothing to forecast metric values
5, 15, and 60 minutes into the future.
Publishes predicted_state CognitiveEvents to cognitive.events.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple

import numpy as np
from statsmodels.tsa.holtwinters import ExponentialSmoothing


@dataclass
class Prediction:
    entity_id:     str
    event_type:    str
    horizon_min:   int
    predicted:     float
    lower_bound:   float
    upper_bound:   float
    confidence:    float


class TemporalPredictor:
    HORIZONS = [5, 15, 60]   # minutes

    def predict(
        self,
        entity_id: str,
        event_type: str,
        values: list[float],
        freq_minutes: int = 1,
    ) -> list[Prediction]:
        """
        Fit an ExponentialSmoothing model and generate forecasts.
        values: ordered list of metric values (most recent last).
        freq_minutes: observation interval (1 = one obs per minute).
        Requires at least 10 observations.
        """
        if len(values) < 10:
            return []

        arr = np.array(values)
        predictions = []

        try:
            model = ExponentialSmoothing(
                arr,
                trend="add",
                seasonal=None,    # enable 'add' if you have 2+ full cycles
                initialization_method="estimated",
            ).fit(optimized=True, remove_bias=True)

            for h in self.HORIZONS:
                steps = h // freq_minutes
                forecast = model.forecast(steps)
                point = float(forecast[-1])

                # Simple confidence interval: ±1.5σ of residuals
                residuals = model.resid
                sigma = float(np.std(residuals)) * (1 + steps * 0.02)

                predictions.append(Prediction(
                    entity_id=entity_id,
                    event_type=event_type,
                    horizon_min=h,
                    predicted=round(point, 3),
                    lower_bound=round(point - 1.5 * sigma, 3),
                    upper_bound=round(point + 1.5 * sigma, 3),
                    confidence=round(max(0.5, 0.95 - steps * 0.005), 3),
                ))
        except Exception as e:
            pass   # insufficient data or convergence failure — return empty

        return predictions
