from __future__ import annotations
import asyncio
from .base_agent import BaseAgent, AgentConfig


class PredictionAgent(BaseAgent):
    def __init__(self, cfg: AgentConfig, prediction_interval_s: int = 60):
        super().__init__(cfg)
        self._interval = prediction_interval_s

    async def run(self):
        await self.start()
        try:
            await asyncio.gather(self._prediction_loop(), self.heartbeat_loop())
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def _prediction_loop(self):
        while self._running:
            await asyncio.sleep(self._interval)
            try:
                # Get all degraded entities from World Model
                r = await self._http.get(f"{self.cfg.world_model_url}/world/anomalies")
                if r.status_code != 200: continue
                for anomaly in r.json()[:5]:   # top 5 active anomalies
                    entity_id  = anomaly.get("entity_id")
                    event_type = anomaly.get("event_type")
                    pred_r = await self._http.get(
                        f"{self.cfg.world_model_url}/world/predict/{entity_id}",
                        params={"event_type": event_type},
                    )
                    if pred_r.status_code == 200:
                        predictions = pred_r.json()
                        await self.emit_event(
                            "reasoning_completed", "info",
                            {"signal": "predicted_state",
                             "entity_id": entity_id,
                             "event_type": event_type,
                             "predictions": predictions},
                            entity_refs=[entity_id, self.agent_id],
                        )
            except Exception as e:
                self._log.warning(f"Prediction error: {e}")
