from __future__ import annotations
import asyncio
from .base_agent import BaseAgent, AgentConfig

SEVERITY_SCORES = {"critical": 5, "high": 4, "medium": 3, "low": 2, "info": 1}


class MonitorAgent(BaseAgent):
    def __init__(
        self, cfg: AgentConfig,
        poll_interval_s: int = 15,
        trigger_severity: str = "medium",
    ):
        super().__init__(cfg)
        self._poll_s   = poll_interval_s
        self._min_sev  = SEVERITY_SCORES.get(trigger_severity, 3)
        self._last_sig: dict[str, str] = {}  # entity_id -> last anomaly_id emitted

    async def run(self):
        await self.start()
        try:
            await asyncio.gather(self._monitor_loop(), self.heartbeat_loop())
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def _monitor_loop(self):
        while self._running:
            try:
                situation = await self.get_situation(top_n=5)
                await self._evaluate_and_signal(situation)
            except Exception as e:
                import traceback
                self._log.warning(f"Monitor poll error: {e}\n{traceback.format_exc()}")
            await asyncio.sleep(self._poll_s)

    async def _evaluate_and_signal(self, situation: dict) -> None:
        for anomaly in situation.get("ranked_anomalies", []):
            sev_score = SEVERITY_SCORES.get(anomaly.get("severity", "info"), 1)
            if sev_score < self._min_sev:
                continue

            anomaly_id = anomaly.get("anomaly_id", "")
            entity_id  = anomaly.get("entity_id", "")

            # Deduplicate: don't re-signal the same anomaly if already emitted
            if self._last_sig.get(entity_id) == anomaly_id:
                continue

            self._last_sig[entity_id] = anomaly_id
            await self.emit_event(
                event_type="task_delegated",
                severity=anomaly.get("severity"),
                payload={
                    "signal":       "situation_detected",
                    "anomaly_id":   anomaly_id,
                    "entity_id":    entity_id,
                    "event_type":   anomaly.get("event_type"),
                    "severity":     anomaly.get("severity"),
                    "confidence":   anomaly.get("confidence"),
                    "situation":    situation,
                    "from_agent":   self.agent_id,
                },
                entity_refs=[entity_id, self.agent_id],
            )
            self._log.info(
                f"Signalled planner: {anomaly.get('event_type')} on {entity_id}"
            )
