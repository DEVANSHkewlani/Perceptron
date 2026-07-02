from __future__ import annotations
import asyncio
from .base_agent import BaseAgent, AgentConfig


class SpecialistAgent(BaseAgent):
    def __init__(self, cfg: AgentConfig, task_payload: dict | None = None):
        super().__init__(cfg)
        self.task_payload = task_payload or {}

    async def run(self):
        """Runs the specialist reasoning task and terminates."""
        await self.start()
        try:
            self._log.info(f"[{self.agent_id}] Running specialized task for domain: {self.cfg.domain}")
            r = await self._http.post(
                f"{self.cfg.reasoning_url}/reasoning/reason",
                json={
                    "agent_id": self.agent_id,
                    "domain": self.cfg.domain,
                    "task_payload": self.task_payload
                }
            )
            if r.status_code == 200:
                result = r.json()
                self._log.info(f"[{self.agent_id}] Reasoning result: {result}")
                await self.emit_event(
                    "action_completed", "info",
                    {"result": result, "status": "completed"}
                )
            else:
                self._log.warning(f"[{self.agent_id}] Failed to call reasoning API: {r.status_code}")
        except Exception as e:
            self._log.error(f"[{self.agent_id}] Specialist reasoning failure: {e}")
        finally:
            await self.stop()
