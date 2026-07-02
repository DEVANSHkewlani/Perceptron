from __future__ import annotations
import asyncio
from .base_agent import BaseAgent, AgentConfig


class MemoryAgent(BaseAgent):
    def __init__(self, cfg: AgentConfig, consolidation_interval_s: int = 300):
        super().__init__(cfg)
        self._interval = consolidation_interval_s

    async def run(self):
        await self.start()
        try:
            await asyncio.gather(self._consolidation_loop(), self.heartbeat_loop())
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def _consolidation_loop(self):
        while self._running:
            await asyncio.sleep(self._interval)
            try:
                # Trigger working memory flush (moves expired events to episodic)
                r = await self._http.post(
                    f"{self.cfg.memory_url}/memory/working/flush"
                )
                flushed = r.json().get("flushed", 0) if r.status_code == 200 else 0
                await self.emit_event("action_completed", "info",
                                      {"operation": "memory_consolidation",
                                       "flushed_events": flushed})
                self._log.info(f"Memory consolidation: flushed {flushed} events")
            except Exception as e:
                self._log.warning(f"Consolidation error: {e}")
