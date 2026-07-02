from __future__ import annotations
import asyncio
from .base_agent import BaseAgent, AgentConfig


class ExecutorAgent(BaseAgent):
    def __init__(self, cfg: AgentConfig, poll_interval_s: int = 5):
        super().__init__(cfg)
        self._poll_s = poll_interval_s

    async def run(self):
        await self.start()
        try:
            await asyncio.gather(self._task_loop(), self.heartbeat_loop())
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()

    async def _task_loop(self):
        while self._running:
            try:
                tasks = await self._poll_tasks()
                for task in tasks:
                    asyncio.create_task(self._execute_task(task))
            except Exception as e:
                import traceback
                self._log.warning(f"Task poll error: {e}\n{traceback.format_exc()}")
            await asyncio.sleep(self._poll_s)

    async def _poll_tasks(self) -> list[dict]:
        r = await self._http.get(
            f"{self.cfg.world_model_url}/world/tasks/{self.agent_id}"
        )
        return r.json() if r.status_code == 200 else []

    async def _execute_task(self, task: dict) -> None:
        try:
            plan_id = task.get("plan_id")
            # Fetch full plan and execute it
            plan_r = await self._http.get(
                f"{self.cfg.planning_url}/planning/plans/{plan_id}"
            )
            if plan_r.status_code != 200:
                return

            exec_r = await self._http.post(
                f"{self.cfg.execution_url}/execution/execute",
                json=plan_r.json(),
            )
            success = exec_r.status_code in (200, 202)

            # Mark task complete in World Model
            await self._http.patch(
                f"{self.cfg.world_model_url}/world/tasks/{task['id']}",
                json={"status": "completed" if success else "failed", "result": {"success": success}},
            )
            await self.emit_event(
                "task_completed" if success else "task_failed", "info",
                {"task_id": task["id"], "plan_id": plan_id, "agent_id": self.agent_id},
            )
        except Exception as e:
            self._log.error(f"Task execution error: {e}")
