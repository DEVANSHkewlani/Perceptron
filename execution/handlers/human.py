"""
HumanHandoffHandler — pauses plan execution, presents context to human,
waits for approval/rejection. Polls Planning API for approval status.
Escalates if timeout expires without a decision.
"""
from __future__ import annotations
import asyncio
import httpx
from .base import BaseActionHandler


class HumanHandoffHandler(BaseActionHandler):
    def __init__(self, producer, planning_api_url: str, poll_interval_s: int = 10):
        super().__init__(producer)
        self._planning_url    = planning_api_url
        self._poll_interval_s = poll_interval_s

    async def execute(self, action: str, parameters: dict) -> dict:
        plan_id  = parameters.get("plan_id", "unknown")
        reason   = parameters.get("reason", "Human approval required.")
        urgency  = parameters.get("urgency", "within_hour")
        timeout_s= parameters.get("timeout_s", 1800)

        print(f"[HumanHandoff] Plan {plan_id} paused. Reason: {reason}. Urgency: {urgency}")

        # Poll Planning API for approval — until approved, rejected, or timeout
        elapsed = 0
        async with httpx.AsyncClient(timeout=5.0) as client:
            while elapsed < timeout_s:
                try:
                    r = await client.get(f"{self._planning_url}/planning/plans/{plan_id}")
                    if r.status_code == 200:
                        plan_data = r.json()
                        # Approval gate step succeeded means human approved
                        for step in plan_data.get("steps", []):
                            if step.get("is_approval_gate"):
                                if step.get("status") == "succeeded":
                                    return {"approved": True, "plan_id": plan_id}
                                if step.get("status") == "failed":
                                    raise RuntimeError("Human rejected this action.")
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(self._poll_interval_s)
                elapsed += self._poll_interval_s

        raise TimeoutError(f"Human approval timeout after {timeout_s}s. Escalating.")
