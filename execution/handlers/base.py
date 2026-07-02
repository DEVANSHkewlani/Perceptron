"""
BaseActionHandler — abstract base for every action type.
Subclasses implement execute(). The base handles:
  - action_started event emission (before execute)
  - action_completed / action_failed event emission (after execute)
  - error capture and duration tracking
"""
from __future__ import annotations
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer


@dataclass
class ExecutionResult:
    success:  bool
    output:   dict
    error:    str | None = None
    duration_ms: float = 0.0


class BaseActionHandler(ABC):
    def __init__(self, producer: AIOKafkaProducer | None):
        self._producer = producer

    async def run(
        self, action: str, parameters: dict,
        plan_id: str, step_id: str, entity_refs: list[str]
    ) -> ExecutionResult:
        """
        Full execution lifecycle with feedback events.
        Handlers implement execute() — this method wraps it.
        """
        await self._emit(plan_id, step_id, "action_started", action,
                         entity_refs, {}, "info")
        t0 = time.monotonic()
        try:
            output = await self.execute(action, parameters)
            duration = (time.monotonic() - t0) * 1000
            result = ExecutionResult(success=True, output=output, duration_ms=duration)
            await self._emit(plan_id, step_id, "action_completed", action,
                             entity_refs, output, "info")
        except Exception as e:
            duration = (time.monotonic() - t0) * 1000
            result = ExecutionResult(success=False, output={}, error=str(e), duration_ms=duration)
            await self._emit(plan_id, step_id, "action_failed", action,
                             entity_refs, {"error": str(e)}, "high")
        return result

    @abstractmethod
    async def execute(self, action: str, parameters: dict) -> dict:
        """Perform the action. Raise on failure. Return output dict on success."""
        ...

    async def _emit(
        self, plan_id: str, step_id: str, event_type: str,
        action: str, entity_refs: list[str], payload: dict, severity: str,
    ) -> None:
        event = {
            "event_type":  event_type, "source_type": "agent_event",
            "source_id":   "agent:executor", "severity": severity,
            "confidence":  1.0, "entity_refs": entity_refs or ["agent:executor"],
            "payload": {
                "plan_id": plan_id,
                "step_id": step_id,
                "action": action,
                **payload
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if self._producer:
            await self._producer.send_and_wait(
                "cognitive.events", json.dumps(event).encode()
            )
