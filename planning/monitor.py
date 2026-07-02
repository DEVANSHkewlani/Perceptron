"""
PlanMonitor — checks success criteria after each step, triggers rollback on failure.
Publishes plan lifecycle events back to cognitive.events (Kafka) so the
World Model and Reasoning Engine always know plan progress.
"""
from __future__ import annotations
import asyncio
from datetime import datetime, timezone
import httpx
from aiokafka import AIOKafkaProducer
import json
from .schema import Plan, PlanStatus, PlanStep, StepStatus


class PlanMonitor:
    def __init__(
        self,
        kafka_bootstrap: str = "localhost:9092",
        world_model_url: str = "http://localhost:8092",
    ):
        self._kafka_url   = kafka_bootstrap
        self._wm_url      = world_model_url
        self._producer: AIOKafkaProducer | None = None
        self._http        = httpx.AsyncClient(timeout=10.0)

    async def start(self):
        self._producer = AIOKafkaProducer(bootstrap_servers=self._kafka_url)
        await self._producer.start()

    async def stop(self):
        if self._producer:
            await self._producer.stop()
        await self._http.aclose()

    async def on_step_completed(self, plan: Plan, step: PlanStep) -> None:
        """Called by the Execution Layer after each step finishes."""
        await self._emit_step_event(plan, step)

        if step.status == StepStatus.FAILED:
            if step.on_failure == "rollback":
                await self._trigger_rollback(plan, step)
            elif step.on_failure == "abort":
                plan.status = PlanStatus.ABORTED
                await self._emit_plan_event(plan, "plan_aborted")
            return

        # Check success criteria on each successful step
        if await self._check_success_criteria(plan):
            plan.status = PlanStatus.SUCCEEDED
            plan.completed_at = datetime.now(timezone.utc)
            await self._emit_plan_event(plan, "plan_succeeded")

    async def _check_success_criteria(self, plan: Plan) -> bool:
        for criterion in plan.success_criteria:
            if criterion.type == "world_model_query":
                # Parse check: "anomaly:consumer_lag_critical resolved"
                parts = criterion.check.split()
                if "resolved" in parts:
                    try:
                        r = await self._http.get(
                            f"{self._wm_url}/world/anomalies"
                        )
                        anomalies = r.json()
                        # If no open anomalies of that type remain, success
                        anomaly_ref = parts[0].replace("anomaly:", "")
                        open_types = [a.get("event_type") for a in anomalies]
                        if anomaly_ref not in open_types:
                            return True
                    except Exception:
                        pass
        return False

    async def _trigger_rollback(self, plan: Plan, failed_step: PlanStep) -> None:
        plan.status = PlanStatus.ROLLED_BACK
        await self._emit_plan_event(plan, "plan_rolling_back")
        # Rollback steps are emitted as agent_events to the execution layer
        for rb in plan.rollback_plan:
            await self._emit_rollback_action(plan, rb)

    async def _emit_step_event(self, plan: Plan, step: PlanStep) -> None:
        status_val = step.status.value if hasattr(step.status, "value") else str(step.status)
        agent_id_val = plan.agent_id or "agent:planner"
        event = {
            "event_type":   "action_completed" if step.status == StepStatus.SUCCEEDED else "action_failed",
            "source_type":  "agent_event",
            "source_id":    agent_id_val,
            "agent_id":     plan.agent_id,
            "severity":     "info" if step.status == StepStatus.SUCCEEDED else "high",
            "confidence":   1.0,
            "entity_refs":  [agent_id_val],
            "payload": {
                "plan_id":   plan.plan_id,
                "step_id":   step.step_id,
                "action":   step.action,
                "status":   status_val,
                "result":   step.result,
                "error":    step.error,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if self._producer:
            await self._producer.send_and_wait(
                "cognitive.events", json.dumps(event).encode()
            )

    async def _emit_plan_event(self, plan: Plan, event_type: str) -> None:
        status_val = plan.status.value if hasattr(plan.status, "value") else str(plan.status)
        agent_id_val = plan.agent_id or "agent:planner"
        event = {
            "event_type": event_type,
            "source_type": "agent_event",
            "source_id": agent_id_val,
            "agent_id": plan.agent_id,
            "severity": "info",
            "confidence": 1.0,
            "entity_refs": [agent_id_val],
            "payload": {
                "plan_id": plan.plan_id,
                "status": status_val,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if self._producer:
            await self._producer.send_and_wait(
                "cognitive.events", json.dumps(event).encode()
            )

    async def _emit_rollback_action(self, plan: Plan, rb) -> None:
        agent_id_val = plan.agent_id or "agent:planner"
        event = {
            "event_type": "rollback_executed",
            "source_type": "agent_event",
            "source_id": agent_id_val,
            "agent_id": plan.agent_id,
            "severity": "high",
            "confidence": 1.0,
            "entity_refs": [agent_id_val],
            "payload": {
                "plan_id": plan.plan_id,
                "rollback_action": rb.action,
                "parameters": rb.parameters,
            },
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if self._producer:
            await self._producer.send_and_wait(
                "cognitive.events", json.dumps(event).encode()
            )
