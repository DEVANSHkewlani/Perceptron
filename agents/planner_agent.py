from __future__ import annotations
import asyncio, json
from aiokafka import AIOKafkaConsumer
from .base_agent import BaseAgent, AgentConfig


class PlannerAgent(BaseAgent):
    def __init__(
        self, cfg: AgentConfig,
        executor_agent_id: str = "agent:executor-general",
    ):
        super().__init__(cfg)
        self._executor_id = executor_agent_id

    async def run(self):
        await self.start()
        consumer = AIOKafkaConsumer(
            "cognitive.events",
            bootstrap_servers=self.cfg.kafka_bootstrap,
            group_id=f"planner-{self.cfg.domain}",
            auto_offset_reset="latest",
            enable_auto_commit=True,
            value_deserializer=lambda b: json.loads(b.decode()),
        )
        await consumer.start()
        try:
            await asyncio.gather(
                self._consume_situations(consumer),
                self.heartbeat_loop(),
            )
        except asyncio.CancelledError:
            pass
        finally:
            await consumer.stop()
            await self.stop()

    async def _consume_situations(self, consumer) -> None:
        async for msg in consumer:
            event = msg.value
            if (event.get("event_type") == "task_delegated"
                    and event.get("payload", {}).get("signal") == "situation_detected"):
                asyncio.create_task(self._handle_situation_safe(event["payload"]))

    async def _handle_situation_safe(self, payload: dict) -> None:
        try:
            await self._handle_situation(payload)
        except Exception as e:
            import traceback
            self._log.error(f"Error handling situation: {e}\n{traceback.format_exc()}")

    async def _handle_situation(self, payload: dict) -> None:
        situation = payload.get("situation", {})
        if not situation.get("ranked_anomalies"):
            return

        # Filter to our domain
        situation["ranked_anomalies"] = [
            a for a in situation["ranked_anomalies"]
            if self._is_domain_relevant(a)
        ]
        if not situation["ranked_anomalies"]:
            return   # not our domain

        # 1. Trigger reasoning engine
        r = await self._http.post(
            f"{self.cfg.reasoning_url}/reasoning/reason",
            json={"agent_id": self.agent_id, "domain": self.cfg.domain}
        )
        if r.status_code != 200:
            return
        decision = r.json()
        if decision.get("status") == "no_action_needed" or not decision.get("recommended_action"):
            return

        # 2. Generate plan
        decision["agent_id"] = self.agent_id
        plan_r = await self._http.post(
            f"{self.cfg.planning_url}/planning/generate",
            json=decision,
        )
        if plan_r.status_code != 201:
            return
        plan = plan_r.json()

        # 3. Create delegation task for executor agent
        task_r = await self._http.post(
            f"{self.cfg.world_model_url}/world/tasks",
            json={
                "agent_id":   self._executor_id,
                "plan_id":    plan["plan_id"],
                "action":     decision.get("recommended_action"),
                "parameters": decision.get("action_parameters", {}),
                "priority":   5 if decision.get("confidence", 0) > 0.8 else 3,
            }
        )
        self._log.info(
            f"[{self.agent_id}] Created task for {self._executor_id}: "
            f"{decision.get('recommended_action')}"
        )
        await self.emit_event("task_delegated", "info", {
            "plan_id":  plan["plan_id"],
            "to_agent": self._executor_id,
            "action":   decision.get("recommended_action"),
        })
