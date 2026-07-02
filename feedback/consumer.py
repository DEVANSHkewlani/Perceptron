"""
FeedbackConsumer — the central orchestrator for Phase 10.
Runs as an independent service, consuming cognitive.events
under its own consumer group: 'feedback-loop'.

On each action_completed or action_failed event:
  1. Fetches plan context from Planning API (optional, can fallback if offline)
  2. Runs OutcomeVerifier (waits delay, checks World Model)
  3. Runs OutcomeRecorder, PlaybookUpdater, GraphUpdater in parallel
  4. Emits feedback_signal back to cognitive.events for observability
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx
import yaml
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from .verifier import OutcomeVerifier
from .recorder import OutcomeRecorder
from .playbook_updater import PlaybookUpdater
from .graph_updater import GraphUpdater

log = logging.getLogger("feedback")

ACTION_EVENTS = {
    "action_completed",
    "action_failed",
    "plan_succeeded",
    "plan_rolling_back",
    "task_completed",
    "task_failed",
}


@dataclass
class FeedbackConfig:
    kafka_bootstrap:       str   = "localhost:9092"
    kafka_topic:           str   = "cognitive.events"
    world_model_url:       str   = "http://localhost:8092"
    planning_url:          str   = "http://localhost:8094"
    memory_url:            str   = "http://localhost:8090"
    neo4j_uri:             str   = "bolt://localhost:7687"
    neo4j_user:            str   = "neo4j"
    neo4j_pass:            str   = "password123"
    config_path:           str   = "feedback_config.yaml"
    default_delay_s:       int   = 90


class FeedbackConsumer:
    def __init__(self, cfg: FeedbackConfig | None = None):
        self.cfg      = cfg or FeedbackConfig()
        self._config  = self._load_config()
        self._http    = httpx.AsyncClient(timeout=10.0)
        self._producer: AIOKafkaProducer | None = None

        # Sub-components
        self.verifier       = OutcomeVerifier(self.cfg.world_model_url, self._config)
        self.recorder       = OutcomeRecorder(self.cfg.memory_url)
        self.pb_updater     = PlaybookUpdater(self.cfg.memory_url)
        self.graph_updater  = GraphUpdater(
            self.cfg.neo4j_uri, self.cfg.neo4j_user,
            self.cfg.neo4j_pass, self._config
        )
        self.metrics = {"total_processed": 0, "successes": 0, "failures": 0, "partials": 0}

    def _load_config(self) -> dict:
        try:
            with open(self.cfg.config_path) as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            return {"defaults": {"verification_delay_s": self.cfg.default_delay_s}}

    async def start(self):
        self._producer = AIOKafkaProducer(bootstrap_servers=self.cfg.kafka_bootstrap)
        await self._producer.start()
        try:
            await self.graph_updater.connect()
        except Exception as e:
            log.warning(f"[FeedbackConsumer] Neo4j connection failed at startup: {e}")
        log.info("[FeedbackConsumer] Started. Listening for action events.")

    async def stop(self):
        if self._producer:
            await self._producer.stop()
        try:
            await self.graph_updater.disconnect()
        except Exception:
            pass
        await self._http.aclose()

    async def run(self):
        await self.start()
        consumer = AIOKafkaConsumer(
            self.cfg.kafka_topic,
            bootstrap_servers=self.cfg.kafka_bootstrap,
            group_id="feedback-loop",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda b: json.loads(b.decode()),
        )
        await consumer.start()
        try:
            async for msg in consumer:
                event = msg.value
                if event.get("event_type") in ACTION_EVENTS:
                    # Non-blocking — process each event independently
                    asyncio.create_task(self._process_safe(event))
        finally:
            await consumer.stop()
            await self.stop()

    async def _process_safe(self, event: dict) -> None:
        """Wrap the full feedback cycle with top-level error isolation."""
        try:
            await self._process(event)
        except Exception as e:
            log.error(f"[FeedbackConsumer] Unhandled error in feedback cycle: {e}")

    async def _process(self, event: dict) -> None:
        """Full 7-step feedback cycle for one action event."""
        payload    = event.get("payload", {})
        plan_id    = payload.get("plan_id")
        action     = payload.get("action")
        event_type = event.get("event_type")

        if not plan_id or not action:
            log.debug(f"[FeedbackConsumer] Skipping event without plan_id/action: {event_type}")
            return

        log.info(f"[FeedbackConsumer] Processing feedback for plan={plan_id} action={action}")

        # Step ③ — verify (includes async delay, non-blocking for other events)
        verification = await self.verifier.verify(plan_id, action, event)

        # Update metrics
        self.metrics["total_processed"] += 1
        if verification.outcome == "success":
            self.metrics["successes"] += 1
        elif verification.outcome == "failure":
            self.metrics["failures"] += 1
        else:
            self.metrics["partials"] += 1

        # Steps ④ ⑤ ⑥ — all in parallel, all non-fatal
        results = await asyncio.gather(
            self.recorder.record(event, verification),
            self.pb_updater.update(action, verification.outcome),
            self.graph_updater.update(action, payload, verification.outcome),
            return_exceptions=True,
        )

        for i, r in enumerate(results):
            if isinstance(r, Exception):
                log.warning(f"[FeedbackConsumer] Sub-step {i+4} failed: {r}")

        # Step ⑦ — emit feedback_signal to cognitive.events
        await self._emit_feedback_signal(action, verification.outcome, plan_id)

        log.info(f"[FeedbackConsumer] Cycle complete: plan={plan_id} outcome={verification.outcome}")

    async def _emit_feedback_signal(
        self, action: str, outcome: str, plan_id: str
    ) -> None:
        """Publish feedback_signal for observability and potential fast-path updates."""
        import uuid
        NAMESPACE = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
        timestamp_str = datetime.now(timezone.utc).isoformat()
        natural_key = f"agent:feedback-loop:reasoning_completed:{timestamp_str}"
        det_uuid = uuid.uuid5(NAMESPACE, natural_key).hex[:12]
        event_id = f"evt_{det_uuid}"

        signal = {
            "event_id":    event_id,
            "event_type":  "reasoning_completed",
            "source_type": "agent_event",
            "source_id":   "agent:feedback-loop",
            "severity":    "info",
            "confidence":  1.0,
            "entity_refs": ["agent:feedback-loop"],
            "payload": {
                "action":  action,
                "outcome": outcome,
                "plan_id": plan_id,
                "signal":  "feedback_completed",
            },
            "timestamp":   timestamp_str,
            "ingested_at": timestamp_str,
            "tags":        ["agent", "feedback-loop"],
        }
        try:
            await self._producer.send_and_wait(
                self.cfg.kafka_topic, json.dumps(signal).encode()
            )
        except Exception as e:
            log.warning(f"[FeedbackConsumer] Failed to emit feedback signal: {e}")
