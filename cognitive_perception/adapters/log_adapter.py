"""
Log Adapter  (source_type: log)
================================
Kafka consumer that reads raw logs from Fluent Bit's output topic,
runs the LogNormalizer on each message, and publishes validated
CognitiveEvents to the cognitive.events topic.

FLOW:
  Application → writes log line to stdout/file
  Fluent Bit → tails the file → publishes raw JSON to Kafka "raw.logs"
  THIS ADAPTER → consumes "raw.logs" → LogNormalizer → "cognitive.events"
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from ..normalizers.log_normalizer import LogNormalizer
from ..schema.event import CognitiveEvent, PerceptionFailure


@dataclass
class LogSourceConfig:
    """Maps a Fluent Bit tag to a source_id in our entity namespace."""
    fluent_bit_tag: str
    source_id: str
    log_format: str = "json"
    tags: list[str] = field(default_factory=list)


class LogAdapter:
    """
    Kafka consumer → LogNormalizer → CognitiveEvents on cognitive.events.
    Multiple instances can run in the same consumer group for parallelism.
    """

    def __init__(
        self,
        sources: list[LogSourceConfig],
        kafka_bootstrap: str = "localhost:9092",
        raw_log_topic: str = "raw.logs",
        output_topic: str = "cognitive.events",
        failure_topic: str = "cognitive.perception_failures",
        consumer_group: str = "perception-log-normalizer",
    ):
        self.sources = sources
        self.kafka_url = kafka_bootstrap
        self.raw_topic = raw_log_topic
        self.output_topic = output_topic
        self.failure_topic = failure_topic
        self.group_id = consumer_group
        self.normalizer = LogNormalizer()
        self._tag_map: dict[str, LogSourceConfig] = {
            src.fluent_bit_tag: src for src in sources
        }

    async def run(self):
        """Start the Kafka consumer loop. Runs forever until cancelled."""
        consumer = AIOKafkaConsumer(
            self.raw_topic,
            bootstrap_servers=self.kafka_url,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        )
        producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_url,
            enable_idempotence=True,
            acks="all",
        )
        await consumer.start()
        await producer.start()

        stats = {"processed": 0, "failed": 0, "skipped": 0}
        try:
            async for msg in consumer:
                try:
                    await self._process_message(msg, producer, stats)
                    # NOTE: We intentionally commit the offset even if the log produced a
                    # PerceptionFailure. This prevents a malformed or "poison pill" log message
                    # from stalling the partition consumer indefinitely. Failed messages are
                    # safely routed to the failure topic and accounted for in stats["failed"].
                    await consumer.commit()
                except Exception as e:
                    print(f"[LogAdapter] Fatal error on message: {e}")
                    stats["failed"] += 1
                if stats["processed"] % 1000 == 0 and stats["processed"] > 0:
                    print(f"[LogAdapter] Stats: {stats}")
        finally:
            await consumer.stop()
            await producer.stop()

    async def _process_message(
        self, msg, producer: AIOKafkaProducer, stats: dict
    ):
        """Process a single Kafka message from raw.logs."""
        raw = msg.value

        tag = (
            raw.get("tag")
            or raw.get("source")
            or (msg.key.decode() if msg.key else None)
            or "unknown"
        )

        src_config = self._tag_map.get(tag)
        if not src_config:
            stats["skipped"] += 1
        source_id = src_config.source_id if src_config else f"svc:{tag}"

        log_content = self._extract_log_content(raw, src_config)
        result = self.normalizer.normalize(log_content, source_id)

        # Add extra tags and extract trace/correlation IDs
        if src_config and isinstance(result, CognitiveEvent):
            result.tags.extend(src_config.tags)
            if isinstance(log_content, dict):
                for id_field in (
                    "trace_id", "request_id", "correlation_id", "span_id",
                ):
                    if val := log_content.get(id_field):
                        result.tags.append(f"{id_field}:{val}")

        import time
        if isinstance(result, CognitiveEvent):
            t0 = time.monotonic()
            await producer.send_and_wait(
                self.output_topic,
                result.model_dump_json().encode("utf-8"),
            )
            kafka_lag_ms = (time.monotonic() - t0) * 1000
            if kafka_lag_ms > 500:
                print(f"[WARNING] [LogAdapter] Kafka publish lag high: {kafka_lag_ms:.1f}ms "
                      f"for {result.event_type}")
            stats["processed"] += 1
        else:
            t0 = time.monotonic()
            await producer.send_and_wait(
                self.failure_topic,
                result.model_dump_json().encode("utf-8"),
            )
            kafka_lag_ms = (time.monotonic() - t0) * 1000
            if kafka_lag_ms > 500:
                print(f"[WARNING] [LogAdapter] Kafka publish lag high: {kafka_lag_ms:.1f}ms "
                      f"for perception failure")
            stats["failed"] += 1

    def _extract_log_content(
        self, fluent_bit_msg: dict, src: LogSourceConfig | None
    ) -> dict | str:
        """Extract the actual log entry from Fluent Bit's envelope."""
        for f in ("message", "msg", "log", "MESSAGE", "body"):
            if content := fluent_bit_msg.get(f):
                if isinstance(content, str):
                    try:
                        inner = json.loads(content)
                        if isinstance(inner, dict):
                            inner.setdefault(
                                "service", fluent_bit_msg.get("tag", "")
                            )
                            return inner
                    except (json.JSONDecodeError, ValueError):
                        pass
                    return fluent_bit_msg
                elif isinstance(content, dict):
                    return content
        return fluent_bit_msg
