"""
Queue Adapter  (source_type: queue)
====================================
Monitors message broker health by polling admin APIs.

SUPPORTED BROKERS:
  - Kafka / Redpanda (via confluent_kafka AdminClient)
  - RabbitMQ (via management REST API)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer

from ..normalizers.queue_normalizer import QueueNormalizer
from ..schema.event import CognitiveEvent


@dataclass
class QueueSourceConfig:
    source_id: str
    broker_type: str
    queue_name: str
    consumer_group: str | None = None
    poll_interval_s: int = 15
    tags: list[str] = field(default_factory=list)
    lag_warn: int = 1000
    lag_critical: int = 10000
    depth_warn: int = 5000
    dlq_warn: int = 10
    msg_age_warn_s: float = 300.0
    kafka_bootstrap: str = "localhost:9092"
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 15672
    rabbitmq_user: str = "guest"
    rabbitmq_pass: str = "guest"


async def collect_kafka_metrics(src: QueueSourceConfig) -> dict | None:
    """Collect consumer lag and topic depth from Kafka/Redpanda asynchronously via thread executor."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, _collect_kafka_metrics_sync, src)


def _collect_kafka_metrics_sync(src: QueueSourceConfig) -> dict | None:
    """Synchronous version — runs in thread pool executor to prevent blocking the async loop."""
    try:
        from confluent_kafka.admin import AdminClient
        from confluent_kafka import TopicPartition, Consumer

        admin = AdminClient({"bootstrap.servers": src.kafka_bootstrap})
        topic_meta = admin.list_topics(src.queue_name, timeout=5)
        partitions = list(
            topic_meta.topics[src.queue_name].partitions.keys()
        )

        total_lag = 0
        if src.consumer_group:
            consumer = Consumer({
                "bootstrap.servers": src.kafka_bootstrap,
                "group.id": src.consumer_group,
                "enable.auto.commit": False,
            })
            topic_parts = [
                TopicPartition(src.queue_name, p) for p in partitions
            ]
            committed = consumer.committed(topic_parts, timeout=5)
            for tp, committed_offset in zip(topic_parts, committed):
                lo, hi = consumer.get_watermark_offsets(tp, timeout=5)
                if (
                    committed_offset is not None
                    and committed_offset.offset >= 0
                ):
                    total_lag += max(0, hi - committed_offset.offset)
                else:
                    total_lag += hi
            consumer.close()

        return {
            "source_id": src.source_id,
            "broker_type": "kafka",
            "queue_name": src.queue_name,
            "consumer_group": src.consumer_group,
            "consumer_lag": total_lag,
            "queue_depth": total_lag,
            "oldest_message_age_s": 0,
            "dead_letter_count": 0,
            "consumer_count": 1,
            "throughput_rate": 0.0,
        }
    except Exception as e:
        print(f"[KafkaCollector] {src.source_id}: {e}")
        return None


async def collect_rabbitmq_metrics(src: QueueSourceConfig) -> dict | None:
    """Collect queue state from RabbitMQ Management API."""
    try:
        import httpx

        url = (
            f"http://{src.rabbitmq_host}:{src.rabbitmq_port}"
            f"/api/queues/%2F/{src.queue_name}"
        )
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(
                url, auth=(src.rabbitmq_user, src.rabbitmq_pass)
            )
            if resp.status_code != 200:
                return None
            data = resp.json()

        return {
            "source_id": src.source_id,
            "broker_type": "rabbitmq",
            "queue_name": src.queue_name,
            "consumer_group": None,
            "consumer_lag": data.get("messages_unacknowledged", 0),
            "queue_depth": data.get("messages", 0),
            "oldest_message_age_s": 0,
            "dead_letter_count": 0,
            "consumer_count": data.get("consumers", 0),
            "throughput_rate": (
                data.get("message_stats", {})
                .get("ack_details", {})
                .get("rate", 0)
            ),
        }
    except Exception as e:
        print(f"[RabbitMQCollector] {src.source_id}: {e}")
        return None


COLLECTOR_MAP = {
    "kafka": collect_kafka_metrics,
    "rabbitmq": collect_rabbitmq_metrics,
}


class QueueAdapter:
    """
    Polls queue broker admin APIs continuously.
    Emits CognitiveEvents when state changes or thresholds are crossed.
    """

    def __init__(
        self,
        sources: list[QueueSourceConfig],
        kafka_bootstrap: str = "localhost:9092",
        kafka_topic: str = "cognitive.events",
        redis_url: str = "redis://localhost:6379",
    ):
        self.sources = sources
        self.kafka_url = kafka_bootstrap
        self.topic = kafka_topic
        self.redis_url = redis_url
        self.normalizer = QueueNormalizer()
        self._producer: AIOKafkaProducer | None = None
        self._redis: aioredis.Redis | None = None

    async def run(self):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_url,
            enable_idempotence=True,
            acks="all",
        )
        self._redis = await aioredis.from_url(self.redis_url)
        await self._producer.start()
        try:
            await asyncio.gather(
                *[self._poll(src) for src in self.sources]
            )
        finally:
            await self._producer.stop()
            await self._redis.aclose()

    async def _poll(self, src: QueueSourceConfig):
        """Poll a single queue source forever."""
        collector = COLLECTOR_MAP.get(src.broker_type)
        if not collector:
            print(f"[QueueAdapter] No collector for: {src.broker_type}")
            return

        while True:
            try:
                metrics = await collector(src)
                if metrics:
                    metrics.update({
                        "lag_warn": src.lag_warn,
                        "lag_critical": src.lag_critical,
                        "dlq_warn": src.dlq_warn,
                        "msg_age_warn_s": src.msg_age_warn_s,
                        "depth_warn": src.depth_warn,
                    })
                    event = self.normalizer.normalize(metrics, src.source_id)
                    # Only publish non-info or state-changed events
                    state_key = f"queue_state:{src.source_id}"
                    state_val = (
                        f"{metrics['consumer_lag']}:"
                        f"{metrics['consumer_count']}"
                    )
                    prev = await self._redis.get(state_key)
                    if (
                        not hasattr(event, "severity")
                        or event.severity != "info"
                        or prev is None
                        or prev.decode() != state_val
                    ):
                        await self._redis.set(state_key, state_val, ex=3600)
                        import time
                        payload = event.model_dump_json().encode("utf-8")
                        t0 = time.monotonic()
                        await self._producer.send_and_wait(self.topic, payload)
                        kafka_lag_ms = (time.monotonic() - t0) * 1000
                        if kafka_lag_ms > 500:
                            print(f"[WARNING] [QueueAdapter] Kafka publish lag high: {kafka_lag_ms:.1f}ms "
                                  f"for {event.event_type if hasattr(event, 'event_type') else 'failure'}")
            except Exception as e:
                print(f"[QueueAdapter] Error polling {src.source_id}: {e}")
            await asyncio.sleep(src.poll_interval_s)
