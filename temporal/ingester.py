"""
TemporalIngester — Kafka consumer for cognitive.events
Writes metric_observations to TimescaleDB.
Maintains Redis sliding windows (ZADD score=timestamp).
Triggers pattern detector on each batch.
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer

from .schema import TemporalSchemaManager
from .detector import TemporalPatternDetector


# Which payload field carries the primary numeric value, per event_type
METRIC_VALUE_MAP: dict[str, str] = {
    "api_latency_spike":              "latency_ms",
    "cpu_spike":                       "value",
    "cpu_sustained_high":              "value",
    "memory_pressure_high":            "value",
    "memory_exhaustion":               "value",
    "swap_usage_high":                 "value",
    "disk_usage_high":                 "value",
    "disk_io_saturation":              "value",
    "network_throughput_spike":        "value",
    "packet_loss_detected":            "value",
    "thread_count_spike":              "value",
    "fd_limit_approaching":            "value",
    "gc_pause_excessive":              "value",
    "gc_frequency_high":               "value",
    "container_cpu_throttled":         "value",
    "connection_pool_high":            "value",
    "slow_request":                    "latency_ms",
    "dns_latency_high":                "latency_ms",
    "consumer_lag_high":               "consumer_lag",
    "consumer_lag_critical":           "consumer_lag",
    "slow_query_detected":             "duration_s",
    "query_latency_spike":             "value",
    "replication_lag_high":            "lag_s",
    "temperature_threshold_exceeded":  "value",
    "lcp_poor":                        "value",
    "lcp_needs_improvement":           "value",
}

# Sliding window TTL in seconds (10 minutes)
WINDOW_TTL_S = 600


class TemporalIngester:
    def __init__(
        self,
        kafka_bootstrap: str,
        postgres_dsn: str,
        redis_url: str,
        kafka_topic: str = "cognitive.events",
        group_id: str = "temporal-engine",
    ):
        self.kafka_bootstrap = kafka_bootstrap
        self.kafka_topic = kafka_topic
        self.group_id = group_id
        self.schema = TemporalSchemaManager(postgres_dsn)
        self.detector = TemporalPatternDetector(postgres_dsn, redis_url)
        self._redis: aioredis.Redis | None = None
        self._redis_url = redis_url

    async def start(self):
        await self.schema.connect()
        await self.schema.initialize()
        await self.detector.connect()
        self._redis = await aioredis.from_url(self._redis_url)

    async def stop(self):
        await self.schema.disconnect()
        await self.detector.disconnect()
        if self._redis:
            await self._redis.aclose()

    async def run(self):
        await self.start()
        consumer = AIOKafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.kafka_bootstrap,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda b: json.loads(b.decode()),
        )
        await consumer.start()
        try:
            async for msg in consumer:
                await self._handle(msg.value)
                await consumer.commit()
        finally:
            await consumer.stop()
            await self.stop()

    async def _handle(self, event: dict) -> None:
        # 1. Resolve metric value from payload
        payload = event.get("payload", {})
        event_type = event.get("event_type", "")
        value_key = METRIC_VALUE_MAP.get(event_type)
        metric_value = payload.get(value_key) if value_key else None

        # Smart fallback: if metric_value is None, try standard numeric keys
        if metric_value is None and isinstance(payload, dict):
            for key in ["value", "latency_ms", "duration_s", "consumer_lag", "lag_s", "cpu_percent", "metric_value"]:
                if key in payload:
                    try:
                        metric_value = float(payload[key])
                        break
                    except (ValueError, TypeError):
                        pass
        else:
            if metric_value is not None:
                try:
                    metric_value = float(metric_value)
                except (ValueError, TypeError):
                    metric_value = None

        # 2. Build observation record
        entity_id = (event.get("entity_refs") or [event.get("source_id", "")])[0]
        ts = datetime.fromisoformat(event["timestamp"].replace("Z", "+00:00"))

        obs = {
            "time":         ts,
            "entity_id":    entity_id,
            "event_type":   event_type,
            "source_type":  event.get("source_type", ""),
            "metric_value": metric_value,
            "severity":     event.get("severity"),
            "confidence":   event.get("confidence"),
            "event_id":     event.get("event_id"),
            "tags":         event.get("tags", []),
        }

        # 3. Persist to TimescaleDB
        await self.schema.insert_observation(obs)

        # 4. Update Redis sliding window
        await self._update_window(entity_id, event_type, ts, metric_value)

        # 5. Run pattern detector
        await self.detector.run_all(entity_id, event_type, ts)

    async def _update_window(
        self, entity_id: str, event_type: str,
        ts: datetime, value: float | None,
    ) -> None:
        score = ts.timestamp()
        window_key = f"tw:{entity_id}:{event_type}"
        # ZADD with score=epoch so we can range-query by time
        val_str = f"{score}:{value}" if value is not None else f"{score}:None"
        await self._redis.zadd(window_key, {val_str: score})
        # Expire entries older than WINDOW_TTL_S
        cutoff = score - WINDOW_TTL_S
        await self._redis.zremrangebyscore(window_key, "-inf", cutoff)
        # Set key TTL so orphan keys clean up automatically
        await self._redis.expire(window_key, WINDOW_TTL_S * 2)

        # Absence sentinel: reset TTL every time we see the event
        absence_key = f"absence:{entity_id}:{event_type}"
        await self._redis.setex(absence_key, WINDOW_TTL_S, "seen")


if __name__ == "__main__":
    import os
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    postgres_dsn = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/cognitive")
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")

    ingester = TemporalIngester(
        kafka_bootstrap=kafka_bootstrap,
        postgres_dsn=postgres_dsn,
        redis_url=redis_url
    )
    print("Starting Temporal Ingester... Press Ctrl+C to stop.")
    try:
        asyncio.run(ingester.run())
    except KeyboardInterrupt:
        print("\nTemporal Ingester stopped.")
