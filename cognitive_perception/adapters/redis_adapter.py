"""
Redis Adapter (source_type: database)
=====================================
Direct query adapter — connects to Redis, polls INFO on a schedule.
Detects: memory pressure, eviction spikes, hit ratio degradation.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer

from ..schema.event import CognitiveEvent, Severity, SourceType


@dataclass
class RedisSourceConfig:
    """Configuration for a Redis monitoring source."""
    source_id: str
    url: str
    poll_interval_s: int = 15
    memory_pct_critical: float = 0.90
    eviction_warn_delta: int = 10
    hit_ratio_warn: float = 0.70
    tags: list[str] = field(default_factory=list)


class RedisAdapter:
    """
    Polls Redis via INFO commands to monitor cache performance.
    Raises:
    - cache_eviction_spike (when used_memory/maxmemory > memory_pct_critical, or evicted_keys delta > eviction_warn_delta)
    - cache_hit_ratio_dropped (when keyspace hit ratio drops below hit_ratio_warn)
    """

    def __init__(
        self,
        sources: list[RedisSourceConfig],
        kafka_bootstrap: str = "localhost:9092",
        kafka_topic: str = "cognitive.events",
    ):
        self.sources = sources
        self.kafka_url = kafka_bootstrap
        self.topic = kafka_topic
        self._producer: AIOKafkaProducer | None = None
        # Track previous stats for deltas
        self._prev_evicted_keys: dict[str, int] = {}
        self._prev_hits: dict[str, int] = {}
        self._prev_misses: dict[str, int] = {}

    async def start(self):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_url,
            enable_idempotence=True,
            acks="all",
        )
        await self._producer.start()

    async def stop(self):
        if self._producer:
            await self._producer.stop()

    async def run(self):
        await self.start()
        try:
            await asyncio.gather(
                *[self._monitor_source(src) for src in self.sources]
            )
        finally:
            await self.stop()

    async def _monitor_source(self, src: RedisSourceConfig):
        while True:
            client = None
            try:
                client = aioredis.from_url(src.url, decode_responses=True)
                # Test connection
                await client.ping()
                info = await client.info("all")
                await self._process_info(info, src)
            except Exception as e:
                print(f"[RedisAdapter] Error polling {src.source_id}: {e}")
                await self._publish_event(
                    source_id=src.source_id,
                    event_type="service_unreachable",
                    severity=Severity.CRITICAL,
                    payload={"error": str(e)},
                    entity_refs=[src.source_id],
                    confidence=0.95,
                    tags=src.tags + ["connection_failure"],
                )
            finally:
                if client:
                    await client.aclose()
            await asyncio.sleep(src.poll_interval_s)

    async def _process_info(self, info: dict, src: RedisSourceConfig):
        # 1. Memory checks
        used_memory = info.get("used_memory", 0)
        maxmemory = info.get("maxmemory", 0)

        # Memory pressure check
        if maxmemory > 0:
            mem_pct = used_memory / maxmemory
            if mem_pct > src.memory_pct_critical:
                event_type = (
                    "redis_memory_critical"
                    if mem_pct > 0.95
                    else "cache_eviction_spike"
                )
                await self._publish_event(
                    source_id=src.source_id,
                    event_type=event_type,
                    severity=Severity.CRITICAL if mem_pct > 0.95 else Severity.HIGH,
                    payload={
                        "used_memory": used_memory,
                        "maxmemory": maxmemory,
                        "ratio": round(mem_pct, 4),
                        "reason": f"Memory pressure: {mem_pct*100:.1f}% exceeds threshold of {src.memory_pct_critical*100:.0f}%",
                    },
                    entity_refs=[src.source_id],
                    confidence=0.98,
                    tags=src.tags + ["memory_pressure"],
                )

        # 2. Eviction checks
        evicted_keys = info.get("evicted_keys", 0)
        prev_evicted = self._prev_evicted_keys.get(src.source_id)
        if prev_evicted is not None:
            eviction_delta = evicted_keys - prev_evicted
            if eviction_delta > src.eviction_warn_delta:
                await self._publish_event(
                    source_id=src.source_id,
                    event_type="cache_eviction_spike",
                    severity=Severity.HIGH,
                    payload={
                        "evicted_keys_cumulative": evicted_keys,
                        "eviction_delta": eviction_delta,
                        "reason": f"Eviction spike: {eviction_delta} evicted keys in poll window",
                    },
                    entity_refs=[src.source_id],
                    confidence=0.98,
                    tags=src.tags + ["eviction"],
                )
        self._prev_evicted_keys[src.source_id] = evicted_keys

        # 3. Hit ratio checks
        hits = info.get("keyspace_hits", 0)
        misses = info.get("keyspace_misses", 0)
        prev_hits = self._prev_hits.get(src.source_id)
        prev_misses = self._prev_misses.get(src.source_id)

        hit_ratio = None
        if prev_hits is not None and prev_misses is not None:
            hits_delta = hits - prev_hits
            misses_delta = misses - prev_misses
            total_delta = hits_delta + misses_delta
            if total_delta > 0:
                hit_ratio = hits_delta / total_delta

        # Fallback to cumulative if no delta or no requests in this window
        if hit_ratio is None:
            total = hits + misses
            if total > 0:
                hit_ratio = hits / total
            else:
                hit_ratio = 1.0

        self._prev_hits[src.source_id] = hits
        self._prev_misses[src.source_id] = misses

        if hit_ratio < src.hit_ratio_warn:
            await self._publish_event(
                source_id=src.source_id,
                event_type="cache_hit_ratio_dropped",
                severity=Severity.MEDIUM,
                payload={
                    "keyspace_hits": hits,
                    "keyspace_misses": misses,
                    "hit_ratio": round(hit_ratio, 4),
                    "reason": f"Hit ratio of {hit_ratio*100:.1f}% dropped below threshold of {src.hit_ratio_warn*100:.0f}%",
                },
                entity_refs=[src.source_id],
                confidence=0.98,
                tags=src.tags + ["hit_ratio"],
            )

    async def _publish_event(
        self,
        source_id: str,
        event_type: str,
        severity: Severity,
        payload: dict,
        entity_refs: list,
        confidence: float,
        tags: list | None = None,
    ):
        safe_source_id = source_id if any(
            source_id.startswith(p)
            for p in ("svc:", "db:", "metric:", "queue:", "file:", "sensor:", "agent:", "browser:", "security:", "ext:", "usr:")
        ) else f"db:{source_id}"

        safe_entity_refs = []
        for ref in entity_refs:
            safe_ref = ref if any(
                ref.startswith(p)
                for p in ("svc:", "db:", "metric:", "queue:", "file:", "sensor:", "agent:", "browser:", "security:", "ext:", "usr:")
            ) else f"db:{ref}"
            safe_entity_refs.append(safe_ref)

        event = CognitiveEvent(
            timestamp=datetime.now(timezone.utc),
            source_type=SourceType.DATABASE,
            source_id=safe_source_id,
            event_type=event_type,
            severity=severity,
            payload=payload,
            entity_refs=safe_entity_refs,
            confidence=confidence,
            tags=tags or ["database"],
        )
        import time
        t0 = time.monotonic()
        await self._producer.send_and_wait(
            self.topic, event.model_dump_json().encode("utf-8")
        )
        kafka_lag_ms = (time.monotonic() - t0) * 1000
        if kafka_lag_ms > 500:
            print(f"[WARNING] [RedisAdapter] Kafka publish lag high: {kafka_lag_ms:.1f}ms "
                  f"for {event.event_type}")
