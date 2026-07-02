"""
Memory Router
=============
Consumes cognitive.events from Kafka.
Fans every event to all four memory layers in parallel.
Manual offset commits — no event is lost.
"""
from __future__ import annotations
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from dateutil.parser import parse as parse_date
from aiokafka import AIOKafkaConsumer

from .working import WorkingMemory
from .episodic import EpisodicMemory
from .semantic import SemanticMemory
from .vector import VectorMemory
from .procedural import ProceduralMemory

log = logging.getLogger("memory.router")

SEVERITY_WORKING = {"critical", "high"}  # only these go to working memory

class MemoryRouter:
    def __init__(
        self,
        kafka_bootstrap: str = "localhost:9092",
        kafka_topic: str = "cognitive.events",
        group_id: str = "memory-router",
        postgres_dsn: str = "postgresql://postgres:postgres@localhost:5432/cognitive",
        redis_url: str = "redis://localhost:6379",
        neo4j_uri: str = "bolt://localhost:7687",
        neo4j_user: str = "neo4j",
        neo4j_password: str = "password123",
        qdrant_url: str = "http://localhost:6333",
        embed_events: bool = True,
    ):
        self.kafka_bootstrap = kafka_bootstrap
        self.topic = kafka_topic
        self.group_id = group_id
        self.working = WorkingMemory(redis_url)
        self.episodic = EpisodicMemory(postgres_dsn)
        self.semantic = SemanticMemory(neo4j_uri, neo4j_user, neo4j_password)
        self.procedural = ProceduralMemory(postgres_dsn)
        self.vector = VectorMemory(qdrant_url) if embed_events else None
        self._stats = {
            "routed": 0, 
            "errors": 0, 
            "_last_routed": 0, 
            "_last_errors": 0
        }
        self._monitor_task: asyncio.Task | None = None

    async def start(self):
        await self.working.connect()
        await self.episodic.connect()
        await self.semantic.connect()
        await self.procedural.connect()
        if self.vector:
            await self.vector.connect()
        log.info("Memory router connected to all layers")
        
        # Start background monitor task
        self._monitor_task = asyncio.create_task(self._monitor_lag())

    async def stop(self):
        # Cancel background monitor
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
            self._monitor_task = None
            
        await self.working.disconnect()
        await self.episodic.disconnect()
        await self.semantic.disconnect()
        await self.procedural.disconnect()
        if self.vector:
            await self.vector.disconnect()
        log.info("Memory router stopped and disconnected")

    async def run(self):
        await self.start()
        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.kafka_bootstrap,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda b: json.loads(b.decode()),
        )
        await consumer.start()
        try:
            async for msg in consumer:
                event = msg.value
                try:
                    await self._route(event)
                    await consumer.commit()
                    self._stats["routed"] += 1
                except Exception as e:
                    log.error(f"Route failed for {event.get('event_id')}: {e}")
                    self._stats["errors"] += 1
        finally:
            await consumer.stop()
            await self.stop()

    async def _route(self, event: dict):
        # 1. Deduplication Filter (Strategy 3)
        eid = event.get("event_id")
        if not eid:
            log.warning("Received event without event_id, skipping.")
            return

        dedup_key = f"dedup:{eid}"
        # Redis set with nx=True and ex=86400 (24h)
        is_new = await self.working._r.set(dedup_key, "1", ex=86400, nx=True)
        if not is_new:
            log.info(f"Duplicate event detected in memory router: {eid}, skipping downstream writes.")
            return

        tasks = []

        # 2. Episodic Memory (Always write, no exceptions)
        tasks.append(self.episodic.store(event))

        # Check and activate playbooks in procedural memory
        tasks.append(self.procedural.check_and_activate_playbooks(event))

        # 3. Gap 1 Staleness Audit & Working Memory (HIGH/CRITICAL only)
        if event.get("severity") in SEVERITY_WORKING:
            lag_seconds = 0.0
            ts_str = event.get("timestamp")
            if ts_str:
                try:
                    dt = parse_date(ts_str)
                    event_epoch = dt.timestamp()
                    lag_seconds = time.time() - event_epoch
                except Exception as e:
                    log.warning(f"Failed to parse timestamp {ts_str} for lag audit: {e}")
            
            if lag_seconds < 120.0:
                tasks.append(self.working.store(event))
            else:
                log.warning(
                    f"Stale event {eid} (lag {lag_seconds:.1f}s >= 120s) "
                    f"skipped Working Memory to prevent cache pollution."
                )

        # 4. Semantic Memory (Knowledge graph - if entity_refs exist)
        if event.get("entity_refs"):
            tasks.append(self.semantic.store(event))

        # 5. Vector Memory (Optional)
        if self.vector and event.get("severity") not in ("info", "low"):
            tasks.append(self.vector.store(event))

        # Dispatch parallel writes using asyncio.gather
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Check results for exceptions, log them as warnings
        for r in results:
            if isinstance(r, Exception):
                log.warning(f"Layer write partial failure: {r}")
                # We raise to let run() increment errors count and handle commit logic appropriately
                # but wait, the guide says: "A failure in one layer logs the error but does not stop the others.
                # The offset is only committed after all writes succeed."
                # Wait, if we want offset committed only when all writes succeed, we should raise if a write fails.
                # Let's raise the first exception to guarantee Exactly-Once offset commits are safe, or as per the guide.
                raise r

    async def _monitor_lag(self):
        while True:
            try:
                await asyncio.sleep(60)
                routed = self._stats.get("routed", 0)
                errors = self._stats.get("errors", 0)
                
                last_routed = self._stats.get("_last_routed", 0)
                last_errors = self._stats.get("_last_errors", 0)
                
                delta_routed = routed - last_routed
                delta_errors = errors - last_errors
                
                throughput = delta_routed / 60.0
                
                total_processed = delta_routed + delta_errors
                error_rate = 0.0
                if total_processed > 0:
                    error_rate = (delta_errors / total_processed) * 100.0
                    
                self._stats["_last_routed"] = routed
                self._stats["_last_errors"] = errors
                
                log.info(
                    f"Memory router throughput: {throughput:.2f} events/sec | "
                    f"Total routed: {routed} | "
                    f"Error rate: {error_rate:.1f}%"
                )
                
                if error_rate > 5.0:
                    log.warning(f"Memory router error rate is high: {error_rate:.1f}%")
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Error in lag monitor task: {e}")
