"""
WorldModel — the single authoritative state of the environment.

Architecture:
  - Subscribes to cognitive.events Kafka topic
  - On each event: updates EntityRegistry + AnomalyRegistry
  - Runs background temporal sync loop (every 30s)
  - Runs background anomaly expiry loop (every 60s)
  - Exposes query methods: get_current_situation, get_entity_state,
    get_blast_radius, get_causal_chain, get_prediction
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx
import asyncpg
from aiokafka import AIOKafkaConsumer

from .entity_registry import EntityRegistry
from .anomaly_registry import AnomalyRegistry
from .causal_engine import CausalEngine


@dataclass
class WorldModelConfig:
    kafka_bootstrap:  str = "localhost:9092"
    kafka_topic:      str = "cognitive.events"
    kafka_group:      str = "world-model"
    redis_url:        str = "redis://localhost:6379"
    neo4j_uri:        str = "bolt://localhost:7687"
    neo4j_user:       str = "neo4j"
    neo4j_pass:       str = "password123"
    postgres_dsn:     str = "postgresql://postgres:postgres@localhost:5432/cognitive"
    temporal_api_url: str = "http://localhost:8091"
    memory_api_url:   str = "http://localhost:8090"
    temporal_sync_interval_s:  int = 30
    anomaly_expiry_interval_s: int = 60


class WorldModel:
    def __init__(self, cfg: WorldModelConfig):
        self.cfg            = cfg
        self.entities       = EntityRegistry(cfg.redis_url)
        self.anomalies      = AnomalyRegistry(cfg.redis_url)
        self.causal         = CausalEngine(cfg.neo4j_uri, cfg.neo4j_user, cfg.neo4j_pass)
        self._pg_pool       = None
        self._http: httpx.AsyncClient | None = None
        self._running = False

    # ── LIFECYCLE ──────────────────────────────────────────────

    async def start(self):
        await self.entities.connect()
        await self.anomalies.connect()
        await self.causal.connect()
        self._pg_pool = await asyncpg.create_pool(self.cfg.postgres_dsn, min_size=2, max_size=10)
        self._http = httpx.AsyncClient(timeout=10.0)
        self._running = True

    async def stop(self):
        self._running = False
        await self.entities.disconnect()
        await self.anomalies.disconnect()
        await self.causal.disconnect()
        if self._pg_pool:
            await self._pg_pool.close()
        if self._http: await self._http.aclose()

    async def run(self):
        """Main loop: Kafka subscriber + background tasks."""
        await self.start()
        try:
            await asyncio.gather(
                self._kafka_loop(),
                self._temporal_sync_loop(),
                self._anomaly_expiry_loop(),
            )
        except asyncio.CancelledError:
            pass

    # ── KAFKA EVENT CONSUMER ───────────────────────────────────

    async def _kafka_loop(self):
        consumer = AIOKafkaConsumer(
            self.cfg.kafka_topic,
            bootstrap_servers=self.cfg.kafka_bootstrap,
            group_id=self.cfg.kafka_group,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda b: json.loads(b.decode()),
        )
        await consumer.start()
        try:
            async for msg in consumer:
                await self._process_event(msg.value)
        finally:
            await consumer.stop()

    async def _process_event(self, event: dict) -> None:
        await asyncio.gather(
            self.entities.upsert_from_event(event),
            self.anomalies.process_event(event),
            return_exceptions=True,
        )

    # ── BACKGROUND LOOPS ───────────────────────────────────────

    async def _temporal_sync_loop(self):
        """Poll TemporalAPI for state updates for each tracked entity."""
        while self._running:
            await asyncio.sleep(self.cfg.temporal_sync_interval_s)
            for entity in self.entities.get_all():
                try:
                    # Use the entity's last_event_type as the metric to query
                    r = await self._http.get(
                        f"{self.cfg.temporal_api_url}/temporal/state/{entity.entity_id}",
                        params={"event_type": entity.last_event_type},
                    )
                    if r.status_code == 200:
                        await self.entities.update_temporal_state(
                            entity.entity_id, r.json()
                        )
                except Exception:
                    pass

    async def _anomaly_expiry_loop(self):
        while self._running:
            await asyncio.sleep(self.cfg.anomaly_expiry_interval_s)
            await self.anomalies.expire_stale()

    # ── QUERY INTERFACE ────────────────────────────────────────
    # These five methods are the ONLY interface the Reasoning Engine uses.

    def get_current_situation(self, top_n: int = 5) -> dict:
        """
        Returns a compressed situation summary.
        Primary input to the Reasoning Engine prompt.
        """
        open_anomalies = self.anomalies.get_open()[:top_n]
        degraded_entities = self.entities.get_degraded()[:top_n]

        return {
            "assessed_at":      datetime.now(timezone.utc).isoformat(),
            "anomaly_count":     self.anomalies.count_by_severity(),
            "critical_entities": [
                {"id": e.entity_id, "health": e.health_status,
                 "last_event": e.last_event_type, "trend": e.trend_direction}
                for e in degraded_entities
            ],
            "top_anomalies":     [
                {"id": a.anomaly_id, "entity": a.entity_id,
                 "type": a.event_type, "severity": a.severity,
                 "confidence": a.confidence, "opened_at": a.opened_at}
                for a in open_anomalies
            ],
        }

    def get_entity_state(self, entity_id: str) -> dict | None:
        entity = self.entities.get(entity_id)
        if not entity: return None
        from dataclasses import asdict
        state = asdict(entity)
        # Attach open anomalies for this entity
        state["open_anomalies"] = [
            a.event_type for a in self.anomalies.get_open()
            if a.entity_id == entity_id
        ]
        return state

    async def get_blast_radius(self, entity_id: str) -> list[dict]:
        results = await self.causal.get_blast_radius(entity_id)
        return [
            {
                "entity_id":       r.affected_entity_id,
                "entity_type":     r.entity_type,
                "relationship":    r.relationship_type,
                "hop_distance":    r.hop_distance,
                "current_health":  (
                    self.entities.get(r.affected_entity_id).health_status
                    if self.entities.get(r.affected_entity_id) else "unknown"
                ),
            }
            for r in results
        ]

    async def get_causal_chain(self, entity_id: str) -> dict:
        links = await self.causal.get_causal_chain(entity_id)
        corr  = await self.causal.get_correlation_partners(entity_id)
        return {
            "dependency_chain": [
                {"from": l.from_entity, "to": l.to_entity,
                 "type": l.relationship_type, "confidence": l.confidence}
                for l in links
            ],
            "correlated_entities": corr,
        }

    async def get_prediction(
        self, entity_id: str, event_type: str
    ) -> list[dict]:
        try:
            r = await self._http.get(
                f"{self.cfg.temporal_api_url}/temporal/predict/{entity_id}",
                params={"event_type": event_type},
            )
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        return []

    async def create_task(self, task: dict) -> dict:
        """
        Create a delegation task for an executor agent.
        Stored in both PostgreSQL (fast lookup) and Neo4j (conflict detection).
        """
        import uuid
        task_id = f"task_{uuid.uuid4().hex[:8]}"
        task["id"] = task_id

        # Persist to PostgreSQL
        async with self._pg_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO agent_tasks (id, agent_id, plan_id, action, parameters, status)
                VALUES ($1,$2,$3,$4,$5,'pending')
            """, task_id, task["agent_id"], task.get("plan_id"),
                 task["action"], json.dumps(task.get("parameters", {})))

        # Mirror to Neo4j as Task node for conflict detection
        await self.causal.create_task_node(task)
        return task

    async def get_agent_tasks(self, agent_id: str, status: str = "pending") -> list[dict]:
        """Retrieve tasks assigned to a specific agent. Polled by executor agents."""
        async with self._pg_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM agent_tasks WHERE agent_id=$1 AND status=$2 ORDER BY priority,created_at",
                agent_id, status
            )
        # Convert row records to dict and format jsonb fields
        res = []
        for r in rows:
            d = dict(r)
            if isinstance(d.get("parameters"), str):
                d["parameters"] = json.loads(d["parameters"])
            if isinstance(d.get("result"), str):
                d["result"] = json.loads(d["result"])
            res.append(d)
        return res

    async def complete_task(self, task_id: str, result: dict) -> None:
        """Mark task complete/cancelled in PostgreSQL and Neo4j."""
        from datetime import datetime, timezone
        status = result.get("status", "completed")
        async with self._pg_pool.acquire() as conn:
            await conn.execute("""
                UPDATE agent_tasks SET status=$1, completed_at=$2, result=$3 WHERE id=$4
            """, status, datetime.now(timezone.utc), json.dumps(result), task_id)
        await self.causal.update_task_node(task_id, status)
