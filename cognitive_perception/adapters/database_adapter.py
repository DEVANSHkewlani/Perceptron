"""
Database Adapter  (source_type: database)
==========================================
Direct query adapter — connects to PostgreSQL using asyncpg.
Polls pg_stat_activity, pg_stat_replication, pg_locks.
Detects: slow queries, lock waits, idle-in-transaction, replication lag.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer

from ..schema.event import CognitiveEvent, Severity, SourceType


@dataclass
class DatabaseSourceConfig:
    """Configuration for a direct database query adapter instance."""
    source_id: str
    dsn: str
    db_type: str = "postgres"
    poll_interval_s: int = 30
    slow_query_ms: float = 100.0
    pool_warn_pct: float = 0.70
    pool_critical_pct: float = 0.90
    replication_lag_warn_s: float = 30.0
    lock_wait_warn_s: float = 5.0
    connection_leak_warn_s: float = 60.0


class DatabaseAdapter:
    """
    Polls PostgreSQL system tables directly to detect issues
    Prometheus exporters cannot expose (specific queries, lock holders).
    """

    ACTIVITY_QUERY = """
        SELECT pid, usename, datname, state, wait_event_type, wait_event,
               CASE 
                   WHEN state = 'idle in transaction' THEN EXTRACT(EPOCH FROM (now() - state_change))
                   ELSE EXTRACT(EPOCH FROM (now() - query_start))
               END AS duration_s,
               left(query, 200) AS query_snippet, client_addr::text
        FROM pg_stat_activity
        WHERE state != 'idle' AND pid != pg_backend_pid()
        ORDER BY duration_s DESC NULLS LAST LIMIT 20
    """

    REPLICATION_QUERY = """
        SELECT client_addr::text, state,
               EXTRACT(EPOCH FROM (now() - reply_time)) AS lag_s,
               sent_lsn - replay_lsn AS lag_bytes
        FROM pg_stat_replication
    """

    LOCK_QUERY = """
        SELECT blocked.pid, blocked.usename,
               left(ba.query, 200) AS blocked_query,
               blocking.pid AS blocking_pid,
               left(bla.query, 200) AS blocking_query,
               EXTRACT(EPOCH FROM now() - ba.query_start) AS wait_duration_s
        FROM pg_catalog.pg_locks AS blocked
        JOIN pg_catalog.pg_stat_activity AS ba ON ba.pid = blocked.pid
        JOIN pg_catalog.pg_locks AS blocking
            ON blocking.locktype = blocked.locktype
           AND blocking.relation IS NOT DISTINCT FROM blocked.relation
           AND blocking.page IS NOT DISTINCT FROM blocked.page
           AND blocking.tuple IS NOT DISTINCT FROM blocked.tuple
           AND blocking.transactionid IS NOT DISTINCT FROM blocked.transactionid
           AND blocking.classid IS NOT DISTINCT FROM blocked.classid
           AND blocking.objid IS NOT DISTINCT FROM blocked.objid
           AND blocking.objsubid IS NOT DISTINCT FROM blocked.objsubid
           AND blocking.pid != blocked.pid
        JOIN pg_catalog.pg_stat_activity AS bla ON bla.pid = blocking.pid
        WHERE NOT blocked.granted
          AND EXTRACT(EPOCH FROM now() - ba.query_start) > $1
    """

    POOL_QUERY = """
        SELECT
            (SELECT count(*)::float FROM pg_stat_activity WHERE datname = current_database()) AS active,
            (SELECT setting::float FROM pg_settings WHERE name = 'max_connections') AS max_conn
    """

    def __init__(
        self,
        sources: list[DatabaseSourceConfig],
        kafka_bootstrap: str = "localhost:9092",
        kafka_topic: str = "cognitive.events",
    ):
        self.sources = sources
        self.kafka_url = kafka_bootstrap
        self.topic = kafka_topic
        self._producer: AIOKafkaProducer | None = None

    async def run(self):
        """Start all polling loops concurrently."""
        try:
            import asyncpg  # noqa: F401
        except ImportError:
            raise ImportError("Install asyncpg: pip install asyncpg")

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_url,
            enable_idempotence=True,
            acks="all",
        )
        await self._producer.start()
        try:
            await asyncio.gather(
                *[self._monitor_source(src) for src in self.sources]
            )
        finally:
            await self._producer.stop()

    async def _monitor_source(self, src: DatabaseSourceConfig):
        """Monitor a single database source forever."""
        import asyncpg

        while True:
            try:
                conn = await asyncpg.connect(src.dsn, timeout=10)
                try:
                    await self._check_activity(conn, src)
                    await self._check_pool_usage(conn, src)
                    await self._check_replication(conn, src)
                    await self._check_locks(conn, src)
                finally:
                    await conn.close()
            except Exception as e:
                await self._publish_event(
                    source_id=src.source_id,
                    event_type="service_unreachable",
                    severity=Severity.CRITICAL,
                    payload={"error": str(e), "db_type": src.db_type},
                    entity_refs=[src.source_id],
                    confidence=0.95,
                    tags=["database", "connection_failure"],
                )
            await asyncio.sleep(src.poll_interval_s)

    async def _check_activity(self, conn, src: DatabaseSourceConfig):
        """Detect slow queries and idle-in-transaction sessions."""
        rows = await conn.fetch(self.ACTIVITY_QUERY)
        seen_slow = False
        for row in rows:
            duration_s = row["duration_s"] or 0
            state = row["state"] or ""
            if state != "idle in transaction" and duration_s > (src.slow_query_ms / 1000) and not seen_slow:
                seen_slow = True
                await self._publish_event(
                    source_id=src.source_id,
                    event_type="slow_query_detected",
                    severity=(
                        Severity.MEDIUM if duration_s < 10 else Severity.HIGH
                    ),
                    payload={
                        "pid": row["pid"],
                        "duration_s": round(duration_s, 2),
                        "query_snippet": row["query_snippet"],
                        "database": row["datname"],
                        "user": row["usename"],
                    },
                    entity_refs=[src.source_id],
                    confidence=0.97,
                    tags=["database", "slow_query"],
                )
            if state == "idle in transaction" and duration_s > src.connection_leak_warn_s:
                await self._publish_event(
                    source_id=src.source_id,
                    event_type="connection_leak_detected",
                    severity=Severity.HIGH,
                    payload={
                        "pid": row["pid"],
                        "duration_s": round(duration_s, 2),
                        "query_snippet": row["query_snippet"],
                        "client": row["client_addr"],
                    },
                    entity_refs=[src.source_id],
                    confidence=0.93,
                    tags=["database", "connection_leak"],
                )

    async def _check_pool_usage(self, conn, src: DatabaseSourceConfig):
        """Detect connection pool pressure via pg_stat_activity vs max_connections."""
        row = await conn.fetchrow(self.POOL_QUERY)
        if not row or not row["max_conn"]:
            return
        active = row["active"] or 0
        max_conn = row["max_conn"]
        usage = active / max_conn
        if usage >= src.pool_critical_pct:
            await self._publish_event(
                source_id=src.source_id,
                event_type="connection_pool_exhausted",
                severity=Severity.CRITICAL,
                payload={
                    "active_connections": int(active),
                    "max_connections": int(max_conn),
                    "usage_pct": round(usage * 100, 1),
                },
                entity_refs=[src.source_id, "svc:product-service"],
                confidence=0.96,
                tags=["database", "connection_pool"],
            )
        elif usage >= src.pool_warn_pct:
            await self._publish_event(
                source_id=src.source_id,
                event_type="connection_pool_high",
                severity=Severity.MEDIUM,
                payload={
                    "active_connections": int(active),
                    "max_connections": int(max_conn),
                    "usage_pct": round(usage * 100, 1),
                },
                entity_refs=[src.source_id],
                confidence=0.94,
                tags=["database", "connection_pool"],
            )

    async def _check_replication(self, conn, src: DatabaseSourceConfig):
        """Detect replication lag on standby servers."""
        rows = await conn.fetch(self.REPLICATION_QUERY)
        for row in rows:
            lag_s = row["lag_s"] or 0
            if lag_s > src.replication_lag_warn_s:
                await self._publish_event(
                    source_id=src.source_id,
                    event_type="replication_lag_high",
                    severity=(
                        Severity.HIGH
                        if lag_s > src.replication_lag_warn_s * 3
                        else Severity.MEDIUM
                    ),
                    payload={
                        "lag_s": round(lag_s, 1),
                        "lag_bytes": row["lag_bytes"],
                        "replica": row["client_addr"],
                        "state": row["state"],
                    },
                    entity_refs=[src.source_id],
                    confidence=0.98,
                    tags=["database", "replication"],
                )

    async def _check_locks(self, conn, src: DatabaseSourceConfig):
        """Detect lock wait chains."""
        rows = await conn.fetch(self.LOCK_QUERY, src.lock_wait_warn_s)
        if rows:
            await self._publish_event(
                source_id=src.source_id,
                event_type="lock_wait_timeout",
                severity=Severity.HIGH,
                payload={
                    "blocked_pid": rows[0]["pid"],
                    "blocking_pid": rows[0]["blocking_pid"],
                    "blocked_query": rows[0]["blocked_query"],
                    "blocking_query": rows[0]["blocking_query"],
                    "wait_duration_s": round(rows[0]["wait_duration_s"], 1),
                    "total_blocked": len(rows),
                },
                entity_refs=[src.source_id],
                confidence=0.99,
                tags=["database", "locks"],
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
        # Ensure source_id has a valid namespaced prefix
        safe_source_id = source_id if any(
            source_id.startswith(p)
            for p in ("svc:", "db:", "metric:", "queue:", "file:", "sensor:", "agent:", "browser:", "security:", "ext:", "usr:")
        ) else f"db:{source_id}"

        # Ensure all entity_refs are valid namespaced refs
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
            print(f"[WARNING] [DatabaseAdapter] Kafka publish lag high: {kafka_lag_ms:.1f}ms "
                  f"for {event.event_type if hasattr(event, 'event_type') else 'failure'}")
