"""
Perception Orchestrator / Main Entrypoint
=========================================
Saves the world by loading `sources.yaml`, instantiating the correct
adapter for each signal source, and running them all concurrently
in an async event loop.
"""

from __future__ import annotations

import asyncio
import os
import sys
import traceback

from cognitive_perception.adapters.sources import load_sources, write_example_config
from cognitive_perception.adapters.api_adapter import APIAdapter, APISourceConfig
from cognitive_perception.adapters.log_adapter import LogAdapter, LogSourceConfig
from cognitive_perception.adapters.database_adapter import DatabaseAdapter, DatabaseSourceConfig
from cognitive_perception.adapters.queue_adapter import QueueAdapter, QueueSourceConfig
from cognitive_perception.adapters.file_adapter import FileAdapter, FileSourceConfig
from cognitive_perception.adapters.redis_adapter import RedisAdapter, RedisSourceConfig
# SensorAdapter disabled per request
# from cognitive_perception.adapters.sensor_adapter import SensorAdapter, SensorSourceConfig


# Connection settings with environment variable overrides
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_URL       = os.getenv("REDIS_URL", "redis://localhost:6379")


async def run_orchestrator(config_path: str = "sources.yaml"):
    """
    Read sources.yaml, build the active adapters, and run them concurrently.
    """
    print("=" * 60)
    print("Starting Digital Cognitive Architecture — Perception Layer (Phase 1)")
    print(f"Config: {config_path}")
    print(f"Kafka:  {KAFKA_BOOTSTRAP}")
    print(f"Redis:  {REDIS_URL}")
    print("=" * 60)

    # Ensure config file exists
    if not os.path.exists(config_path):
        print(f"Config file {config_path} not found. Generating default...")
        write_example_config(config_path)

    try:
        sources = load_sources(config_path)
    except Exception as e:
        print(f"[Orchestrator] Failed to load configuration: {e}")
        sys.exit(1)

    print(f"[Orchestrator] Loaded {len(sources)} enabled sources.")

    # Group sources by type
    by_type: dict[str, list] = {}
    for src in sources:
        by_type.setdefault(src.type, []).append(src)

    tasks = []

    # 1. API ADAPTERS
    api_sources = by_type.get("api", [])
    if api_sources:
        configs = []
        for s in api_sources:
            # Safely get properties
            configs.append(APISourceConfig(
                source_id=s.id,
                url=s.params.get("url", ""),
                poll_interval_s=s.params.get("poll_interval_s", 30),
                timeout_s=s.params.get("timeout_s", 5.0),
                expected_status=s.params.get("expected_status", 200),
                latency_warn_ms=s.params.get("latency_warn_ms", 500.0),
                latency_high_ms=s.params.get("latency_high_ms", 2000.0),
                tags=s.tags,
                headers=s.params.get("headers", {}),
            ))
        print(f"[Orchestrator] APIAdapter: Monitoring {len(configs)} endpoints.")
        adapter = APIAdapter(
            sources=configs,
            kafka_bootstrap=KAFKA_BOOTSTRAP,
            redis_url=REDIS_URL,
        )
        tasks.append(run_adapter_safe("APIAdapter", adapter.run))

    # 2. LOG ADAPTERS
    log_sources = by_type.get("log", [])
    if log_sources:
        configs = []
        for s in log_sources:
            configs.append(LogSourceConfig(
                fluent_bit_tag=s.params.get("fluent_bit_tag", s.id.replace("svc:", "")),
                source_id=s.id,
                log_format=s.params.get("log_format", "json"),
                tags=s.tags,
            ))
        print(f"[Orchestrator] LogAdapter: Monitoring logs for {len(configs)} services.")
        adapter = LogAdapter(
            sources=configs,
            kafka_bootstrap=KAFKA_BOOTSTRAP,
        )
        tasks.append(run_adapter_safe("LogAdapter", adapter.run))

    # 3. DATABASE ADAPTERS (direct asyncpg polling)
    db_sources = by_type.get("database", [])
    if db_sources:
        configs = []
        for s in db_sources:
            configs.append(DatabaseSourceConfig(
                source_id=s.id,
                dsn=s.params.get("dsn", ""),
                db_type=s.params.get("db_type", "postgres"),
                poll_interval_s=s.params.get("poll_interval_s", 30),
                slow_query_ms=s.params.get("slow_query_ms", 100.0),
                pool_warn_pct=s.params.get("pool_warn_pct", 0.70),
                pool_critical_pct=s.params.get("pool_critical_pct", 0.90),
                replication_lag_warn_s=s.params.get("replication_lag_warn_s", 30.0),
                lock_wait_warn_s=s.params.get("lock_wait_warn_s", 5.0),
                connection_leak_warn_s=s.params.get("connection_leak_warn_s", 60.0),
            ))
        print(f"[Orchestrator] DatabaseAdapter: Polling database metrics for {len(configs)} hosts.")
        adapter = DatabaseAdapter(
            sources=configs,
            kafka_bootstrap=KAFKA_BOOTSTRAP,
        )
        tasks.append(run_adapter_safe("DatabaseAdapter", adapter.run))

    # 4. QUEUE ADAPTERS
    queue_sources = by_type.get("queue", [])
    if queue_sources:
        configs = []
        for s in queue_sources:
            configs.append(QueueSourceConfig(
                source_id=s.id,
                broker_type=s.params.get("broker_type", "kafka"),
                queue_name=s.params.get("queue_name", s.params.get("topic", "")),
                consumer_group=s.params.get("consumer_group"),
                poll_interval_s=s.params.get("poll_interval_s", 15),
                tags=s.tags,
                lag_warn=s.params.get("lag_warn", 1000),
                lag_critical=s.params.get("lag_critical", 10000),
                depth_warn=s.params.get("depth_warn", 5000),
                dlq_warn=s.params.get("dlq_warn", 10),
                msg_age_warn_s=s.params.get("msg_age_warn_s", 300.0),
                kafka_bootstrap=s.params.get("kafka_bootstrap", KAFKA_BOOTSTRAP),
                rabbitmq_host=s.params.get("rabbitmq_host", "localhost"),
                rabbitmq_port=s.params.get("rabbitmq_port", 15672),
                rabbitmq_user=s.params.get("rabbitmq_user", "guest"),
                rabbitmq_pass=s.params.get("rabbitmq_pass", "guest"),
            ))
        print(f"[Orchestrator] QueueAdapter: Monitoring {len(configs)} queues.")
        adapter = QueueAdapter(
            sources=configs,
            kafka_bootstrap=KAFKA_BOOTSTRAP,
            redis_url=REDIS_URL,
        )
        tasks.append(run_adapter_safe("QueueAdapter", adapter.run))

    # 5. FILESYSTEM ADAPTERS
    file_sources = by_type.get("file", [])
    if file_sources:
        configs = []
        for s in file_sources:
            # Watchdog paths must be created or exist
            watch_path = s.params.get("watch_path", "")
            os.makedirs(watch_path, exist_ok=True)
            
            configs.append(FileSourceConfig(
                source_id=s.id,
                watch_path=watch_path,
                recursive=s.params.get("recursive", False),
                tags=s.tags,
                watch_creates="create" in s.params.get("events", ["create", "modify", "delete"]),
                watch_modifies="modify" in s.params.get("events", ["create", "modify", "delete"]),
                watch_deletes="delete" in s.params.get("events", ["create", "modify", "delete"]),
                cert_expiry_warn_days=s.params.get("cert_expiry_warn_days", 30),
                is_secret="secret" in s.id.lower() or s.params.get("is_secret", False),
            ))
        print(f"[Orchestrator] FileAdapter: Watching {len(configs)} directories.")
        adapter = FileAdapter(
            sources=configs,
            kafka_bootstrap=KAFKA_BOOTSTRAP,
        )
        tasks.append(run_adapter_safe("FileAdapter", adapter.run))

    # 6. SENSOR ADAPTERS (Disabled)
    # sensor_sources = by_type.get("sensor", [])
    # if sensor_sources:
    #     configs = []
    #     for s in sensor_sources:
    #         configs.append(SensorSourceConfig(
    #             source_id=s.id,
    #             sensor_type=s.params.get("sensor_type", "generic"),
    #             protocol=s.params.get("protocol", "mqtt").lower(),
    #             mqtt_broker=s.params.get("mqtt_broker", "localhost"),
    #             mqtt_port=s.params.get("mqtt_port", 1883),
    #             mqtt_topic=s.params.get("mqtt_topic", "sensors/#"),
    #             poll_url=s.params.get("endpoint", s.params.get("poll_url", "")),
    #             poll_interval_s=s.params.get("poll_interval_s", 30),
    #             tags=s.tags,
    #             thresholds=s.params.get("thresholds", {}),
    #             location=s.params.get("location", ""),
    #         ))
    #     print(f"[Orchestrator] SensorAdapter: Ingesting data for {len(configs)} IoT sensors.")
    #     adapter = SensorAdapter(
    #         sources=configs,
    #         kafka_bootstrap=KAFKA_BOOTSTRAP,
    #     )
    #     tasks.append(run_adapter_safe("SensorAdapter", adapter.run))

    # 7. METRIC Webhook info
    metric_sources = by_type.get("metric", [])
    if metric_sources:
        print(f"[Orchestrator] Metric sources configured: {len(metric_sources)} sources (Handled directly by Push Webhook API).")

    # 8. REDIS ADAPTERS
    redis_sources = by_type.get("redis", [])
    if redis_sources:
        configs = []
        for s in redis_sources:
            configs.append(RedisSourceConfig(
                source_id=s.id,
                url=s.params.get("url", "redis://localhost:6379"),
                poll_interval_s=s.params.get("poll_interval_s", 15),
                memory_pct_critical=s.params.get("memory_pct_critical", 0.90),
                eviction_warn_delta=s.params.get("eviction_warn_delta", 10),
                hit_ratio_warn=s.params.get("hit_ratio_warn", 0.70),
                tags=s.tags,
            ))
        print(f"[Orchestrator] RedisAdapter: Polling Redis metrics for {len(configs)} hosts.")
        adapter = RedisAdapter(
            sources=configs,
            kafka_bootstrap=KAFKA_BOOTSTRAP,
        )
        tasks.append(run_adapter_safe("RedisAdapter", adapter.run))

    if not tasks:
        print("[Orchestrator] Warning: No adapter tasks are scheduled. Check config enabled flags.")
        return

    print(f"[Orchestrator] Initialized {len(tasks)} adapter runtimes concurrently.")
    
    # Run all tasks concurrently
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("[Orchestrator] Shutdown signal received.")
    except Exception as e:
        print(f"[Orchestrator] Critical error in gather loop: {e}")


async def run_adapter_safe(name: str, run_func):
    """Wraps an adapter execution in error-handling and restart logic."""
    backoff = 2
    while True:
        try:
            print(f"[{name}] Starting...")
            await run_func()
            print(f"[{name}] Finished execution.")
            break
        except asyncio.CancelledError:
            print(f"[{name}] Cancelled.")
            raise
        except Exception as e:
            print(f"[{name}] CRITICAL FAILURE: {e}")
            traceback.print_exc()
            print(f"[{name}] Restarting in {backoff} seconds...")
            await asyncio.sleep(backoff)
            backoff = min(60, backoff * 2)


if __name__ == "__main__":
    config_file = sys.argv[1] if len(sys.argv) > 1 else "sources.yaml"
    try:
        asyncio.run(run_orchestrator(config_file))
    except KeyboardInterrupt:
        print("\nOrchestrator stopped cleanly.")
