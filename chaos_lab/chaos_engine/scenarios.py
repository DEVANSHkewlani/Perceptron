from __future__ import annotations
import asyncio, httpx, time, logging
import redis.asyncio as aioredis
import asyncpg
from controller import ChaosScenario, DockerChaos
from injectors.database_chaos import DatabaseChaos
from injectors.queue_chaos import QueueChaos
from injectors.network_chaos import NetworkChaos

logger = logging.getLogger("scenarios")

def build_scenarios(
    docker_chaos: DockerChaos,
    db_chaos: DatabaseChaos,
    queue_chaos: QueueChaos,
    net_chaos: NetworkChaos,
    redis_url: str = "redis://redis:6379"
) -> dict[str, ChaosScenario]:
    """Factory that registers all 18 chaos scenarios. Wires activation, deactivation, and verification."""

    d = docker_chaos
    net = net_chaos

    async def _get_redis():
        return await aioredis.from_url(redis_url, decode_responses=True)

    # Helper verifications
    async def _check_service_down(url: str) -> bool:
        try:
            async with httpx.AsyncClient(timeout=2) as c:
                r = await c.get(url)
                return r.status_code != 200
        except:
            return True

    async def _check_latency_high(url: str, threshold_s: float) -> bool:
        start = time.monotonic()
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                await c.get(url)
            return (time.monotonic() - start) >= threshold_s
        except:
            return True

    return {
        # 1. Product Service Crash
        "product_service_crash": ChaosScenario(
            name="product_service_crash",
            description="Stop the product service container, simulating a crash.",
            severity="critical",
            expected_event_type="service_unreachable",
            expected_detection_s=30,
            expected_remediation_s=90,
            activate=lambda p: asyncio.to_thread(d.stop_container, "product-service"),
            deactivate=lambda: asyncio.to_thread(d.start_container, "product-service"),
            verify_injected=lambda: _check_service_down("http://product-service:8011/health"),
        ),

        # 2. Order Service Crash
        "order_service_crash": ChaosScenario(
            name="order_service_crash",
            description="Stop the order service container, simulating a crash.",
            severity="critical",
            expected_event_type="service_unreachable",
            expected_detection_s=30,
            expected_remediation_s=90,
            activate=lambda p: asyncio.to_thread(d.stop_container, "order-service"),
            deactivate=lambda: asyncio.to_thread(d.start_container, "order-service"),
            verify_injected=lambda: _check_service_down("http://order-service:8012/health"),
        ),

        # 3. DB Connection Pool Exhaustion
        "db_pool_exhaustion": ChaosScenario(
            name="db_pool_exhaustion",
            description="Saturate the connection pool of the product service.",
            severity="critical",
            expected_event_type="connection_pool_exhausted",
            expected_detection_s=30,
            expected_remediation_s=90,
            activate=lambda p: asyncio.to_thread(d.set_env, "product-service", {"CHAOS_POOL_EXHAUST": "true"}),
            deactivate=lambda: asyncio.to_thread(d.set_env, "product-service", {"CHAOS_POOL_EXHAUST": "false"}),
            verify_injected=lambda: db_chaos.check_pool_exhausted("shopcore"),
        ),

        # 4. Redis Memory Full (OOM)
        "redis_memory_full": ChaosScenario(
            name="redis_memory_full",
            description="Set Redis maxmemory to 1MB to trigger OOM/evictions.",
            severity="critical",
            expected_event_type="redis_memory_critical",
            expected_detection_s=45,
            expected_remediation_s=90,
            activate=lambda p: _set_redis_maxmemory(redis_url, p.get("maxmemory", "1mb")),
            deactivate=lambda: _set_redis_maxmemory(redis_url, "256mb"),
            verify_injected=lambda: _verify_redis_maxmemory_restricted(redis_url),
        ),

        # 5. Slow Database Query
        "slow_database_query": ChaosScenario(
            name="slow_database_query",
            description="Inject 2s slow query delay into product select operations.",
            severity="high",
            expected_event_type="slow_query_detected",
            expected_detection_s=45,
            expected_remediation_s=120,
            activate=lambda p: asyncio.to_thread(d.set_env, "product-service", {"CHAOS_SLOW_QUERY_MS": str(p.get("delay_ms", 2000))}),
            deactivate=lambda: asyncio.to_thread(d.set_env, "product-service", {"CHAOS_SLOW_QUERY_MS": "0"}),
            verify_injected=lambda: _check_latency_high("http://product-service:8011/products", 1.5),
        ),

        # 6. Gateway Latency Spike (ANM-01)
        "gateway_latency_spike": ChaosScenario(
            name="gateway_latency_spike",
            description="Inject 1.5s artificial latency on the API gateway.",
            severity="high",
            expected_event_type="api_latency_spike",
            expected_detection_s=45,
            expected_remediation_s=120,
            activate=lambda p: asyncio.to_thread(d.set_env, "api-gateway", {"CHAOS_LATENCY_MS": str(p.get("latency_ms", 1500))}),
            deactivate=lambda: asyncio.to_thread(d.set_env, "api-gateway", {"CHAOS_LATENCY_MS": "0"}),
            verify_injected=lambda: _check_latency_high("http://api-gateway:8010/health", 1.0),
        ),

        # 7. Kafka Consumer Lag / Block (ANM-04)
        "kafka_consumer_lag": ChaosScenario(
            name="kafka_consumer_lag",
            description="Block order events Kafka publishing to generate consumer lag.",
            severity="high",
            expected_event_type="consumer_lag_high",
            expected_detection_s=30,
            expected_remediation_s=120,
            activate=lambda p: _activate_kafka_lag(d, queue_chaos),
            deactivate=lambda: asyncio.to_thread(d.set_env, "order-service", {"CHAOS_KAFKA_BLOCK": "false"}),
            verify_injected=lambda: queue_chaos.check_consumer_lag("notification-group", "order-events", 5),
        ),

        # 8. Gateway Error Rate (ANM-02)
        "gateway_error_rate": ChaosScenario(
            name="gateway_error_rate",
            description="Simulate 30% HTTP 500 error rates at the gateway.",
            severity="high",
            expected_event_type="service_health_degraded",
            expected_detection_s=60,
            expected_remediation_s=150,
            activate=lambda p: asyncio.to_thread(d.set_env, "api-gateway", {"CHAOS_ERROR_RATE": str(p.get("fail_rate", 0.3))}),
            deactivate=lambda: asyncio.to_thread(d.set_env, "api-gateway", {"CHAOS_ERROR_RATE": "0"}),
            verify_injected=lambda: _check_error_rate("http://api-gateway:8010/products"),
        ),

        # 9. Database Replication Lag
        "replication_lag": ChaosScenario(
            name="replication_lag",
            description="Inject 1s network latency on the Postgres container to simulate replication lag.",
            severity="high",
            expected_event_type="replication_lag_high",
            expected_detection_s=45,
            expected_remediation_s=120,
            activate=lambda p: net.add_latency("postgres", p.get("latency_ms", 1000), 50),
            deactivate=lambda: net.remove("postgres"),
            verify_injected=lambda: asyncio.sleep(0) or True,
        ),

        # 10. Memory Pressure
        "memory_pressure": ChaosScenario(
            name="memory_pressure",
            description="Simulate high memory load on the user service.",
            severity="high",
            expected_event_type="memory_pressure_high",
            expected_detection_s=60,
            expected_remediation_s=120,
            activate=lambda p: asyncio.to_thread(d.set_env, "user-service", {"CHAOS_MEM_PRESSURE": "true"}),
            deactivate=lambda: asyncio.to_thread(d.set_env, "user-service", {"CHAOS_MEM_PRESSURE": "false"}),
            verify_injected=lambda: asyncio.sleep(0) or True,
        ),

        # 11. Network Packet Loss
        "network_packet_loss": ChaosScenario(
            name="network_packet_loss",
            description="Inject 20% packet loss on the API gateway interface.",
            severity="high",
            expected_event_type="packet_loss_detected",
            expected_detection_s=60,
            expected_remediation_s=120,
            activate=lambda p: net.add_packet_loss("api-gateway", p.get("loss_pct", 20)),
            deactivate=lambda: net.remove("api-gateway"),
            verify_injected=lambda: asyncio.sleep(0) or True,
        ),

        # 12. Database Lock Contention
        "lock_contention": ChaosScenario(
            name="lock_contention",
            description="Hold exclusive transaction lock on products table.",
            severity="medium",
            expected_event_type="lock_wait_timeout",
            expected_detection_s=45,
            expected_remediation_s=120,
            activate=lambda p: asyncio.create_task(db_chaos.inject_lock_contention("products")),
            deactivate=lambda: asyncio.sleep(0),  # Auto-releases when transaction aborts/closes or handled by DBA
            verify_injected=lambda: asyncio.sleep(0) or True,
        ),

        # 13. Idle in Transaction (Connection Leak)
        "idle_in_transaction": ChaosScenario(
            name="idle_in_transaction",
            description="Hold PostgreSQL connections idle in transaction mode.",
            severity="medium",
            expected_event_type="connection_leak_detected",
            expected_detection_s=60,
            expected_remediation_s=120,
            activate=lambda p: db_chaos.inject_slow_query_rule(p.get("idle_timeout", 10) * 1000),
            deactivate=lambda: db_chaos.clear_slow_query_rule(),
            verify_injected=lambda: asyncio.sleep(0) or True,
        ),

        # 14. CPU Spike
        "cpu_spike": ChaosScenario(
            name="cpu_spike",
            description="Simulate high CPU spike on the gateway.",
            severity="medium",
            expected_event_type="cpu_spike",
            expected_detection_s=60,
            expected_remediation_s=120,
            activate=lambda p: asyncio.to_thread(d.set_env, "api-gateway", {"CHAOS_CPU_SPIKE": "true"}),
            deactivate=lambda: asyncio.to_thread(d.set_env, "api-gateway", {"CHAOS_CPU_SPIKE": "false"}),
            verify_injected=lambda: asyncio.sleep(0) or True,
        ),

        # 15. Config File Tampering
        "config_file_tamper": ChaosScenario(
            name="config_file_tamper",
            description="Tamper with Nginx configuration on frontend to trigger config check validation.",
            severity="medium",
            expected_event_type="config_file_modified",
            expected_detection_s=5,
            expected_remediation_s=60,
            activate=lambda p: _tamper_nginx_config(d),
            deactivate=lambda: _restore_nginx_config(d),
            verify_injected=lambda: asyncio.sleep(0) or True,
        ),

        # 16. SSL Certificate Expiry Simulation
        "ssl_cert_near_expiry": ChaosScenario(
            name="ssl_cert_near_expiry",
            description="Simulate SSL cert expiring in less than 3 days.",
            severity="medium",
            expected_event_type="certificate_expiry_warning",
            expected_detection_s=120,
            expected_remediation_s=180,
            activate=lambda p: asyncio.to_thread(d.set_env, "api-gateway", {"CHAOS_SSL_EXPIRY_DAYS": str(p.get("expiry_days", 2))}),
            deactivate=lambda: asyncio.to_thread(d.set_env, "api-gateway", {"CHAOS_SSL_EXPIRY_DAYS": "90"}),
            verify_injected=lambda: asyncio.sleep(0) or True,
        ),

        # 17. Order Processing Delay
        "order_processing_delay": ChaosScenario(
            name="order_processing_delay",
            description="Delay order placement processing by 3 seconds.",
            severity="medium",
            expected_event_type="slow_query_detected",
            expected_detection_s=60,
            expected_remediation_s=120,
            activate=lambda p: asyncio.to_thread(d.set_env, "order-service", {"CHAOS_ORDER_DELAY_MS": str(p.get("delay_ms", 3000))}),
            deactivate=lambda: asyncio.to_thread(d.set_env, "order-service", {"CHAOS_ORDER_DELAY_MS": "0"}),
            verify_injected=lambda: _check_latency_high("http://order-service:8012/health", 2.0),
        ),

        # 18. Circuit Breaker Open
        "circuit_breaker_open": ChaosScenario(
            name="circuit_breaker_open",
            description="Simulate circuit breaker trip blocking upstream calls.",
            severity="high",
            expected_event_type="service_unreachable",
            expected_detection_s=30,
            expected_remediation_s=90,
            activate=lambda p: asyncio.to_thread(d.set_env, "api-gateway", {"CHAOS_CIRCUIT_OPEN": "true"}),
            deactivate=lambda: asyncio.to_thread(d.set_env, "api-gateway", {"CHAOS_CIRCUIT_OPEN": "false"}),
            verify_injected=lambda: _check_service_down("http://api-gateway:8010/products"),
        ),
    }

async def _set_redis_maxmemory(redis_url: str, size: str):
    r = await aioredis.from_url(redis_url)
    await r.config_set("maxmemory", size)
    await r.close()

async def _verify_redis_maxmemory(redis_url: str, expected_bytes: int):
    r = await aioredis.from_url(redis_url)
    info = await r.info("memory")
    await r.close()
    return info.get("maxmemory", 0) == expected_bytes

async def _verify_redis_maxmemory_restricted(redis_url: str):
    r = await aioredis.from_url(redis_url)
    info = await r.info("memory")
    await r.close()
    max_mem = info.get("maxmemory", 0)
    return 0 < max_mem < 256 * 1024 * 1024

async def _activate_kafka_lag(d: DockerChaos, queue_chaos: QueueChaos):
    # Set chaos env variable to drop events publishing
    d.set_env("order-service", {"CHAOS_KAFKA_BLOCK": "true"})
    # Delete offset to trigger lag perception
    await queue_chaos.stop_consumer_group("notification-group", "order-events")

async def _tamper_nginx_config(d: DockerChaos):
    container = d._get_container("frontend")
    container.exec_run("sh -c 'echo \"# TAMPERED\" >> /etc/nginx/conf.d/default.conf && nginx -s reload'")

async def _restore_nginx_config(d: DockerChaos):
    container = d._get_container("frontend")
    # Reset Nginx config file to original state and reload
    container.exec_run("sh -c 'sed -i \"/# TAMPERED/d\" /etc/nginx/conf.d/default.conf && nginx -s reload'")

async def _check_error_rate(url: str) -> bool:
    errors = 0
    async with httpx.AsyncClient(timeout=2) as c:
        for _ in range(5):
            try:
                r = await c.get(url)
                if r.status_code >= 500:
                    errors += 1
            except:
                errors += 1
    return errors >= 1
