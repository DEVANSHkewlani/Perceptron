import asyncio, random, logging, argparse, httpx, time
from datetime import datetime, timezone

logger = logging.getLogger("chaos-scheduler")
logging.basicConfig(format="%(message)s", level=logging.INFO)

ALL_SCENARIOS = [
    "product_service_crash", "slow_database_query", "db_pool_exhaustion",
    "gateway_latency_spike", "kafka_consumer_lag", "gateway_error_rate",
    "replication_lag", "memory_pressure", "network_packet_loss",
    "lock_contention", "idle_in_transaction", "cpu_spike",
    "config_file_tamper", "ssl_cert_near_expiry", "order_processing_delay",
    "circuit_breaker_open"
]

CHAOS_API = "http://localhost:9091"

async def run_gauntlet(
    mode: str = "random",
    interval_min: int = 120,
    interval_max: int = 300,
    duration_minutes: int = 60,
    inject_duration_s: int = 120,
) -> None:
    deadline = time.monotonic() + duration_minutes * 60
    scenario_list = ALL_SCENARIOS[:]
    if mode == "sequential":
        random.shuffle(scenario_list)
    iteration = 0
    active: list[str] = []

    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            iteration += 1

            if mode == "sequential":
                name = scenario_list[(iteration - 1) % len(scenario_list)]
            else:
                name = random.choice(scenario_list)

            logger.info(f"[gauntlet] iter={iteration} → activating {name}")
            try:
                await client.post(f"{CHAOS_API}/scenarios/{name}/activate")
                active.append(name)
            except Exception as e:
                logger.error(f"[gauntlet] failed to activate {name}: {e}")
                continue

            # Wait for DCA to detect + remediate
            await asyncio.sleep(inject_duration_s)

            # Deactivate (safety cleanup even if DCA already healed)
            logger.info(f"[gauntlet] iter={iteration} → deactivating {name}")
            try:
                await client.post(f"{CHAOS_API}/scenarios/{name}/deactivate")
                if name in active:
                    active.remove(name)
            except Exception as e:
                logger.error(f"[gauntlet] failed to deactivate {name}: {e}")

            # Cool-down before next injection
            wait = random.randint(interval_min, interval_max)
            logger.info(f"[gauntlet] cool-down {wait}s before next scenario")
            await asyncio.sleep(wait)

    logger.info(f"[gauntlet] complete after {iteration} iterations")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode",             default="random")
    parser.add_argument("--interval-min",     type=int, default=120)
    parser.add_argument("--interval-max",     type=int, default=300)
    parser.add_argument("--duration-minutes", type=int, default=60)
    parser.add_argument("--inject-duration-s",type=int, default=120)
    args = parser.parse_args()
    asyncio.run(run_gauntlet(
        mode=args.mode,
        interval_min=args.interval_min,
        interval_max=args.interval_max,
        duration_minutes=args.duration_minutes,
        inject_duration_s=args.inject_duration_s,
    ))
