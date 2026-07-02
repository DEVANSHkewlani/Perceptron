"""
DockerHandler — executes docker commands on ShopCore containers for remediation.
Used for: restart_service, restart_connection_pool, scale_consumer_group, kill_slow_query.
"""
from __future__ import annotations
import asyncio
import os
import httpx
from aiokafka import AIOKafkaProducer
from .base import BaseActionHandler

SERVICE_ALIASES = {
    "svc:api-gateway": "api-gateway",
    "svc:api-gateway-traffic": "api-gateway",
    "svc:product-service": "product-service",
    "svc:product-service-workload": "product-service",
    "svc:order-service": "order-service",
    "svc:user-service": "user-service",
    "svc:cart-service": "cart-service",
    "svc:notification-service": "notification-service",
    "db:shopcore-postgres": "product-service",
    "queue:order-events": "notification-service",
}

CHAOS_SCENARIO_BY_ACTION = {
    "restart_connection_pool": "db_pool_exhaustion",
    "scale_consumer_group": "kafka_consumer_lag",
    "kill_slow_query": "slow_database_query",
}


class DockerHandler(BaseActionHandler):
    def __init__(self, producer: AIOKafkaProducer | None):
        super().__init__(producer)
        self.chaos_url = os.getenv("CHAOS_ENGINE_URL", "http://localhost:9091")

    def _resolve_service(self, raw: str) -> str:
        if not raw:
            return "product-service"
        if raw in SERVICE_ALIASES:
            return SERVICE_ALIASES[raw]
        cleaned = raw.replace("svc:", "").replace("db:", "").replace("queue:", "")
        for key, val in SERVICE_ALIASES.items():
            if cleaned in key or cleaned in val:
                return val
        if "gateway" in cleaned:
            return "api-gateway"
        if "product" in cleaned:
            return "product-service"
        if "order" in cleaned:
            return "order-service"
        if "user" in cleaned:
            return "user-service"
        if "cart" in cleaned:
            return "cart-service"
        if "notification" in cleaned or "queue" in cleaned:
            return "notification-service"
        return cleaned.replace("shopcore-", "")

    async def _deactivate_chaos(self, scenario: str) -> None:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                await client.post(f"{self.chaos_url}/scenarios/{scenario}/deactivate")
        except Exception:
            pass

    async def execute(self, action: str, parameters: dict) -> dict:
        service_id = self._resolve_service(
            parameters.get("service_id")
            or parameters.get("db_id")
            or parameters.get("queue_id")
            or ""
        )
        container_name = f"shopcore-{service_id}"
        scenario = CHAOS_SCENARIO_BY_ACTION.get(action)
        if scenario:
            await self._deactivate_chaos(scenario)

        if action == "restart_connection_pool":
            cmd = "docker restart shopcore-product-service"
            containers = ["shopcore-product-service"]
        elif action == "scale_consumer_group":
            cmd = "docker restart shopcore-order-service shopcore-notification-service"
            containers = ["shopcore-order-service", "shopcore-notification-service"]
        elif action == "kill_slow_query":
            pid = parameters.get("pid")
            if pid:
                cmd = (
                    f"docker exec shopcore-postgres psql -U shopcore -d shopcore "
                    f"-c \"SELECT pg_terminate_backend({int(pid)})\""
                )
            else:
                cmd = "docker restart shopcore-product-service"
            containers = [container_name]
        elif action == "restart_service":
            cmd = f"docker restart {container_name}"
            containers = [container_name]
        else:
            cmd = f"docker restart {container_name}"
            containers = [container_name]

        process = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            err_msg = stderr.decode().strip()
            raise Exception(f"Command '{cmd}' failed: {err_msg}")

        return {
            "status": "success",
            "action": action,
            "container": container_name,
            "containers": containers,
            "command": cmd,
            "output": stdout.decode().strip(),
        }
