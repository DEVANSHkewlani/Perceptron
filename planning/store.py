"""
PlanStore — Redis-backed storage persistence for active and completed Plans.
"""
from __future__ import annotations
import redis.asyncio as aioredis
from .schema import Plan


class PlanStore:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self._redis_url = redis_url
        self._redis: aioredis.Redis | None = None

    async def connect(self) -> None:
        self._redis = await aioredis.from_url(self._redis_url, decode_responses=True)

    async def disconnect(self) -> None:
        if self._redis:
            await self._redis.aclose()

    async def save(self, plan: Plan) -> None:
        if not self._redis:
            raise RuntimeError("Redis client not connected. Call connect() first.")
        await self._redis.setex(
            f"plan:{plan.plan_id}",
            86400,
            plan.model_dump_json()
        )

    async def get(self, plan_id: str) -> Plan | None:
        if not self._redis:
            raise RuntimeError("Redis client not connected. Call connect() first.")
        raw = await self._redis.get(f"plan:{plan_id}")
        return Plan.model_validate_json(raw) if raw else None

    async def list_active(self) -> list[Plan]:
        if not self._redis:
            raise RuntimeError("Redis client not connected. Call connect() first.")
        keys = await self._redis.keys("plan:*")
        plans = []
        for k in keys:
            raw = await self._redis.get(k)
            if raw:
                p = Plan.model_validate_json(raw)
                status_str = p.status.value if hasattr(p.status, "value") else str(p.status)
                if status_str in ("created", "running", "awaiting_approval"):
                    plans.append(p)
        return plans
