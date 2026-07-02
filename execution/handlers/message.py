"""
MessageHandler — send_alert and notification actions.
Uses httpx for Slack/PagerDuty webhooks.
Includes deduplication via Redis to prevent alert storms.
"""
from __future__ import annotations
import hashlib
import json
import httpx
import redis.asyncio as aioredis
from .base import BaseActionHandler


class MessageHandler(BaseActionHandler):
    def __init__(self, producer, redis_url: str, slack_webhook: str = ""):
        super().__init__(producer)
        self._redis_url = redis_url
        self._slack_url = slack_webhook
        self._redis: aioredis.Redis | None = None

    async def connect(self) -> None:
        try:
            self._redis = await aioredis.from_url(self._redis_url, decode_responses=True)
        except Exception:
            self._redis = None

    async def disconnect(self) -> None:
        if self._redis:
            await self._redis.aclose()

    async def execute(self, action: str, parameters: dict) -> dict:
        if action in ("send_alert", "monitor_and_wait"):
            return await self._send_alert(parameters)
        return {"sent": False, "reason": f"unknown message action: {action}"}

    async def _send_alert(self, params: dict) -> dict:
        message = params.get("message", "")
        severity = params.get("severity", "medium")

        # Deduplication: don't send the same alert twice in 5 minutes
        dedup_key = f"alert_dedup:{hashlib.md5(message.encode()).hexdigest()}"
        if self._redis:
            try:
                if await self._redis.exists(dedup_key):
                    return {"sent": False, "reason": "deduplicated"}
                await self._redis.setex(dedup_key, 300, "1")
            except Exception:
                pass

        if self._slack_url:
            async with httpx.AsyncClient(timeout=10.0) as client:
                emoji = {"critical": "🔴", "high": "🟠", "medium": "🟡"}.get(severity, "ℹ️")
                await client.post(self._slack_url, json={
                    "text": f"{emoji} *[{severity.upper()}]* {message}"
                })
        return {"sent": True, "severity": severity}
