"""
API Adapter  (source_type: api)
================================
Polls HTTP endpoints on a configurable schedule.
Uses change-detection: only emits events when state actually changes.

FLOW:
  1. Reads source config (URL, interval, thresholds)
  2. Polls the endpoint every N seconds using async HTTP
  3. Compares response to cached previous state in Redis
  4. If state changed: normalizes → publishes to Kafka
  5. If no change: discards silently
"""

from __future__ import annotations

import asyncio
import socket
import ssl
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx
import redis.asyncio as aioredis
from aiokafka import AIOKafkaProducer

from ..normalizers.api_normalizer import APINormalizer
from ..schema.event import CognitiveEvent, PerceptionFailure


@dataclass
class APISourceConfig:
    """Configuration for a single API/service source."""
    source_id: str
    url: str
    poll_interval_s: int = 30
    timeout_s: float = 5.0
    expected_status: int = 200
    latency_warn_ms: float = 500.0
    latency_high_ms: float = 2000.0
    tags: list[str] = field(default_factory=list)
    headers: dict[str, str] = field(default_factory=dict)


class APIAdapter:
    """
    Continuously polls a list of API sources.
    Emits CognitiveEvents to Kafka only when state changes.
    """

    def __init__(
        self,
        sources: list[APISourceConfig],
        kafka_bootstrap: str = "localhost:9092",
        kafka_topic: str = "cognitive.events",
        redis_url: str = "redis://localhost:6379",
    ):
        self.sources = sources
        self.kafka_url = kafka_bootstrap
        self.topic = kafka_topic
        self.redis_url = redis_url
        self.normalizer = APINormalizer()
        self._producer: AIOKafkaProducer | None = None
        self._redis: aioredis.Redis | None = None

    async def start(self):
        """Initialize Kafka producer and Redis connection."""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_url,
            enable_idempotence=True,
            acks="all",
        )
        self._redis = await aioredis.from_url(self.redis_url)
        await self._producer.start()

    async def stop(self):
        """Cleanly shut down connections."""
        if self._producer:
            await self._producer.stop()
        if self._redis:
            await self._redis.aclose()

    async def run(self):
        """Main loop. Runs all source pollers concurrently."""
        await self.start()
        try:
            await asyncio.gather(
                *[self._poll_source(src) for src in self.sources]
            )
        finally:
            await self.stop()

    async def _poll_source(self, src: APISourceConfig):
        """Poll a single source forever at its configured interval."""
        async with httpx.AsyncClient(
            timeout=src.timeout_s, headers=src.headers, verify=True,
        ) as client:
            while True:
                try:
                    result = await self._do_poll(client, src)
                    await self._process_result(result, src)
                except Exception as e:
                    print(f"[APIAdapter] Error polling {src.source_id}: {e}")
                await asyncio.sleep(src.poll_interval_s)

    async def _do_poll(
        self, client: httpx.AsyncClient, src: APISourceConfig
    ) -> dict:
        """Make the HTTP request and return a structured result dict."""
        result: dict = {
            "url": src.url,
            "latency_warn_ms": src.latency_warn_ms,
            "latency_high_ms": src.latency_high_ms,
            "tags": src.tags,
        }
        start = time.monotonic()
        try:
            resp = await client.get(src.url)
            latency_ms = (time.monotonic() - start) * 1000
            result.update({
                "status_code": resp.status_code,
                "latency_ms": latency_ms,
                "ok": resp.status_code == src.expected_status,
                "error": None,
            })
            try:
                result["response_body"] = resp.json()
            except Exception:
                pass
            if src.url.startswith("https://"):
                loop = asyncio.get_running_loop()
                result["ssl_days_remaining"] = await loop.run_in_executor(
                    None, self._check_ssl, src.url
                )
        except httpx.ConnectError as e:
            result.update({
                "status_code": None, "latency_ms": 0,
                "ok": False, "error": str(e),
            })
        except httpx.TimeoutException:
            result.update({
                "status_code": None,
                "latency_ms": src.timeout_s * 1000,
                "ok": False, "error": "timeout",
            })
        return result

    async def _process_result(self, result: dict, src: APISourceConfig):
        """Check if state changed. If yes, normalize and publish."""
        latency_bucket = (
            "ok" if result.get("latency_ms", 0) < src.latency_warn_ms
            else "slow" if result.get("latency_ms", 0) < src.latency_high_ms
            else "very_slow"
        )
        current_state = (
            f"{result.get('ok')}:{result.get('status_code')}:{latency_bucket}"
        )
        state_key = f"api_state:{src.source_id}"
        prev_state = await self._redis.get(state_key)
        if prev_state is not None and prev_state.decode() == current_state:
            return  # No change — don't emit
        await self._redis.set(state_key, current_state, ex=3600)
        event_or_failure = self.normalizer.normalize(result, src.source_id)
        await self._publish(event_or_failure)

    async def _publish(self, event: CognitiveEvent | PerceptionFailure):
        """Serialize and publish to Kafka with publish lag monitoring."""
        import time
        payload = event.model_dump_json().encode("utf-8")
        
        t0 = time.monotonic()
        await self._producer.send_and_wait(self.topic, payload)
        kafka_lag_ms = (time.monotonic() - t0) * 1000
        
        if kafka_lag_ms > 500:
            print(f"[WARNING] [APIAdapter] Kafka publish lag high: {kafka_lag_ms:.1f}ms "
                  f"for {event.event_type if hasattr(event, 'event_type') else 'failure'}")

    @staticmethod
    def _check_ssl(url: str) -> int | None:
        """Return days remaining on the SSL certificate, or None."""
        try:
            parsed = urlparse(url)
            host = parsed.hostname
            port = parsed.port or 443
            ctx = ssl.create_default_context()
            with ctx.wrap_socket(socket.socket(), server_hostname=host) as s:
                s.settimeout(3.0)
                s.connect((host, port))
                cert = s.getpeercert()
                expiry_str = cert["notAfter"]
                expiry = datetime.strptime(expiry_str, "%b %d %H:%M:%S %Y %Z")
                expiry = expiry.replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                return (expiry - now).days
        except Exception:
            return None
