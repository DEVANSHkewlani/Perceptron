"""
Sensor Adapter  (source_type: sensor)
======================================
Connects to IoT/physical sensors via MQTT or HTTP push/poll.

SUPPORTED PROTOCOLS:
  - MQTT (via aiomqtt) — most IoT sensors
  - HTTP poll (for sensors with REST APIs)
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field

from aiokafka import AIOKafkaProducer

from ..normalizers.sensor_normalizer import SensorNormalizer
from ..schema.event import CognitiveEvent, PerceptionFailure


@dataclass
class SensorSourceConfig:
    """Configuration for a sensor source."""
    source_id: str
    sensor_type: str
    protocol: str = "mqtt"
    mqtt_broker: str = "localhost"
    mqtt_port: int = 1883
    mqtt_topic: str = "sensors/#"
    poll_url: str = ""
    poll_interval_s: int = 30
    tags: list[str] = field(default_factory=list)
    thresholds: dict = field(default_factory=dict)
    location: str = ""


class SensorAdapter:
    """
    Ingests sensor data via MQTT subscription or HTTP polling.
    Normalizes readings and publishes CognitiveEvents to Kafka.
    """

    def __init__(
        self,
        sources: list[SensorSourceConfig],
        kafka_bootstrap: str = "localhost:9092",
        kafka_topic: str = "cognitive.events",
    ):
        self.sources = sources
        self.kafka_url = kafka_bootstrap
        self.topic = kafka_topic
        self.normalizer = SensorNormalizer()
        self._producer: AIOKafkaProducer | None = None

    async def run(self):
        """Start all sensor ingestion loops concurrently."""
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_url,
            enable_idempotence=True,
            acks="all",
        )
        await self._producer.start()
        try:
            tasks = []
            for src in self.sources:
                if src.protocol == "mqtt":
                    tasks.append(self._run_mqtt(src))
                elif src.protocol == "http_poll":
                    tasks.append(self._run_http_poll(src))
                else:
                    print(
                        f"[SensorAdapter] Unknown protocol: "
                        f"{src.protocol} for {src.source_id}"
                    )
            if tasks:
                await asyncio.gather(*tasks)
        finally:
            await self._producer.stop()

    async def _run_mqtt(self, src: SensorSourceConfig):
        """Subscribe to MQTT topic and process sensor messages."""
        try:
            import aiomqtt
        except ImportError:
            print(
                "[SensorAdapter] aiomqtt not installed. "
                "Install with: pip install aiomqtt"
            )
            return

        while True:
            try:
                async with aiomqtt.Client(
                    hostname=src.mqtt_broker, port=src.mqtt_port
                ) as client:
                    await client.subscribe(src.mqtt_topic)
                    async for message in client.messages:
                        try:
                            payload = json.loads(
                                message.payload.decode("utf-8")
                            )
                            payload.setdefault("sensor_type", src.sensor_type)
                            payload.setdefault("location", src.location)
                            payload.setdefault("thresholds", src.thresholds)
                            await self._process(payload, src.source_id)
                        except Exception as e:
                            print(f"[SensorAdapter] MQTT msg error: {e}")
            except Exception as e:
                print(
                    f"[SensorAdapter] MQTT connection error "
                    f"{src.source_id}: {e}"
                )
                await asyncio.sleep(5)

    async def _run_http_poll(self, src: SensorSourceConfig):
        """Poll a sensor HTTP endpoint on schedule."""
        import httpx

        async with httpx.AsyncClient(timeout=10) as client:
            while True:
                try:
                    resp = await client.get(src.poll_url)
                    if resp.status_code == 200:
                        payload = resp.json()
                        payload.setdefault("sensor_type", src.sensor_type)
                        payload.setdefault("location", src.location)
                        payload.setdefault("thresholds", src.thresholds)
                        await self._process(payload, src.source_id)
                except Exception as e:
                    print(
                        f"[SensorAdapter] HTTP poll error "
                        f"{src.source_id}: {e}"
                    )
                await asyncio.sleep(src.poll_interval_s)

    async def _process(self, raw: dict, source_id: str):
        """Normalize and publish a sensor reading."""
        result = self.normalizer.normalize(raw, source_id)
        if isinstance(result, (CognitiveEvent, PerceptionFailure)):
            import time
            t0 = time.monotonic()
            await self._producer.send_and_wait(
                self.topic,
                result.model_dump_json().encode("utf-8"),
            )
            kafka_lag_ms = (time.monotonic() - t0) * 1000
            if kafka_lag_ms > 500:
                print(f"[WARNING] [SensorAdapter] Kafka publish lag high: {kafka_lag_ms:.1f}ms "
                      f"for {result.event_type if hasattr(result, 'event_type') else 'failure'}")
