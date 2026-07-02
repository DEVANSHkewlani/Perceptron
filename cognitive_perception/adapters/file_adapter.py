"""
File Adapter  (source_type: file)
==================================
Watches filesystem paths using OS kernel inotify (Linux)
or fsevents (macOS) via Python watchdog library.
Not polling — the OS kernel pushes change events instantly (<10ms).
"""

from __future__ import annotations

import asyncio
import ssl
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from aiokafka import AIOKafkaProducer

from ..normalizers.file_normalizer import FileNormalizer


@dataclass
class FileSourceConfig:
    source_id: str
    watch_path: str
    recursive: bool = False
    tags: list[str] = field(default_factory=list)
    watch_creates: bool = True
    watch_modifies: bool = True
    watch_deletes: bool = True
    cert_expiry_warn_days: int = 30
    is_secret: bool = False


def _check_cert_expiry(cert_path: str) -> int | None:
    """Read a certificate file and return days until expiry."""
    try:
        cert = ssl._ssl._test_decode_cert(cert_path)
        not_after = datetime.strptime(
            cert["notAfter"], "%b %d %H:%M:%S %Y %Z"
        )
        not_after = not_after.replace(tzinfo=timezone.utc)
        return (not_after - datetime.now(timezone.utc)).days
    except Exception:
        return None


class FileAdapter:
    """
    Watches filesystem paths using Python watchdog.
    Converts filesystem events → CognitiveEvents → Kafka.
    Bridges watchdog threads to the async event loop via asyncio.Queue.
    """

    def __init__(
        self,
        sources: list[FileSourceConfig],
        kafka_bootstrap: str = "localhost:9092",
        kafka_topic: str = "cognitive.events",
    ):
        self.sources = sources
        self.kafka_url = kafka_bootstrap
        self.topic = kafka_topic
        self.normalizer = FileNormalizer()
        self._event_queue: asyncio.Queue = asyncio.Queue()
        self._producer: AIOKafkaProducer | None = None

    async def run(self):
        """Start watchdog observers and the async event publisher."""
        try:
            from watchdog.observers import Observer
        except ImportError:
            raise ImportError("Install watchdog: pip install watchdog")

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_url,
            enable_idempotence=True,
            acks="all",
        )
        await self._producer.start()

        loop = asyncio.get_running_loop()

        observers = []
        for src in self.sources:
            handler = self._make_handler(src, loop)
            observer = Observer()
            observer.schedule(handler, src.watch_path, recursive=src.recursive)
            observer.start()
            observers.append(observer)

        try:
            await self._publish_loop()
        finally:
            for obs in observers:
                obs.stop()
                obs.join()
            await self._producer.stop()

    def _make_handler(self, src: FileSourceConfig, loop: asyncio.AbstractEventLoop):
        """Create a watchdog event handler for a source config."""
        from watchdog.events import FileSystemEventHandler

        queue = self._event_queue
        source_id = src.source_id
        is_secret = src.is_secret

        class Handler(FileSystemEventHandler):
            def _handle(self_h, event, kind: str):
                path = event.src_path
                ext = Path(path).suffix.lower()
                cert_days = None
                if (
                    ext in FileNormalizer.CERT_EXTENSIONS
                    and not event.is_directory
                ):
                    cert_days = _check_cert_expiry(path)

                raw = {
                    "event_kind": kind,
                    "path": path,
                    "is_directory": event.is_directory,
                    "is_secret": is_secret,
                    "cert_days_remaining": cert_days,
                    "source_id": source_id,
                }
                try:
                    loop.call_soon_threadsafe(
                        queue.put_nowait, (raw, source_id)
                    )
                except Exception:
                    pass

            def on_created(self_h, event):
                if src.watch_creates:
                    self_h._handle(event, "created")

            def on_modified(self_h, event):
                if src.watch_modifies and not event.is_directory:
                    self_h._handle(event, "modified")

            def on_deleted(self_h, event):
                if src.watch_deletes:
                    self_h._handle(event, "deleted")

            def on_moved(self_h, event):
                self_h._handle(event, "moved")

        return Handler()

    async def _publish_loop(self):
        """Consume events from the queue and publish to Kafka."""
        while True:
            try:
                raw, source_id = await asyncio.wait_for(
                    self._event_queue.get(), timeout=1.0
                )
                event = self.normalizer.normalize(raw, source_id)
                import time
                t0 = time.monotonic()
                await self._producer.send_and_wait(
                    self.topic,
                    event.model_dump_json().encode("utf-8"),
                )
                kafka_lag_ms = (time.monotonic() - t0) * 1000
                if kafka_lag_ms > 500:
                    print(f"[WARNING] [FileAdapter] Kafka publish lag high: {kafka_lag_ms:.1f}ms "
                          f"for {event.event_type if hasattr(event, 'event_type') else 'failure'}")
            except asyncio.TimeoutError:
                pass  # normal — no events in the last second
            except Exception as e:
                print(f"[FileAdapter] Publish error: {e}")
