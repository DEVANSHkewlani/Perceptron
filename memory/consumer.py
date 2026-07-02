"""
Memory consumer entrypoint.

Runs the MemoryRouter as a standalone long-lived process so the Kafka
`cognitive.events` stream is persisted into all memory layers.
"""
from __future__ import annotations

import asyncio
import os

from .router import MemoryRouter


async def main() -> None:
    router = MemoryRouter(
        kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")),
        postgres_dsn=os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/cognitive"),
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379"),
        neo4j_uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        neo4j_user=os.getenv("NEO4J_USER", "neo4j"),
        neo4j_password=os.getenv("NEO4J_PASSWORD", "password123"),
        qdrant_url=os.getenv("QDRANT_URL", "http://localhost:6333"),
        embed_events=os.getenv("MEMORY_EMBED_EVENTS", "true").lower() != "false",
    )
    await router.run()


if __name__ == "__main__":
    print("Starting Memory Router consumer... Press Ctrl+C to stop.")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nMemory Router stopped.")
