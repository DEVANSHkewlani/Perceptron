import asyncio
import os
import redis.asyncio as aioredis
import asyncpg
from neo4j import AsyncGraphDatabase

async def reset_redis():
    try:
        url = os.getenv("REDIS_URL", "redis://localhost:6379")
        print(f"Connecting to Redis at {url}...")
        r = await aioredis.from_url(url)
        await r.flushall()
        await r.aclose()
        print("Successfully flushed Redis.")
    except Exception as e:
        print(f"Warning: Redis flush failed: {e}")

async def reset_postgres():
    try:
        dsn = os.getenv("POSTGRES_DSN", "postgresql://postgres:postgres@localhost:5432/cognitive")
        print(f"Connecting to Postgres at {dsn}...")
        conn = await asyncpg.connect(dsn)
        # Truncate all tables so we start fully clean
        tables = ["agent_tasks", "cognitive_events", "temporal_patterns", "metric_observations", "temporal_baselines"]
        for table in tables:
            try:
                await conn.execute(f"TRUNCATE TABLE {table} CASCADE;")
                print(f"Truncated Postgres table: {table}")
            except Exception as te:
                print(f"Could not truncate {table}: {te}")
        await conn.close()
        print("Successfully cleaned PostgreSQL database.")
    except Exception as e:
        print(f"Warning: Postgres reset failed: {e}")

async def reset_neo4j():
    try:
        uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        password = os.getenv("NEO4J_PASSWORD", "password123")
        print(f"Connecting to Neo4j at {uri}...")
        async with AsyncGraphDatabase.driver(uri, auth=(user, password)) as driver:
            async with driver.session() as session:
                await session.run("MATCH (n) DETACH DELETE n;")
        print("Successfully wiped Neo4j nodes and edges.")
    except Exception as e:
        print(f"Warning: Neo4j reset failed: {e}")

async def reset_qdrant():
    try:
        url = os.getenv("QDRANT_URL", "http://localhost:6333")
        print(f"Connecting to Qdrant at {url}...")
        from qdrant_client import AsyncQdrantClient
        client = AsyncQdrantClient(url=url)
        for col in ["cognitive_events", "test_cognitive_events"]:
            exists = await client.collection_exists(col)
            if exists:
                await client.delete_collection(col)
                print(f"Deleted Qdrant collection: {col}")
        await client.close()
        print("Successfully wiped Qdrant.")
    except Exception as e:
        print(f"Warning: Qdrant reset failed: {e}")

async def main():
    print("=== STARTING DCA DATABASES CLEAN RESET ===")
    await asyncio.gather(
        reset_redis(),
        reset_postgres(),
        reset_neo4j(),
        reset_qdrant(),
    )
    print("=== DCA DATABASES RESET COMPLETED ===")


if __name__ == "__main__":
    asyncio.run(main())
