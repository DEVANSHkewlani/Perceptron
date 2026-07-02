import asyncio
from qdrant_client import AsyncQdrantClient

async def clean():
    client = AsyncQdrantClient(url="http://localhost:6333")
    for col in ["cognitive_events", "test_cognitive_events"]:
        exists = await client.collection_exists(col)
        if exists:
            await client.delete_collection(col)
            print(f"Deleted collection: {col}")
    await client.close()

if __name__ == "__main__":
    asyncio.run(clean())
