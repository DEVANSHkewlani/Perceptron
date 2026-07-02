import asyncio
import uuid
from qdrant_client import AsyncQdrantClient

def _get_qdrant_id(event_id: str) -> str:
    try:
        return str(uuid.UUID(event_id))
    except ValueError:
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, event_id))

async def debug():
    client = AsyncQdrantClient(url="http://localhost:6333")
    event_id = "evt_0dfb4a726d37"
    qdrant_id = _get_qdrant_id(event_id)
    print("Event ID:", event_id, "Qdrant ID:", qdrant_id)
    
    points = await client.retrieve(
        collection_name="cognitive_events",
        ids=[qdrant_id],
        with_payload=True,
        with_vectors=True
    )
    print("Point details:", points)
    await client.close()

if __name__ == "__main__":
    asyncio.run(debug())
