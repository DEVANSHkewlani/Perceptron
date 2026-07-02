import asyncio
import os
from sentence_transformers import SentenceTransformer
from qdrant_client import AsyncQdrantClient

async def debug():
    client = AsyncQdrantClient(url="http://localhost:6333")
    collections = await client.get_collections()
    print("Collections:", collections)
    
    # Check if cognitive_events exists
    exists = await client.collection_exists("cognitive_events")
    if exists:
        info = await client.get_collection("cognitive_events")
        print("Collection info:", info)
        
        # Retrieve some points
        points = await client.scroll(
            collection_name="cognitive_events",
            limit=10,
            with_payload=True,
            with_vectors=False
        )
        print("Scrolled points:", points)
        
        # Test embedding and search
        model = SentenceTransformer("all-MiniLM-L6-v2")
        embeddings = model.encode(["Testing HTTP endpoints"])
        vector = embeddings[0].tolist()
        
        res = await client.query_points(
            collection_name="cognitive_events",
            query=vector,
            limit=5
        )
        print("Query results:")
        for p in res.points:
            print("- id:", p.id, "score:", p.score, "payload:", p.payload)
    else:
        print("Collection 'cognitive_events' does not exist")
    await client.close()

if __name__ == "__main__":
    asyncio.run(debug())
