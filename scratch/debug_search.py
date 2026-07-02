import asyncio
from sentence_transformers import SentenceTransformer
from qdrant_client import AsyncQdrantClient

async def debug():
    client = AsyncQdrantClient(url="http://localhost:6333")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    
    # Let's search for "Testing HTTP endpoints"
    embeddings = model.encode(["Testing HTTP endpoints"])
    vector = embeddings[0].tolist()
    
    res = await client.query_points(
        collection_name="cognitive_events",
        query=vector,
        limit=20
    )
    print("Top 20 results in 'cognitive_events':")
    for i, p in enumerate(res.points):
        payload = p.payload
        event_id = payload.get("event_id") if payload else None
        event_type = payload.get("event_type") if payload else None
        recommended_action = payload.get("payload", {}).get("decision", {}).get("recommended_action") if payload else None
        text_snippet = str(payload)[:150]
        print(f"{i+1}. id={p.id} score={p.score:.4f} event_id={event_id} event_type={event_type} recommended_action={recommended_action} snippet={text_snippet}")
        
    await client.close()

if __name__ == "__main__":
    asyncio.run(debug())
