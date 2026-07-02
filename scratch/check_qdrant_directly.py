import asyncio
import uuid
import json
from qdrant_client import AsyncQdrantClient

async def main():
    qdrant_url = "http://localhost:6333"
    collection_name = "cognitive_events"
    event_id = "evt_5ce8815dfba2"
    
    print(f"Connecting directly to Qdrant at {qdrant_url}...")
    client = AsyncQdrantClient(url=qdrant_url, timeout=60.0)
    try:
        # Calculate Qdrant UUID
        try:
            qdrant_id = str(uuid.UUID(event_id))
        except ValueError:
            qdrant_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, event_id))
            
        print(f"Retrieving point ID {qdrant_id} for event {event_id}...")
        points = await client.retrieve(
            collection_name=collection_name,
            ids=[qdrant_id]
        )
        
        if points:
            print("Found point in Qdrant!")
            point = points[0]
            print(f"Point ID: {point.id}")
            payload = point.payload
            print(f"Payload keys: {list(payload.keys()) if payload else None}")
            print(f"Event ID in payload: {payload.get('event_id')}")
            print(f"Outcome in payload: {payload.get('outcome')}")
            print(f"Verified At in payload: {payload.get('verified_at')}")
            
            # Print nested payload outcome
            nested_payload = payload.get("payload", {})
            if isinstance(nested_payload, str):
                try:
                    nested_payload = json.loads(nested_payload)
                except Exception:
                    pass
            if isinstance(nested_payload, dict):
                print(f"Outcome inside payload.payload: {nested_payload.get('outcome')}")
                print(f"Plan ID inside payload.payload: {nested_payload.get('plan_id')}")
            else:
                print(f"payload.payload is not dict: {type(nested_payload)}")
        else:
            print("Point NOT found in Qdrant.")
            
    except Exception as e:
        import traceback
        traceback.print_exc()
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
