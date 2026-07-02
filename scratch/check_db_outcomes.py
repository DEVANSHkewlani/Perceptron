import asyncio
import httpx
import json

async def main():
    async with httpx.AsyncClient(timeout=10.0) as client:
        # Query Episodic Memory (TimescaleDB) by type
        print("--- Querying Episodic Memory (TimescaleDB) via Memory API ---")
        try:
            r = await client.get("http://localhost:8090/memory/episodic/type/reasoning_completed?hours=24")
            if r.status_code == 200:
                events = r.json()
                print(f"Found {len(events)} reasoning_completed events in TimescaleDB:")
                for e in events:
                    payload = e.get("payload")
                    if isinstance(payload, str):
                        try:
                            payload = json.loads(payload)
                        except Exception:
                            pass
                    plan_id = payload.get("plan_id") if isinstance(payload, dict) else None
                    decision_id = payload.get("decision", {}).get("decision_id") if isinstance(payload, dict) else None
                    print(f"  Event ID: {e.get('event_id')}")
                    print(f"    Timestamp: {e.get('timestamp')}")
                    print(f"    Outcome: {e.get('outcome')}")
                    print(f"    Plan ID: {plan_id}")
                    print(f"    Decision ID: {decision_id}")
            else:
                print(f"Failed to fetch: {r.status_code} {r.text}")
        except Exception as e:
            import traceback
            print("Error:")
            traceback.print_exc()

        # Query Qdrant via vector search
        print("\n--- Querying Vector Memory (Qdrant) via search endpoint ---")
        try:
            r = await client.get("http://localhost:8090/memory/episodic/search?q=reasoning_completed&limit=10")
            if r.status_code == 200:
                results = r.json().get("results", [])
                print(f"Found {len(results)} vector search results:")
                for res in results:
                    print(f"  Result: {json.dumps(res, indent=2)}")
            else:
                print(f"Failed to fetch vector: {r.status_code} {r.text}")
        except Exception as e:
            import traceback
            print("Error search:")
            traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
