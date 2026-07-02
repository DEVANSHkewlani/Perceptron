import asyncio
import asyncpg
import json

async def main():
    dsn = "postgresql://postgres:postgres@localhost:5432/cognitive"
    print(f"Connecting directly to TimescaleDB at {dsn}...")
    try:
        conn = await asyncpg.connect(dsn)
        try:
            # Query recent reasoning_completed events that have a plan_id or outcome
            rows = await conn.fetch("""
                SELECT event_id, timestamp, outcome, verified_at, payload
                FROM cognitive_events
                WHERE event_type = 'reasoning_completed'
                  AND (payload->>'plan_id' IS NOT NULL OR outcome IS NOT NULL)
                ORDER BY timestamp DESC
                LIMIT 10
            """)
            print(f"Found {len(rows)} reasoning_completed events:")
            for row in rows:
                payload = row['payload']
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except:
                        pass
                plan_id = payload.get("plan_id") if isinstance(payload, dict) else None
                decision_id = payload.get("decision", {}).get("decision_id") if isinstance(payload, dict) else None
                print(f"Event ID: {row['event_id']}")
                print(f"  Timestamp: {row['timestamp']}")
                print(f"  Outcome: {row['outcome']}")
                print(f"  Verified At: {row['verified_at']}")
                print(f"  Plan ID: {plan_id}")
                print(f"  Decision ID: {decision_id}")
        finally:
            await conn.close()
    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
