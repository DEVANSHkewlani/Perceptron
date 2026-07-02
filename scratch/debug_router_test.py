import asyncio
import time
from memory.router import MemoryRouter

async def main():
    router = MemoryRouter()
    await router.start()
    
    event_id = f"evt_route_test_001_{int(time.time() * 1000)}"
    fresh_event = {
        "event_id":    event_id,
        "event_type":  "connection_pool_exhausted",
        "severity":    "critical",
        "source_id":   "svc:auth-service",
        "source_type": "log",
        "confidence":  0.95,
        "timestamp":   time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ingested_at": "2024-01-15T14:23:12Z",
        "payload":     {"pool_size": 20, "waiting": 47},
        "entity_refs": ["svc:auth-service", "db:postgres-primary"],
        "tags":        ["database", "critical"],
    }
    
    print("Dumping all current events in working memory before routing:")
    pre_events = await router.working.get_recent_events(100)
    for e in pre_events:
        print("  - ", e.get("event_id"))
        
    print(f"Routing fresh event: {event_id}")
    await router._route(fresh_event)
    
    # Custom get_recent_events function using zrevrangebyscore
    async def get_recent_events_fixed(wm, limit=50):
        now = time.time()
        cutoff = now - 900
        ids = await wm._r.zrevrangebyscore(
            "events:recent", now, cutoff, start=0, num=limit
        )
        if not ids:
            return []
        pipe = wm._r.pipeline()
        for eid in reversed(ids):
            pipe.hgetall(f"event:{eid}")
        results = await pipe.execute()
        events = []
        for r in results:
            if r:
                if "payload" in r and isinstance(r["payload"], str):
                    try:
                        r["payload"] = json.loads(r["payload"])
                    except: pass
                if "confidence" in r:
                    try:
                        r["confidence"] = float(r["confidence"])
                    except: pass
                events.append(r)
        return events

    print("Checking if event is in working memory (fixed method):")
    post_events = await get_recent_events_fixed(router.working, 100)
    found = any(e["event_id"] == event_id for e in post_events)
    print(f"Found: {found}")
    
    # Print the raw zset contents
    zset_contents = await router.working._r.zrange("events:recent", 0, -1, withscores=True)
    print(f"events:recent members count: {len(zset_contents)}")
    for member, score in zset_contents:
        if "evt_route_test_001" in member:
            print(f"  member={member}, score={score}, current_time={time.time()}")

    
    print("Dumping all keys in Redis:")
    keys = await router.working.get_all_keys()
    for k in keys:
        print(f"  {k['key']}: {k['type']} (ttl={k['ttl']})")
        
    await router.stop()

if __name__ == "__main__":
    asyncio.run(main())
