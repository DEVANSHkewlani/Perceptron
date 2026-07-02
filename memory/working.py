"""
Working Memory — Redis
======================
Stores current active context for the reasoning engine.
Only HIGH/CRITICAL severity events. All with TTL.
"""
from __future__ import annotations
import json
import time
import redis.asyncio as aioredis

TTL_HIGH = 900       # 15 min
TTL_CRITICAL = 1800  # 30 min
TTL_ENTITY = 3600    # 60 min
RECENT_WINDOW = 900  # keep last 15 min in sorted set

class WorkingMemory:
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self._url = redis_url
        self._r: aioredis.Redis | None = None

    async def connect(self):
        self._r = await aioredis.from_url(self._url, decode_responses=True)

    async def disconnect(self):
        if self._r:
            await self._r.aclose()

    async def store(self, event: dict):
        """Store a HIGH/CRITICAL event in working memory."""
        eid = event["event_id"]
        severity = event["severity"]
        ttl = TTL_CRITICAL if severity == "critical" else TTL_HIGH
        ts = event.get("timestamp", "")
        score = time.time()

        pipe = self._r.pipeline()

        # 1. Store full event as hash
        pipe.hset(f"event:{eid}", mapping={
            "event_id": eid,
            "event_type": event["event_type"],
            "severity": severity,
            "source_id": event["source_id"],
            "source_type": event["source_type"],
            "confidence": str(event.get("confidence", 1.0)),
            "payload": json.dumps(event.get("payload", {})),
            "timestamp": ts,
        })
        pipe.expire(f"event:{eid}", ttl)

        # 2. Add to time-ordered sorted set (score = unix timestamp)
        pipe.zadd("events:recent", {eid: score})
        pipe.expire("events:recent", RECENT_WINDOW + 60)

        # 3. Update entity state for each entity_ref
        for ref in event.get("entity_refs", []):
            pipe.hset(f"entity:{ref}", mapping={
                "last_seen": ts,
                "last_event": event["event_type"],
                "severity": severity,
                "source_type": event["source_type"],
            })
            pipe.expire(f"entity:{ref}", TTL_ENTITY)

        await pipe.execute()

    # ── READ METHODS ──────────────────────────────────────────────

    async def get_recent_events(self, limit: int = 50) -> list[dict]:
        """Get the N most recent HIGH/CRITICAL events."""
        now = time.time()
        cutoff = now - RECENT_WINDOW
        ids = await self._r.zrevrangebyscore(
            "events:recent", now, cutoff, start=0, num=limit
        )
        if not ids:
            return []
        pipe = self._r.pipeline()
        for eid in reversed(ids):
            pipe.hgetall(f"event:{eid}")
        results = await pipe.execute()
        
        # Deserialize JSON payload and cast types for downstream ergonomics
        events = []
        for r in results:
            if r:
                if "payload" in r and isinstance(r["payload"], str):
                    try:
                        r["payload"] = json.loads(r["payload"])
                    except Exception:
                        pass
                if "confidence" in r:
                    try:
                        r["confidence"] = float(r["confidence"])
                    except Exception:
                        pass
                events.append(r)
        return events

    async def get_entity_state(self, entity_id: str) -> dict | None:
        """Get the current state of an entity."""
        return await self._r.hgetall(f"entity:{entity_id}") or None

    async def get_active_critical(self) -> list[dict]:
        """Get all currently active CRITICAL events."""
        events = await self.get_recent_events(100)
        return [e for e in events if e.get("severity") == "critical"]

    async def flush_expired(self) -> int:
        """Prune elements from events:recent older than RECENT_WINDOW."""
        cutoff = time.time() - RECENT_WINDOW
        removed = await self._r.zremrangebyscore("events:recent", "-inf", cutoff)
        return removed

    async def get_all_keys(self) -> list[dict]:
        """Fetch all keys with type, value description, and TTL from Redis."""
        if not self._r:
            return []
        keys = await self._r.keys("*")
        results = []
        for k in keys:
            # Skip internal dedup keys if we want a cleaner look, or include them.
            # Let's show everything to be transparent!
            t = await self._r.type(k)
            val_desc = ""
            try:
                if t == "hash":
                    fields = await self._r.hkeys(k)
                    val_desc = f"Fields: {fields}"
                elif t == "string":
                    val_desc = await self._r.get(k)
                elif t == "zset":
                    cnt = await self._r.zcard(k)
                    val_desc = f"ZSET with {cnt} members"
                elif t == "list":
                    cnt = await self._r.llen(k)
                    val_desc = f"LIST with {cnt} elements"
                elif t == "set":
                    cnt = await self._r.scard(k)
                    val_desc = f"SET with {cnt} members"
            except Exception as e:
                val_desc = f"Error: {e}"
            
            ttl = await self._r.ttl(k)
            results.append({
                "key": k,
                "type": t,
                "value": val_desc,
                "ttl": ttl
            })
        # Sort by key name for consistent display
        results.sort(key=lambda x: x["key"])
        return results

