import json, os, logging
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
TTL       = int(os.getenv("CART_TTL_SECONDS", "3600"))
logger    = logging.getLogger("cart-service")
logging.basicConfig(format="%(message)s", level=logging.INFO)


class CartItem(BaseModel): product_id: int; quantity: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    yield
    await app.state.redis.aclose()

app = FastAPI(title="ShopCore Cart", lifespan=lifespan)


@app.get("/cart/{user_id}")
async def get_cart(user_id: str, request: Request):
    r = request.app.state.redis
    # CHAOS: simulate cache miss (delete key before reading)
    if os.getenv("CHAOS_CACHE_MISS") == "true":
        logger.warning(json.dumps({"msg": "cache miss — key evicted unexpectedly",
                                    "key": f"cart:{user_id}", "service": "cart-service"}))
        await r.delete(f"cart:{user_id}")

    items = await r.hgetall(f"cart:{user_id}")
    return {"user_id": user_id, "items": {k: int(v) for k, v in items.items()}}


@app.post("/cart/{user_id}/items")
async def add_item(user_id: str, item: CartItem, request: Request):
    r = request.app.state.redis
    key = f"cart:{user_id}"
    await r.hset(key, str(item.product_id), item.quantity)
    await r.expire(key, TTL)
    return {"status": "added", "product_id": item.product_id, "quantity": item.quantity}


@app.delete("/cart/{user_id}/items/{product_id}")
async def remove_item(user_id: str, product_id: int, request: Request):
    await request.app.state.redis.hdel(f"cart:{user_id}", str(product_id))
    return {"status": "removed"}


@app.delete("/cart/{user_id}")
async def clear_cart(user_id: str, request: Request):
    await request.app.state.redis.delete(f"cart:{user_id}")
    return {"status": "cleared"}


@app.get("/health")
async def health(): return {"status": "ok", "service": "cart-service"}
