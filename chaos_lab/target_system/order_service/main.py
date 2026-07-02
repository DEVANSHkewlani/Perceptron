import asyncio, json, logging, os, random, time, uuid
from contextlib import asynccontextmanager

import asyncpg
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

DB_DSN   = os.getenv("DATABASE_URL", "postgresql://shopcore:shopcore@postgres:5432/shopcore")
KAFKA_BS = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
logger   = logging.getLogger("order-service")
logging.basicConfig(format="%(message)s", level=logging.INFO)


class OrderRequest(BaseModel):
    user_id: str; product_ids: list[int]; total_amount: float


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool     = await asyncpg.create_pool(DB_DSN, min_size=2, max_size=10)
    app.state.producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BS)
    await app.state.producer.start()
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id TEXT NOT NULL, product_ids INTEGER[],
                total_amount NUMERIC(10,2),
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
    yield
    await app.state.producer.stop()
    await app.state.pool.close()

app = FastAPI(title="ShopCore Orders", lifespan=lifespan)


@app.post("/orders", status_code=201)
async def create_order(order: OrderRequest, request: Request):
    order_id = str(uuid.uuid4())

    # CHAOS: processing delay
    delay = float(os.getenv("CHAOS_ORDER_DELAY_MS", "0"))
    if delay > 0:
        logger.warning(json.dumps({"msg": "order processing delayed",
                                     "slow_query": True, "elapsed_ms": delay}))
        await asyncio.sleep(delay / 1000)

    # CHAOS: random failure
    fail = float(os.getenv("CHAOS_ORDER_FAIL_RATE", "0"))
    if random.random() < fail:
        logger.error(json.dumps({"msg": "internal server error in order processing",
                                   "order_id": order_id}))
        raise HTTPException(500, "Order processing failed")

    async with request.app.state.pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO orders(id,user_id,product_ids,total_amount) VALUES($1,$2,$3,$4)",
            order_id, order.user_id, order.product_ids, order.total_amount)

    # CHAOS: block Kafka publish (causes consumer lag to build up)
    if os.getenv("CHAOS_KAFKA_BLOCK") != "true":
        evt = {"order_id": order_id, "user_id": order.user_id,
               "total": order.total_amount, "ts": time.time()}
        await request.app.state.producer.send_and_wait(
            "order-events", json.dumps(evt).encode())
    else:
        logger.warning(json.dumps({"msg": "kafka blocked — downstream dependency call failed",
                                    "service": "order-service", "order_id": order_id}))

    return {"order_id": order_id, "status": "created"}


@app.get("/orders")
async def list_orders(user_id: str, request: Request):
    async with request.app.state.pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM orders WHERE user_id=$1 ORDER BY created_at DESC", user_id)
    return [dict(r) for r in rows]


@app.get("/health")
async def health(): return {"status": "ok", "service": "order-service"}
