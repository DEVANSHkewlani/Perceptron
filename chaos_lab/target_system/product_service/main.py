import asyncio, json, logging, os, random, time
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

DB_DSN   = os.getenv("DATABASE_URL", "postgresql://shopcore:shopcore@postgres:5432/shopcore")
POOL_MAX = 10

QUERY_LATENCY = Histogram('product_db_query_seconds', 'DB query latency', ['op'])
POOL_USED     = Gauge('product_db_pool_used', 'Used DB connections')
ERRORS        = Counter('product_errors_total', 'Errors', ['type'])
logger        = logging.getLogger("product-service")


class Product(BaseModel):
    name: str; price: float; stock: int; category: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(DB_DSN, min_size=2, max_size=POOL_MAX)
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS products (
                id SERIAL PRIMARY KEY, name TEXT NOT NULL,
                price NUMERIC(10,2) NOT NULL, stock INT DEFAULT 0,
                category TEXT, created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        count = await conn.fetchval("SELECT COUNT(*) FROM products")
        if count == 0:
            await conn.executemany(
                "INSERT INTO products(name,price,stock,category) VALUES($1,$2,$3,$4)",
                [("Laptop Pro 15", 1299.99, 50, "electronics"),
                 ("Wireless Mouse", 29.99,  200, "peripherals"),
                 ("USB-C Hub",      49.99,  150, "peripherals"),
                 ("4K Monitor",     599.99, 30,  "electronics"),
                 ("Mech Keyboard",  149.99, 75,  "peripherals"),
                 ("Webcam HD",       79.99,  120, "peripherals"),
                 ("SSD 1TB",         89.99,  80,  "storage"),
                 ("Desk Lamp LED",   39.99,  200, "accessories")]
            )
    yield
    await app.state.pool.close()

app = FastAPI(title="ShopCore Products", lifespan=lifespan)


async def get_conn(pool) -> asyncpg.Connection:
    POOL_USED.set(pool.get_size() - pool.get_idle_size())
    # CHAOS: pool exhaustion
    if os.getenv("CHAOS_POOL_EXHAUST") == "true":
        logger.error("connection pool exhausted database=shopcore service=product-service")
        ERRORS.labels(type="pool_exhausted").inc()
        raise HTTPException(503, "DB pool exhausted")
    return await pool.acquire()


@app.get("/products")
async def list_products(request: Request, category: str = None):
    conn = await get_conn(request.app.state.pool)
    t0   = time.monotonic()
    try:
        # CHAOS: slow query
        slow_ms = float(os.getenv("CHAOS_SLOW_QUERY_MS", "0"))
        if slow_ms > 0:
            logger.warning(f"slow query injected slow_query=true elapsed_ms={slow_ms} database=shopcore")
            await conn.execute(f"SELECT pg_sleep({slow_ms/1000})")

        # CHAOS: random DB errors
        err_rate = float(os.getenv("CHAOS_DB_ERROR_RATE", "0"))
        if random.random() < err_rate:
            raise asyncpg.PostgresError("injected DB error")

        q = "SELECT * FROM products"
        params = []
        if category:
            q += " WHERE category=$1"; params = [category]
        rows = await conn.fetch(q + " ORDER BY id", *params)
        QUERY_LATENCY.labels(op="list").observe(time.monotonic() - t0)
        return [dict(r) for r in rows]
    finally:
        await request.app.state.pool.release(conn)


@app.get("/products/{pid}")
async def get_product(pid: int, request: Request):
    conn = await get_conn(request.app.state.pool)
    try:
        row = await conn.fetchrow("SELECT * FROM products WHERE id=$1", pid)
        if not row: raise HTTPException(404, "Not found")
        return dict(row)
    finally:
        await request.app.state.pool.release(conn)


@app.post("/products", status_code=201)
async def create_product(product: Product, request: Request):
    conn = await get_conn(request.app.state.pool)
    try:
        row = await conn.fetchrow(
            "INSERT INTO products(name,price,stock,category) VALUES($1,$2,$3,$4) RETURNING *",
            product.name, product.price, product.stock, product.category)
        return dict(row)
    finally:
        await request.app.state.pool.release(conn)


@app.get("/health")
async def health(): return {"status": "ok", "service": "product-service"}

@app.get("/metrics")
async def metrics(): return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
