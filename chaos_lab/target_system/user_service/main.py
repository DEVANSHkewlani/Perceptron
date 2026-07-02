import json, logging, os, random, uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone

import asyncpg, bcrypt, jwt
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Header, Request
from pydantic import BaseModel

DB_DSN      = os.getenv("DATABASE_URL", "postgresql://shopcore:shopcore@postgres:5432/shopcore")
REDIS_URL   = os.getenv("REDIS_URL", "redis://redis:6379")
JWT_SECRET  = os.getenv("JWT_SECRET", "shopcore-dev-secret-change-in-prod")
JWT_EXPIRE  = int(os.getenv("JWT_EXPIRE_MINUTES", "60"))
SESSION_TTL = JWT_EXPIRE * 60
logger      = logging.getLogger("user-service")
logging.basicConfig(format="%(message)s", level=logging.INFO)


class RegisterReq(BaseModel): email: str; password: str; name: str
class LoginReq(BaseModel):    email: str; password: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool  = await asyncpg.create_pool(DB_DSN, min_size=2, max_size=10)
    app.state.redis = await aioredis.from_url(REDIS_URL, decode_responses=True)
    async with app.state.pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                email TEXT UNIQUE NOT NULL, name TEXT NOT NULL,
                password_hash TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
    yield
    await app.state.pool.close()
    await app.state.redis.aclose()

app = FastAPI(title="ShopCore Users", lifespan=lifespan)


def make_jwt(user_id: str) -> str:
    exp = datetime.now(timezone.utc) + timedelta(minutes=JWT_EXPIRE)
    return jwt.encode({"sub": user_id, "exp": exp}, JWT_SECRET, algorithm="HS256")


async def get_current_user(authorization: str, redis) -> dict:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Missing token")
    token = authorization[7:]
    if await redis.sismember("jwt:blacklist", token):
        raise HTTPException(401, "Token revoked")
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Token expired")


@app.post("/users/register", status_code=201)
async def register(body: RegisterReq, request: Request):
    hashed = bcrypt.hashpw(body.password.encode(), bcrypt.gensalt()).decode()
    try:
        async with request.app.state.pool.acquire() as conn:
            row = await conn.fetchrow(
                "INSERT INTO users(email,name,password_hash) VALUES($1,$2,$3) RETURNING id,email,name",
                body.email, body.name, hashed)
        token = make_jwt(str(row["id"]))
        return {"user": dict(row), "token": token}
    except asyncpg.UniqueViolationError:
        raise HTTPException(409, "Email already registered")


@app.post("/users/login")
async def login(body: LoginReq, request: Request):
    # CHAOS: auth failure injection
    if random.random() < float(os.getenv("CHAOS_AUTH_FAIL_RATE", "0")):
        logger.error(json.dumps({"msg": "auth service failure — brute force suspected",
                                   "service": "user-service"}))
        raise HTTPException(500, "Auth service error")

    async with request.app.state.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE email=$1", body.email)
    if not row or not bcrypt.checkpw(body.password.encode(), row["password_hash"].encode()):
        raise HTTPException(401, "Invalid credentials")

    token = make_jwt(str(row["id"]))
    # Store session in Redis
    await request.app.state.redis.setex(f"session:{row['id']}", SESSION_TTL, token)
    return {"token": token, "user_id": str(row["id"]), "name": row["name"]}


@app.get("/users/me")
async def me(request: Request, authorization: str = Header(None)):
    payload = await get_current_user(authorization, request.app.state.redis)
    async with request.app.state.pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id,email,name,created_at FROM users WHERE id=$1",
                                    payload["sub"])
    if not row: raise HTTPException(404, "User not found")
    return dict(row)


@app.post("/users/logout")
async def logout(request: Request, authorization: str = Header(None)):
    if authorization and authorization.startswith("Bearer "):
        token = authorization[7:]
        await request.app.state.redis.sadd("jwt:blacklist", token)
        await request.app.state.redis.expire("jwt:blacklist", SESSION_TTL)
    return {"status": "logged out"}


@app.get("/health")
async def health(): return {"status": "ok", "service": "user-service"}
