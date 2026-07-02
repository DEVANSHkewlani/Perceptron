from __future__ import annotations
import asyncio, json, logging, os, random, time, uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

# ── Structured JSON logger ───────────────────────────────────────────────
logging.basicConfig(format="%(message)s", level=logging.INFO)
logger = logging.getLogger("api-gateway")

def log(level: str, msg: str, **extra):
    record = {"ts": datetime.now(timezone.utc).isoformat(),
              "level": level, "service": "api-gateway",
              "msg": msg, **extra}
    print(json.dumps(record), flush=True)

# ── Prometheus ───────────────────────────────────────────────────────────
REQ_COUNT   = Counter('shopcore_gateway_requests_total', 'Requests', ['method', 'path', 'status'])
REQ_LATENCY = Histogram('shopcore_gateway_latency_seconds', 'Latency', ['path'],
                buckets=[.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5])

# ── Upstream map ─────────────────────────────────────────────────────────
SERVICES = {
    "products": os.getenv("PRODUCT_SERVICE_URL", "http://product-service:8011"),
    "orders":   os.getenv("ORDER_SERVICE_URL",   "http://order-service:8012"),
    "users":    os.getenv("USER_SERVICE_URL",    "http://user-service:8013"),
    "cart":     os.getenv("CART_SERVICE_URL",    "http://cart-service:8014"),
}

# ── Chaos helpers (read env at call time — chaos engine sets these) ───────
def chaos_latency(): return float(os.getenv("CHAOS_LATENCY_MS", "0"))
def chaos_error():   return float(os.getenv("CHAOS_ERROR_RATE", "0"))
def circuit_open(): return os.getenv("CHAOS_CIRCUIT_OPEN", "false") == "true"

def clean_path(path: str) -> str:
    if path.endswith("/") and len(path) > 1:
        return path.rstrip("/")
    return path

async def proxy(req: Request, upstream: str, path: str, client: httpx.AsyncClient):
    tid = str(uuid.uuid4())[:8]

    latency = chaos_latency()
    if latency > 0:
        await asyncio.sleep(latency / 1000)
        log("WARNING", "artificial latency applied", trace_id=tid,
            latency_ms=latency, path=path)

    if random.random() < chaos_error():
        log("ERROR", "chaos error injected", trace_id=tid, path=path)
        raise HTTPException(500, "Internal server error (chaos)")

    if circuit_open():
        log("ERROR", "circuit breaker open — upstream blocked", trace_id=tid)
        raise HTTPException(503, "Service unavailable")

    t0 = time.monotonic()
    try:
        body = await req.body()
        resp = await client.request(
            method=req.method, url=f"{upstream}{path}",
            headers={k: v for k, v in req.headers.items() if k.lower() != "host"},
            content=body, params=dict(req.query_params),
        )
        elapsed = (time.monotonic() - t0) * 1000
        log("INFO", "proxied", trace_id=tid, path=path,
            status=resp.status_code, elapsed_ms=round(elapsed, 1))

        # Copy headers except hop-by-hop headers
        exclude_headers = {
            "content-encoding", "content-length", "transfer-encoding",
            "connection", "keep-alive", "proxy-authenticate",
            "proxy-authorization", "te", "trailers", "upgrade"
        }
        headers = {}
        for k, v in resp.headers.items():
            if k.lower() not in exclude_headers:
                if k.lower() == "location":
                    # Rewrite internal docker hostname redirects to relative paths for browser safety
                    for s_url in SERVICES.values():
                        if v.startswith(s_url):
                            v = v.replace(s_url, "", 1)
                            break
                headers[k] = v

        # Support both lists and dicts
        try:
            res_json = resp.json()
            return JSONResponse(res_json, status_code=resp.status_code, headers=headers)
        except Exception:
            return Response(resp.content, status_code=resp.status_code, media_type=resp.headers.get("content-type"), headers=headers)
    except httpx.ConnectError:
        log("ERROR", "upstream connection refused", trace_id=tid, path=path)
        raise HTTPException(503, "Upstream unreachable")
    except httpx.TimeoutException:
        log("ERROR", "upstream timeout", trace_id=tid, path=path)
        raise HTTPException(504, "Gateway timeout")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.client = httpx.AsyncClient(timeout=10.0)
    log("INFO", "api-gateway started")
    yield
    await app.state.client.aclose()

app = FastAPI(title="ShopCore Gateway", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ── ROUTES ───────────────────────────────────────────────────────────────
@app.api_route("/products", methods=["GET","POST","PUT","DELETE"])
@app.api_route("/products/{path:path}", methods=["GET","POST","PUT","DELETE"])
async def products(req: Request, path: str = ""):
    return await proxy(req, SERVICES["products"], clean_path(req.url.path), req.app.state.client)

@app.api_route("/orders", methods=["GET","POST"])
@app.api_route("/orders/{path:path}", methods=["GET","POST"])
async def orders(req: Request, path: str = ""):
    return await proxy(req, SERVICES["orders"], clean_path(req.url.path), req.app.state.client)

@app.api_route("/users", methods=["GET","POST","PUT"])
@app.api_route("/users/{path:path}", methods=["GET","POST","PUT"])
async def users(req: Request, path: str = ""):
    return await proxy(req, SERVICES["users"], clean_path(req.url.path), req.app.state.client)

@app.api_route("/cart", methods=["GET","POST","DELETE"])
@app.api_route("/cart/{path:path}", methods=["GET","POST","DELETE"])
async def cart(req: Request, path: str = ""):
    return await proxy(req, SERVICES["cart"], clean_path(req.url.path), req.app.state.client)

@app.get("/health")
async def health():
    tid = str(uuid.uuid4())[:8]
    latency = chaos_latency()
    if latency > 0:
        await asyncio.sleep(latency / 1000)
        log("WARNING", "artificial latency applied", trace_id=tid,
            latency_ms=latency, path="/health")
    if random.random() < chaos_error():
        log("ERROR", "chaos error injected", trace_id=tid, path="/health")
        REQ_COUNT.labels(method="GET", path="/health", status="500").inc()
        raise HTTPException(500, "Internal server error (chaos)")
    REQ_COUNT.labels(method="GET", path="/health", status="200").inc()
    return {"status": "ok", "service": "api-gateway"}

@app.get("/metrics")
async def metrics(): return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
