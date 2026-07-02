import asyncio, json, logging, os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer

KAFKA_BS = os.getenv("KAFKA_BOOTSTRAP", "redpanda:9092")
logger   = logging.getLogger("notification-service")
logging.basicConfig(format="%(message)s", level=logging.INFO)


async def consume_loop():
    consumer = AIOKafkaConsumer(
        "order-events",
        bootstrap_servers=KAFKA_BS,
        group_id="notification-group",    # queue_adapter watches this group
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode()),
    )
    await consumer.start()
    logger.info(json.dumps({"msg": "notification consumer started"}))
    try:
        async for msg in consumer:
            delay = float(os.getenv("CHAOS_NOTIFY_DELAY_MS", "0"))
            if delay > 0:
                logger.warning(json.dumps({"msg": "consumer lag building — slow processing",
                                             "delay_ms": delay}))
                await asyncio.sleep(delay / 1000)
            logger.info(json.dumps({
                "msg": "notification dispatched",
                "order_id": msg.value.get("order_id"),
                "user_id":  msg.value.get("user_id"),
            }))
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(consume_loop())
    yield
    task.cancel()

app = FastAPI(title="ShopCore Notifications", lifespan=lifespan)

@app.get("/health")
async def health(): return {"status": "ok", "service": "notification-service"}
