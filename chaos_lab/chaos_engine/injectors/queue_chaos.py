import asyncio, logging
from aiokafka.admin import AIOKafkaAdminClient

logger = logging.getLogger("queue-chaos")

class QueueChaos:
    def __init__(self, bootstrap: str = "localhost:9092"):
        self.bootstrap = bootstrap

    async def stop_consumer_group(self, group_id: str, topic: str) -> None:
        """Force the consumer group to stop by deleting its offset (creates lag)."""
        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap)
        await admin.start()
        try:
            # We must specify partitions as a dict or list of TopicPartition
            from aiokafka import TopicPartition
            await admin.delete_consumer_group_offsets(
                group_id=group_id,
                partitions=[TopicPartition(topic, 0)]
            )
            logger.info(f"[queue-chaos] deleted offset for {group_id} on {topic}")
        except Exception as e:
            logger.error(f"[queue-chaos] delete offsets failed: {e}")
        finally:
            await admin.close()

    async def check_consumer_lag(self, group_id: str, topic: str, threshold: int = 100) -> bool:
        admin = AIOKafkaAdminClient(bootstrap_servers=self.bootstrap)
        await admin.start()
        try:
            offsets = await admin.list_consumer_group_offsets(group_id)
            return len(offsets) == 0 or any(v.offset > threshold for v in offsets.values())
        except Exception as e:
            logger.error(f"[queue-chaos] check consumer lag failed: {e}")
            return False
        finally:
            await admin.close()
