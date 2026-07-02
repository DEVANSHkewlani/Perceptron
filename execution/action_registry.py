"""
ActionRegistry — maps action names to handler instances.
"""
from __future__ import annotations
from aiokafka import AIOKafkaProducer
from .handlers.api_call import ApiCallHandler
from .handlers.message import MessageHandler
from .handlers.human import HumanHandoffHandler
from .handlers.docker_handler import DockerHandler
from .handlers.base import BaseActionHandler


class ActionRegistry:
    def __init__(self, producer: AIOKafkaProducer | None, redis_url: str,
                 planning_url: str, slack_webhook: str = ""):
        api     = ApiCallHandler(producer)
        msg     = MessageHandler(producer, redis_url, slack_webhook)
        human   = HumanHandoffHandler(producer, planning_url)
        docker  = DockerHandler(producer)
        # Map action keys from actions.yaml to their execution handlers
        self._map: dict[str, BaseActionHandler] = {
            "scale_consumer_group":     docker,
            "restart_connection_pool":   docker,
            "kill_slow_query":           docker,
            "scale_read_replicas":        api,
            "scale_service_horizontal":   api,
            "enable_circuit_breaker":     api,
            "restart_service":            docker,
            "send_alert":                 msg,
            "escalate_to_human":          human,
            "human_handoff":              human,
            "increase_log_verbosity":     api,
            "monitor_and_wait":           msg,
        }

    def get(self, action: str) -> BaseActionHandler | None:
        return self._map.get(action)

