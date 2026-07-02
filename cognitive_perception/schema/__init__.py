"""Schema layer — the unified data contract."""
from .event import CognitiveEvent, PerceptionFailure, SourceType, Severity
from .event_types import ALL_EVENT_TYPES, is_known_event_type, get_event_description
from .sources import SourceConfig, load_sources, write_example_config

__all__ = [
    "CognitiveEvent", "PerceptionFailure", "SourceType", "Severity",
    "ALL_EVENT_TYPES", "is_known_event_type", "get_event_description",
    "SourceConfig", "load_sources", "write_example_config",
]
