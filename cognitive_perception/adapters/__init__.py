"""Adapters — connect to signal sources and feed normalizers."""
from .sources import SourceConfig, load_sources, write_example_config

__all__ = [
    "SourceConfig", "load_sources", "write_example_config",
]
