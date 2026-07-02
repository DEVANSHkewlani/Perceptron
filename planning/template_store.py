"""
PlanTemplateStore — loads and parses plan templates from a YAML file.
"""
from __future__ import annotations
import os
import yaml


class PlanTemplateStore:
    def __init__(self, yaml_path: str = "plan_templates.yaml"):
        self.yaml_path = yaml_path
        self._templates = {}
        self.load()

    def load(self) -> None:
        """Load templates from the configured YAML file."""
        if not os.path.exists(self.yaml_path):
            print(f"[PlanTemplateStore] Warning: Template file not found at {self.yaml_path}")
            return
        try:
            with open(self.yaml_path, "r") as f:
                data = yaml.safe_load(f) or {}
                self._templates = data.get("templates", {})
        except Exception as e:
            print(f"[PlanTemplateStore] Error: Failed to load templates from {self.yaml_path}: {e}")
            self._templates = {}

    def get(self, action: str) -> dict | None:
        """Retrieve a template for a given action name."""
        return self._templates.get(action)
