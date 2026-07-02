from __future__ import annotations

class ActionRegistry:
    def __init__(self, actions: dict):
        self._actions = actions

    def is_valid(self, action_name: str) -> bool:
        return action_name in self._actions

    def get_meta(self, action_name: str) -> dict:
        return self._actions.get(action_name, {})

    def requires_approval(self, action_name: str) -> bool:
        meta = self.get_meta(action_name)
        return meta.get("requires_approval", False) or meta.get("risk") == "high"

    def all_names(self) -> list[str]:
        return list(self._actions.keys())
