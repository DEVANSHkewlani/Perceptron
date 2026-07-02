"""
PromptBuilder — assembles the six-section reasoning prompt.
Reads system prompt from prompts/reasoning_system.txt.
Never hardcodes prompt content in Python code.
"""
from __future__ import annotations
import json
from pathlib import Path
from .schema import DecisionObject


PROMPT_TEMPLATE_PATH = Path(__file__).parent.parent / "prompts" / "reasoning_system.txt"

CONSTRAINTS_TEXT = """
Actions requiring MANDATORY human approval:
- Any action with risk: high
- Any action with reversible: false
- Any action whose blast_radius_count exceeds 5
- Any action targeting a production database directly
- restart_service in a production environment

If in doubt, set requires_human_approval: true and escalate_to_human.
"""


class PromptBuilder:
    def __init__(self, action_registry: dict):
        self._template = PROMPT_TEMPLATE_PATH.read_text()
        self._actions  = action_registry
        self._schema   = json.dumps(
            DecisionObject.model_json_schema(), indent=2
        )

    def build(
        self,
        situation: dict,
        past_experiences_text: str,
        playbooks: list[dict],
        agent_id: str = "agent:reasoning-engine",
        domain: str = "general",
    ) -> str:
        """
        Render the prompt template with all six sections populated.
        Returns the complete prompt string ready for the LLM.
        """
        # Format available actions as compact JSON (not full YAML)
        actions_for_prompt = {
            k: {
                "description": v.get("description"),
                "risk":        v.get("risk"),
                "parameters":  list(v.get("parameters", {}).keys()),
            }
            for k, v in self._actions.items()
        }

        # Filter actions if domain is specialized
        if domain != "general":
            domain_actions = {
                "database": ["restart_connection_pool", "kill_slow_query", "scale_read_replicas", "escalate_to_human"],
                "queue":    ["scale_consumer_group", "purge_dead_letter_queue", "send_alert", "escalate_to_human"],
                "service":  ["restart_service", "scale_service_horizontal", "enable_circuit_breaker", "increase_log_verbosity"],
                "security": ["send_alert", "escalate_to_human"],
            }
            vocab = domain_actions.get(domain, [])
            actions_for_prompt = {k: v for k, v in actions_for_prompt.items() if k in vocab}

        # Append active playbooks to experiences
        exp_text = past_experiences_text
        if playbooks:
            pb_lines = [f"\nACTIVE PLAYBOOKS:"]
            for pb in playbooks[:2]:
                pb_lines.append(
                    f"  [{pb.get('name')}]: {pb.get('description', '')}"
                )
            exp_text += "\n" + "\n".join(pb_lines)

        system_override = f"\nAGENT IDENTITY: You are {agent_id}, specialising in the '{domain}' domain.\n"
        if domain != "general":
            other_domains = [d for d in ["database", "queue", "service", "security"] if d != domain]
            system_override += f"Other specialized agents are handling: {', '.join(other_domains)}.\n"
            system_override += "Strictly restrict your recommendations to your domain's action vocabulary.\n"

        prompt_str = self._template.format(
            situation_summary      = json.dumps(situation, indent=2),
            past_experiences       = exp_text,
            available_actions_json = json.dumps(actions_for_prompt, indent=2),
            constraints_text       = CONSTRAINTS_TEXT + system_override,
            output_schema_json     = self._schema,
        )
        return prompt_str
