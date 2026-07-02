import json
import pytest
from reasoning.prompt_builder import PromptBuilder


@pytest.fixture
def mock_actions():
    return {
        "scale_consumer_group": {
            "description": "Scale consumer group",
            "risk": "low",
            "parameters": {"queue_id": {}, "consumer_group": {}, "instance_delta": {}}
        },
        "escalate_to_human": {
            "description": "Escalate to human",
            "risk": "low",
            "parameters": {"reason": {}, "urgency": {}}
        }
    }


def test_prompt_builder_initialization(mock_actions):
    pb = PromptBuilder(mock_actions)
    assert pb._template is not None
    assert "You are the Reasoning Engine" in pb._template


def test_prompt_builder_builds_full_prompt(mock_actions):
    pb = PromptBuilder(mock_actions)
    situation = {"situation_summary": "1 critical anomaly", "ranked_anomalies": []}
    past_exp = "Experience 1: success"
    
    prompt = pb.build(situation, past_exp, [])
    
    assert "1 critical anomaly" in prompt
    assert "Experience 1: success" in prompt
    assert "scale_consumer_group" in prompt
    assert "escalate_to_human" in prompt
    assert "requires_human_approval" in prompt  # check schema is injected


def test_prompt_builder_includes_playbooks(mock_actions):
    pb = PromptBuilder(mock_actions)
    situation = {"situation_summary": "1 critical anomaly", "ranked_anomalies": []}
    past_exp = "Experience 1: success"
    playbooks = [{"name": "Restart DB Runbook", "description": "Steps to restart DB connection pool"}]
    
    prompt = pb.build(situation, past_exp, playbooks)
    
    assert "ACTIVE PLAYBOOKS:" in prompt
    assert "Restart DB Runbook" in prompt
    assert "Steps to restart DB connection pool" in prompt
