import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from reasoning.llm_client import LLMClient, ParseError
from reasoning.schema import DecisionObject
import json


def valid_json_response() -> str:
    return json.dumps({
        "situation_assessment":    "CPU spike on auth node.",
        "root_cause_hypothesis":   {"hypothesis": "High traffic", "confidence": 0.85, "evidence": []},
        "recommended_action":      "scale_service_horizontal",
        "action_parameters":       {"service_id": "svc:auth", "replica_delta": 2},
        "confidence":              0.85,
        "requires_human_approval": False,
        "alternative_actions":     [],
        "reasoning_trace":         "Identified CPU spike. Scaled horizontally.",
    })


@pytest.mark.asyncio
async def test_parse_clean_json_response():
    client = LLMClient.__new__(LLMClient)
    decision = client._parse_and_validate(valid_json_response())
    assert isinstance(decision, DecisionObject)
    assert decision.recommended_action == "scale_service_horizontal"


@pytest.mark.asyncio
async def test_parse_json_inside_markdown_fence():
    client = LLMClient.__new__(LLMClient)
    fenced = f"```json\n{valid_json_response()}\n```"
    decision = client._parse_and_validate(fenced)
    assert decision.recommended_action == "scale_service_horizontal"


@pytest.mark.asyncio
async def test_parse_raises_on_invalid_json():
    client = LLMClient.__new__(LLMClient)
    with pytest.raises(ParseError):
        client._parse_and_validate("This is not JSON at all.")


@pytest.mark.asyncio
async def test_parse_raises_on_schema_mismatch():
    client = LLMClient.__new__(LLMClient)
    bad_json = json.dumps({"wrong_field": "value"})
    with pytest.raises(ParseError):
        client._parse_and_validate(bad_json)


@pytest.mark.asyncio
async def test_llm_call_mocked():
    """Full call path with mocked client in paid mode."""
    mock_content = MagicMock()
    mock_content.text = valid_json_response()
    mock_response = MagicMock()
    mock_response.content = [mock_content]

    # Force paid mode so it calls Anthropic client
    client = LLMClient()
    client._mode = "paid"

    mock_anth = AsyncMock()
    mock_anth.messages.create = AsyncMock(return_value=mock_response)
    client._get_anthropic_client = MagicMock(return_value=mock_anth)

    decision = await client.call("test prompt")
    assert decision.recommended_action == "scale_service_horizontal"
    client._get_anthropic_client.assert_called_once()


@pytest.mark.asyncio
async def test_llm_call_fallback_mocked():
    """Test fallback to OpenAI client when Anthropic fails in paid mode."""
    # Force paid mode
    client = LLMClient()
    client._mode = "paid"

    # Anthropic client throws an exception
    mock_anth = AsyncMock()
    mock_anth.messages.create = AsyncMock(side_effect=Exception("Anthropic API Down"))
    client._get_anthropic_client = MagicMock(return_value=mock_anth)

    # OpenAI client succeeds
    mock_choice = MagicMock()
    mock_choice.message.content = valid_json_response()
    mock_response = MagicMock()
    mock_response.choices = [mock_choice]
    
    mock_oai = AsyncMock()
    mock_oai.chat.completions.create = AsyncMock(return_value=mock_response)
    client._get_openai_client = MagicMock(return_value=mock_oai)

    decision = await client.call("test prompt")
    assert decision.recommended_action == "scale_service_horizontal"
    client._get_anthropic_client.assert_called_once()
    client._get_openai_client.assert_called_once()
