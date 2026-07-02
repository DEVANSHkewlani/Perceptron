import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import httpx
from execution.handlers.base import BaseActionHandler
from execution.handlers.api_call import ApiCallHandler
from execution.handlers.message import MessageHandler


@pytest.fixture
def mock_producer():
    p = AsyncMock()
    p.send_and_wait = AsyncMock()
    return p


@pytest.mark.asyncio
async def test_base_handler_emits_started_and_completed_events(mock_producer):
    class SuccessHandler(BaseActionHandler):
        async def execute(self, action, parameters):
            return {"ok": True}

    handler = SuccessHandler(mock_producer)
    result  = await handler.run("send_alert", {}, "plan_1", "s1", ["svc:test"])

    assert result.success is True
    assert mock_producer.send_and_wait.call_count == 2  # started + completed
    calls = [c.args[0] for c in mock_producer.send_and_wait.call_args_list]
    assert all(c == "cognitive.events" for c in calls)


@pytest.mark.asyncio
async def test_base_handler_emits_failed_event_on_exception(mock_producer):
    class FailHandler(BaseActionHandler):
        async def execute(self, action, parameters):
            raise RuntimeError("simulated failure")

    handler = FailHandler(mock_producer)
    result  = await handler.run("bad_action", {}, "plan_1", "s1", [])

    assert result.success is False
    assert "simulated failure" in result.error
    assert mock_producer.send_and_wait.call_count == 2  # started + failed


@pytest.mark.asyncio
async def test_message_handler_sends_slack_alert(mock_producer):
    with patch("httpx.AsyncClient") as mock_client:
        mock_resp = AsyncMock()
        mock_resp.status_code = 200
        mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
        mock_client.return_value.__aexit__  = AsyncMock(return_value=False)
        mock_client.return_value.post = AsyncMock(return_value=mock_resp)

        handler = MessageHandler(mock_producer, "redis://localhost", "http://hooks.slack.com/test")
        result  = await handler.execute("send_alert", {"message": "CPU high", "severity": "high"})

    assert result["sent"] is True
    assert mock_client.return_value.post.called


@pytest.mark.asyncio
async def test_message_handler_deduplication(mock_producer):
    handler = MessageHandler(mock_producer, "redis://localhost", "http://hooks.slack.com/test")
    
    # Mock Redis to simulate duplicate alert
    mock_redis = AsyncMock()
    mock_redis.exists = AsyncMock(return_value=True)
    handler._redis = mock_redis
    
    result = await handler.execute("send_alert", {"message": "Deduplicate me", "severity": "medium"})
    assert result["sent"] is False
    assert result["reason"] == "deduplicated"


@pytest.mark.asyncio
async def test_api_call_handler_success(mock_producer):
    handler = ApiCallHandler(mock_producer)
    
    with patch("httpx.AsyncClient") as mock_client:
        mock_resp = AsyncMock()
        mock_resp.status_code = 200
        mock_resp.text = "Success output"
        mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
        mock_client.return_value.__aexit__  = AsyncMock(return_value=False)
        mock_client.return_value.request = AsyncMock(return_value=mock_resp)
        
        result = await handler.execute(
            "scale_consumer_group",
            {"cluster": "c1", "consumer_group": "g1", "instance_delta": 3}
        )
        
    assert result["status_code"] == 200
    assert "Success output" in result["response"]


@pytest.mark.asyncio
async def test_api_call_handler_unknown_action(mock_producer):
    handler = ApiCallHandler(mock_producer)
    with pytest.raises(ValueError):
        await handler.execute("unknown_rest_action", {})


@pytest.mark.asyncio
async def test_api_call_handler_retries_and_fails(mock_producer):
    handler = ApiCallHandler(mock_producer)
    
    # Mock tenacity wait to keep test fast
    with patch("tenacity.nap.time.sleep", return_value=None):
        with patch("httpx.AsyncClient") as mock_client:
            mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
            mock_client.return_value.__aexit__  = AsyncMock(return_value=False)
            # Make request always fail
            mock_client.return_value.request = AsyncMock(side_effect=httpx.HTTPError("Network fail"))
            
            with pytest.raises(httpx.HTTPError):
                await handler.execute(
                    "restart_connection_pool",
                    {"service_id": "svc1"}
                )
            
            # 3 attempts
            assert mock_client.return_value.request.call_count == 3
