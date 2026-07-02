import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
import httpx
from execution.handlers.human import HumanHandoffHandler


@pytest.fixture
def mock_producer():
    p = AsyncMock()
    p.send_and_wait = AsyncMock()
    return p


@pytest.mark.asyncio
async def test_human_handoff_approved(mock_producer):
    handler = HumanHandoffHandler(mock_producer, "http://localhost:8094", poll_interval_s=0.001)
    
    with patch("httpx.AsyncClient") as mock_client:
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 200
        mock_resp.json = MagicMock(return_value={
            "steps": [{"is_approval_gate": True, "status": "succeeded"}]
        })
        mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
        mock_client.return_value.__aexit__  = AsyncMock(return_value=False)
        mock_client.return_value.get = AsyncMock(return_value=mock_resp)
        
        with patch("asyncio.sleep", return_value=None):
            result = await handler.execute("human_handoff", {"plan_id": "plan1"})
            
    assert result["approved"] is True
    assert result["plan_id"] == "plan1"


@pytest.mark.asyncio
async def test_human_handoff_rejected(mock_producer):
    handler = HumanHandoffHandler(mock_producer, "http://localhost:8094", poll_interval_s=0.001)
    
    with patch("httpx.AsyncClient") as mock_client:
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 200
        mock_resp.json = MagicMock(return_value={
            "steps": [{"is_approval_gate": True, "status": "failed"}]
        })
        mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
        mock_client.return_value.__aexit__  = AsyncMock(return_value=False)
        mock_client.return_value.get = AsyncMock(return_value=mock_resp)
        
        with patch("asyncio.sleep", return_value=None):
            with pytest.raises(RuntimeError) as exc:
                await handler.execute("human_handoff", {"plan_id": "plan1"})
                
    assert "Human rejected" in str(exc.value)


@pytest.mark.asyncio
async def test_human_handoff_timeout(mock_producer):
    handler = HumanHandoffHandler(mock_producer, "http://localhost:8094", poll_interval_s=1)
    
    with patch("httpx.AsyncClient") as mock_client:
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 200
        mock_resp.json = MagicMock(return_value={
            "steps": [{"is_approval_gate": True, "status": "waiting_approval"}]
        })
        mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
        mock_client.return_value.__aexit__  = AsyncMock(return_value=False)
        mock_client.return_value.get = AsyncMock(return_value=mock_resp)
        
        with patch("asyncio.sleep", return_value=None):
            with pytest.raises(TimeoutError) as exc:
                await handler.execute("human_handoff", {"plan_id": "plan1", "timeout_s": 3})
                
    assert "Human approval timeout after 3s" in str(exc.value)


@pytest.mark.asyncio
async def test_human_handoff_http_error_resiliency(mock_producer):
    handler = HumanHandoffHandler(mock_producer, "http://localhost:8094", poll_interval_s=0.001)
    
    with patch("httpx.AsyncClient") as mock_client:
        mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
        mock_client.return_value.__aexit__  = AsyncMock(return_value=False)
        
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 200
        mock_resp.json = MagicMock(return_value={
            "steps": [{"is_approval_gate": True, "status": "succeeded"}]
        })
        mock_client.return_value.get = AsyncMock(side_effect=[httpx.HTTPError("API error"), mock_resp])
        
        with patch("asyncio.sleep", return_value=None):
            result = await handler.execute("human_handoff", {"plan_id": "plan1", "timeout_s": 10})
            
    assert result["approved"] is True
    assert mock_client.return_value.get.call_count == 2


@pytest.mark.asyncio
async def test_human_handoff_custom_poll_interval(mock_producer):
    handler = HumanHandoffHandler(mock_producer, "http://localhost:8094", poll_interval_s=5)
    assert handler._poll_interval_s == 5


@pytest.mark.asyncio
async def test_human_handoff_custom_timeout(mock_producer):
    handler = HumanHandoffHandler(mock_producer, "http://localhost:8094", poll_interval_s=2)
    
    with patch("httpx.AsyncClient") as mock_client:
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 200
        mock_resp.json = MagicMock(return_value={"steps": [{"is_approval_gate": True, "status": "waiting_approval"}]})
        mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
        mock_client.return_value.__aexit__  = AsyncMock(return_value=False)
        mock_client.return_value.get = AsyncMock(return_value=mock_resp)
        
        with patch("asyncio.sleep", return_value=None):
            with pytest.raises(TimeoutError) as exc:
                await handler.execute("human_handoff", {"plan_id": "plan1", "timeout_s": 4})
                
    assert "timeout after 4s" in str(exc.value)


@pytest.mark.asyncio
async def test_human_handoff_producer_called(mock_producer):
    handler = HumanHandoffHandler(mock_producer, "http://localhost:8094", poll_interval_s=0.001)
    
    with patch("httpx.AsyncClient") as mock_client:
        mock_resp = MagicMock(spec=httpx.Response)
        mock_resp.status_code = 200
        mock_resp.json = MagicMock(return_value={
            "steps": [{"is_approval_gate": True, "status": "succeeded"}]
        })
        mock_client.return_value.__aenter__ = AsyncMock(return_value=mock_client.return_value)
        mock_client.return_value.__aexit__  = AsyncMock(return_value=False)
        mock_client.return_value.get = AsyncMock(return_value=mock_resp)
        
        with patch("asyncio.sleep", return_value=None):
            result = await handler.run(
                "human_handoff",
                {"plan_id": "plan1"},
                "plan1",
                "s0",
                []
            )
            
    assert result.success is True
    assert mock_producer.send_and_wait.call_count == 2
