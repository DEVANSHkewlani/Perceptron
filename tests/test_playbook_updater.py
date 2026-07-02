import pytest
from unittest.mock import AsyncMock, patch
from feedback.playbook_updater import PlaybookUpdater


@pytest.fixture
def updater():
    return PlaybookUpdater("http://localhost:8090")


@pytest.mark.asyncio
async def test_success_increments_success_count(updater):
    playbooks = [{"id": "pb_1", "success_count": 3, "failure_count": 1}]
    get_resp  = AsyncMock(status_code=200)
    get_resp.json = lambda: playbooks
    patch_resp = AsyncMock(status_code=200)
    patched   = {}

    async def mock_patch(url, json=None, **kw):
        patched.update(json or {})
        return patch_resp

    with patch("httpx.AsyncClient") as MockClient:
        mc = AsyncMock()
        mc.__aenter__ = AsyncMock(return_value=mc)
        mc.__aexit__  = AsyncMock(return_value=False)
        mc.get   = AsyncMock(return_value=get_resp)
        mc.patch = mock_patch
        MockClient.return_value = mc
        await updater.update("scale_consumer_group", "success")

    assert patched["success_count"] == 4
    assert patched["failure_count"] == 1
    assert abs(patched["success_rate"] - 4/5) < 0.001


@pytest.mark.asyncio
async def test_failure_increments_failure_count(updater):
    playbooks = [{"id": "pb_2", "success_count": 5, "failure_count": 0}]
    get_resp  = AsyncMock(status_code=200)
    get_resp.json = lambda: playbooks
    patched   = {}

    async def mock_patch(url, json=None, **kw):
        patched.update(json or {})
        return AsyncMock(status_code=200)

    with patch("httpx.AsyncClient") as MockClient:
        mc = AsyncMock()
        mc.__aenter__ = AsyncMock(return_value=mc)
        mc.__aexit__  = AsyncMock(return_value=False)
        mc.get   = AsyncMock(return_value=get_resp)
        mc.patch = mock_patch
        MockClient.return_value = mc
        await updater.update("restart_service", "failure")

    assert patched["failure_count"] == 1
    assert patched["success_count"] == 5
    assert abs(patched["success_rate"] - 5/6) < 0.001


@pytest.mark.asyncio
async def test_no_playbooks_does_not_crash(updater):
    get_resp = AsyncMock(status_code=200)
    get_resp.json = lambda: []
    with patch("httpx.AsyncClient") as MockClient:
        mc = AsyncMock()
        mc.__aenter__ = AsyncMock(return_value=mc)
        mc.__aexit__  = AsyncMock(return_value=False)
        mc.get = AsyncMock(return_value=get_resp)
        MockClient.return_value = mc
        await updater.update("unknown_action", "success")  # must not raise
