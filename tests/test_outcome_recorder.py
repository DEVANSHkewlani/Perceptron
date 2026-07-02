import pytest
from unittest.mock import AsyncMock, patch
from feedback.recorder import OutcomeRecorder
from feedback.verifier import VerificationResult


@pytest.fixture
def recorder():
    return OutcomeRecorder("http://localhost:8090")


@pytest.mark.asyncio
async def test_record_patches_episodic_event(recorder):
    vr = VerificationResult(
        plan_id="plan_abc",
        action="scale_consumer_group",
        outcome="success",
        anomalies_before=2,
        anomalies_after=0,
        entity_health_before="degraded",
        entity_health_after="healthy",
        delay_used_s=60,
        notes="mock notes"
    )
    event = {"payload": {"plan_id": "plan_abc", "action": "scale_consumer_group"}}
    patch_resp = AsyncMock(status_code=200)
    patched_data = {}

    async def mock_patch(url, json=None, **kw):
        patched_data.update(json or {})
        return patch_resp

    with patch("httpx.AsyncClient") as MockClient:
        mc = AsyncMock()
        mc.__aenter__ = AsyncMock(return_value=mc)
        mc.__aexit__  = AsyncMock(return_value=False)
        mc.patch = mock_patch
        MockClient.return_value = mc

        await recorder.record(event, vr)

    assert patched_data["outcome"] == "success"
    assert patched_data["anomalies_before"] == 2
    assert patched_data["anomalies_after"] == 0
    assert patched_data["entity_health_after"] == "healthy"


@pytest.mark.asyncio
async def test_record_missing_plan_id(recorder):
    vr = VerificationResult(plan_id="", action="scale_consumer_group", outcome="success", anomalies_before=1, anomalies_after=0)
    event = {"payload": {"action": "scale_consumer_group"}}
    with patch("httpx.AsyncClient") as MockClient:
        mc = AsyncMock()
        mc.__aenter__ = AsyncMock(return_value=mc)
        mc.patch = AsyncMock()
        MockClient.return_value = mc
        await recorder.record(event, vr)
        mc.patch.assert_not_called()


@pytest.mark.asyncio
async def test_record_patch_fails_non_200(recorder):
    vr = VerificationResult(plan_id="plan_abc", action="scale_consumer_group", outcome="success", anomalies_before=1, anomalies_after=0)
    event = {"payload": {"plan_id": "plan_abc", "action": "scale_consumer_group"}}
    patch_resp = AsyncMock(status_code=500)
    with patch("httpx.AsyncClient") as MockClient:
        mc = AsyncMock()
        mc.__aenter__ = AsyncMock(return_value=mc)
        mc.__aexit__  = AsyncMock(return_value=False)
        mc.patch = AsyncMock(return_value=patch_resp)
        MockClient.return_value = mc
        await recorder.record(event, vr)  # must not raise exception


@pytest.mark.asyncio
async def test_record_http_error_handling(recorder):
    vr = VerificationResult(plan_id="plan_abc", action="scale_consumer_group", outcome="success", anomalies_before=1, anomalies_after=0)
    event = {"payload": {"plan_id": "plan_abc", "action": "scale_consumer_group"}}
    with patch("httpx.AsyncClient") as MockClient:
        mc = AsyncMock()
        mc.__aenter__ = AsyncMock(return_value=mc)
        mc.__aexit__  = AsyncMock(return_value=False)
        import httpx
        mc.patch = AsyncMock(side_effect=httpx.ConnectError("Connection failed"))
        MockClient.return_value = mc
        await recorder.record(event, vr)  # must not raise exception

