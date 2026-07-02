import pytest
from unittest.mock import AsyncMock, patch
from feedback.consumer import FeedbackConsumer, FeedbackConfig
from feedback.verifier import VerificationResult


@pytest.fixture
def consumer():
    cfg = FeedbackConfig(config_path="feedback_config.yaml")
    c   = FeedbackConsumer.__new__(FeedbackConsumer)
    c.verifier      = AsyncMock()
    c.recorder      = AsyncMock()
    c.pb_updater    = AsyncMock()
    c.graph_updater = AsyncMock()
    c._producer     = AsyncMock()
    c._producer.send_and_wait = AsyncMock()
    c.cfg = cfg
    c.metrics = {"total_processed": 0, "successes": 0, "failures": 0, "partials": 0}
    return c


def make_vr(outcome="success") -> VerificationResult:
    return VerificationResult(
        plan_id="plan_1",
        action="scale_consumer_group",
        outcome=outcome,
        anomalies_before=3,
        anomalies_after=1
    )


@pytest.mark.asyncio
async def test_process_calls_all_substeps(consumer):
    consumer.verifier.verify   = AsyncMock(return_value=make_vr())
    consumer.recorder.record   = AsyncMock()
    consumer.pb_updater.update = AsyncMock()
    consumer.graph_updater.update = AsyncMock()

    event = {
        "event_type": "action_completed",
        "payload": {"plan_id": "plan_1", "action": "scale_consumer_group"}
    }
    await consumer._process(event)

    consumer.verifier.verify.assert_called_once()
    consumer.recorder.record.assert_called_once()
    consumer.pb_updater.update.assert_called_once_with("scale_consumer_group", "success")
    consumer.graph_updater.update.assert_called_once()
    consumer._producer.send_and_wait.assert_called_once()


@pytest.mark.asyncio
async def test_skips_events_without_plan_id(consumer):
    event = {
        "event_type": "action_completed",
        "payload": {"action": "send_alert"}
    }
    await consumer._process(event)
    consumer.verifier.verify.assert_not_called()


@pytest.mark.asyncio
async def test_substep_failure_does_not_crash_consumer(consumer):
    consumer.verifier.verify      = AsyncMock(return_value=make_vr())
    consumer.recorder.record      = AsyncMock(side_effect=Exception("DB timeout"))
    consumer.pb_updater.update    = AsyncMock()
    consumer.graph_updater.update = AsyncMock()

    event = {
        "event_type": "action_completed",
        "payload": {"plan_id": "plan_2", "action": "restart_service"}
    }
    await consumer._process_safe(event)   # must not raise
    consumer.pb_updater.update.assert_called_once()  # other steps still run


@pytest.mark.asyncio
async def test_feedback_signal_emitted_with_correct_outcome(consumer):
    consumer.verifier.verify      = AsyncMock(return_value=make_vr("failure"))
    consumer.recorder.record      = AsyncMock()
    consumer.pb_updater.update    = AsyncMock()
    consumer.graph_updater.update = AsyncMock()

    await consumer._process({
        "event_type": "action_failed",
        "payload": {"plan_id": "plan_3", "action": "restart_service"},
    })
    call_args = consumer._producer.send_and_wait.call_args[0]
    import json
    payload = json.loads(call_args[1])
    assert payload["payload"]["outcome"] == "failure"


def test_load_config_fallback_when_file_not_found():
    cfg = FeedbackConfig(config_path="non_existent_file.yaml", default_delay_s=42)
    c = FeedbackConsumer(cfg)
    assert c._config["defaults"]["verification_delay_s"] == 42

