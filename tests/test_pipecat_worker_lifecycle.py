from __future__ import annotations

import json
from functools import partial
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.task import PipelineParams, PipelineTask

from src.pipecat_runtime.artifacts import CallArtifactWriter
from src.pipecat_runtime.call_worker import PipecatCallWorker
from src.pipecat_runtime.pipeline_factory import GatewayPipelineArtifacts
from src.pipecat_runtime.processors.activity import SessionActivityProcessor
from src.pipecat_runtime.processors.egress import GatewayEgressProcessor
from src.pipecat_runtime.processors.ingress import GatewayIngressProcessor
from src.pipecat_runtime.processors.turn_timing import TurnTimingProcessor
from src.runtime.calls.session.runtime import SessionRuntime, SessionRuntimeState


class PipecatCallWorkerLifecycleTest(unittest.TestCase):
    def setUp(self) -> None:
        self._artifact_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._artifact_dir.cleanup)

    class _PassThroughProcessor(FrameProcessor):
        def __init__(self, name: str) -> None:
            super().__init__(name=name, enable_direct_mode=True)

        async def process_frame(self, frame, direction: FrameDirection):
            await super().process_frame(frame, direction)
            await self.push_frame(frame, direction)

    class _FakeBridge:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs
            self.started = False
            self.start_calls = 0
            self.stopped = False
            self.media_started_notified = False
            self.dtmf_digits: list[str] = []

        def start(self):
            self.started = True
            self.start_calls += 1
            return self

        def stop(self) -> None:
            self.stopped = True

        def notify_media_started(self) -> None:
            self.media_started_notified = True

        def enqueue_dtmf_digit(self, digit: str) -> None:
            self.dtmf_digits.append(digit)

        def poll_audio_chunk(self, timeout: float | None = None):
            _ = timeout
            return None

        def poll_dtmf_digit(self, timeout: float | None = None):
            _ = timeout
            return None

        def send_output_audio(self, pcm16: bytes, *, sample_rate: int = 16000) -> None:
            _ = (pcm16, sample_rate)

        def flush_output_audio(self, pad_final_frame: bool = True) -> None:
            _ = pad_final_frame

    def _fake_pipeline_builder(self, **kwargs) -> GatewayPipelineArtifacts:
        bridge = kwargs["bridge"]
        policy = kwargs["policy_controller"]
        ingress = GatewayIngressProcessor(bridge, name="test-ingress")
        activity = SessionActivityProcessor(policy, name="test-activity")
        egress = GatewayEgressProcessor(bridge, name="test-egress")
        pipeline = Pipeline(
            [
                ingress,
                self._PassThroughProcessor("fake-stt"),
                self._PassThroughProcessor("fake-llm"),
                self._PassThroughProcessor("fake-tts"),
                activity,
                egress,
            ]
        )
        task = PipelineTask(
            pipeline,
            params=PipelineParams(audio_in_sample_rate=16000, audio_out_sample_rate=16000),
            cancel_on_idle_timeout=False,
            enable_rtvi=False,
            enable_turn_tracking=False,
        )
        return GatewayPipelineArtifacts(
            ingress=ingress,
            activity=activity,
            egress=egress,
            turn_timing=TurnTimingProcessor("sess-1", name="test-turn-timing"),
            pipeline=pipeline,
            task=task,
            greeting_pcm16=None,
        )

    def _new_worker(
        self,
        *,
        runtime: SessionRuntime | None = None,
        on_hangup_request=None,
    ) -> PipecatCallWorker:
        return PipecatCallWorker(
            session_id="sess-1",
            rx_port=41000,
            tx_port=41001,
            host="127.0.0.1",
            no_speech_timeout_s=10.0,
            max_duration_s=3600.0,
            on_hangup_request=on_hangup_request,
            runtime=runtime,
            bridge_factory=self._FakeBridge,
            pipeline_builder=self._fake_pipeline_builder,
            artifact_writer_factory=partial(
                CallArtifactWriter,
                base_dir=self._artifact_dir.name,
            ),
        )

    def test_start_and_stop_update_runtime_lifecycle(self) -> None:
        worker = self._new_worker()

        worker.start()
        self.assertEqual(worker.runtime.state, SessionRuntimeState.READY)
        self.assertIsNotNone(worker._thread)
        self.assertTrue(worker._thread.is_alive())
        self.assertIsNotNone(worker.bridge)
        self.assertTrue(worker.bridge.started)

        worker.stop()
        self.assertEqual(worker.runtime.state, SessionRuntimeState.ENDED)
        self.assertTrue(worker.bridge.stopped)

    def test_start_is_idempotent_while_running(self) -> None:
        worker = self._new_worker()

        worker.start()
        first_thread = worker._thread
        bridge = worker.bridge
        worker.start()

        self.assertIs(worker._thread, first_thread)
        self.assertIs(worker.bridge, bridge)
        self.assertEqual(worker.bridge.start_calls, 1)
        worker.stop()

    def test_media_ready_gate_and_dtmf_queue_are_available(self) -> None:
        worker = self._new_worker()
        worker.start()

        self.assertFalse(worker.wait_for_media_ready(timeout=0.0))
        worker.notify_media_started()
        self.assertTrue(worker.wait_for_media_ready(timeout=0.0))
        self.assertTrue(worker.bridge.media_started_notified)

        worker.on_dtmf_digit("1")
        worker.on_dtmf_digit("#")
        self.assertEqual(worker.poll_dtmf_digit(), "1")
        self.assertEqual(worker.poll_dtmf_digit(), "#")
        self.assertIsNone(worker.poll_dtmf_digit())
        self.assertEqual(worker.bridge.dtmf_digits, ["1", "#"])
        worker.stop()

    def test_runtime_session_id_mismatch_raises(self) -> None:
        runtime = SessionRuntime(session_id="other-session")

        with self.assertRaises(ValueError):
            self._new_worker(runtime=runtime)

    def test_request_hangup_notifies_manager_callback_once(self) -> None:
        requests: list[tuple[str, str]] = []
        worker = self._new_worker(
            on_hangup_request=lambda session_id, reason: requests.append(
                (session_id, reason)
            )
        )

        worker._request_hangup("policy_timeout")
        worker._request_hangup("policy_timeout_duplicate")

        self.assertEqual(requests, [("sess-1", "policy_timeout")])

    def test_stop_before_media_ready_persists_local_call_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            worker = PipecatCallWorker(
                session_id="sess-1",
                rx_port=41000,
                tx_port=41001,
                host="127.0.0.1",
                no_speech_timeout_s=10.0,
                max_duration_s=3600.0,
                runtime=SessionRuntime(session_id="sess-1"),
                bridge_factory=self._FakeBridge,
                pipeline_builder=self._fake_pipeline_builder,
                artifact_writer_factory=partial(
                    CallArtifactWriter,
                    base_dir=tmp_dir,
                ),
            )

            worker.start()
            worker.stop()

            call_dir = Path(tmp_dir) / "sess-1"
            self.assertTrue((call_dir / "ticket.json").exists())
            self.assertTrue((call_dir / "metadata.json").exists())
            self.assertTrue((call_dir / "caller.wav").exists())
            self.assertTrue((call_dir / "assistant.wav").exists())

            ticket_payload = json.loads((call_dir / "ticket.json").read_text(encoding="utf-8"))
            self.assertEqual(ticket_payload["call_status"], "finalized")
            self.assertEqual(ticket_payload["end_reason"], "worker_stop_requested")

            metadata_payload = json.loads((call_dir / "metadata.json").read_text(encoding="utf-8"))
            self.assertEqual(metadata_payload["status"], "finalized")
            self.assertEqual(metadata_payload["runtime"]["state"], "ended")

    def test_run_async_failure_requests_worker_error_hangup(self) -> None:
        hangups: list[tuple[str, str]] = []
        hangup_event = threading.Event()

        def _on_hangup(session_id: str, reason: str) -> None:
            hangups.append((session_id, reason))
            hangup_event.set()

        worker = PipecatCallWorker(
            session_id="sess-1",
            rx_port=41000,
            tx_port=41001,
            host="127.0.0.1",
            no_speech_timeout_s=10.0,
            max_duration_s=3600.0,
            on_hangup_request=_on_hangup,
            runtime=SessionRuntime(session_id="sess-1"),
            bridge_factory=self._FakeBridge,
            artifact_writer_factory=partial(
                CallArtifactWriter,
                base_dir=self._artifact_dir.name,
            ),
        )

        async def _raise_runtime_error() -> None:
            raise RuntimeError("runtime failed")

        try:
            with patch.object(worker, "_run_async", new=_raise_runtime_error):
                worker._run()
            self.assertTrue(hangup_event.is_set())
            self.assertEqual(hangups, [("sess-1", "worker_error")])
            self.assertEqual(
                worker.runtime.cancel_reason,
                "hangup_requested:worker_error",
            )
        finally:
            worker.stop()

    def test_gateway_call_end_reason_is_preserved_in_final_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            worker = PipecatCallWorker(
                session_id="sess-1",
                rx_port=41000,
                tx_port=41001,
                host="127.0.0.1",
                no_speech_timeout_s=10.0,
                max_duration_s=3600.0,
                runtime=SessionRuntime(session_id="sess-1"),
                bridge_factory=self._FakeBridge,
                pipeline_builder=self._fake_pipeline_builder,
                artifact_writer_factory=partial(
                    CallArtifactWriter,
                    base_dir=tmp_dir,
                ),
            )

            worker.start()
            worker.record_control_event(
                source="gateway",
                event="call_end",
                data={"reason": "disconnected"},
            )
            worker.stop()

            call_dir = Path(tmp_dir) / "sess-1"
            ticket_payload = json.loads((call_dir / "ticket.json").read_text(encoding="utf-8"))
            metadata_payload = json.loads((call_dir / "metadata.json").read_text(encoding="utf-8"))

            self.assertEqual(ticket_payload["end_reason"], "gateway_end:disconnected")
            self.assertEqual(metadata_payload["runtime"]["cancel_reason"], "gateway_end:disconnected")
