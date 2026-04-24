from __future__ import annotations

from functools import partial
import queue
import tempfile
import threading
import unittest
import wave
from pathlib import Path

from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

from src.pipecat_runtime.artifacts import CallArtifactWriter
from src.pipecat_runtime.call_worker import PipecatCallWorker
from src.pipecat_runtime.pipeline_factory import build_gateway_pipeline


class _FakeBridge:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.media_started_notified = False
        self.dtmf_digits: list[str] = []
        self.sent_audio: list[tuple[bytes, int]] = []
        self.flush_calls: list[bool] = []
        self._audio_queue: queue.Queue[bytes] = queue.Queue()
        self._dtmf_queue: queue.Queue[str] = queue.Queue()

    def start(self):
        self.started = True
        return self

    def stop(self) -> None:
        self.stopped = True

    def notify_media_started(self) -> None:
        self.media_started_notified = True

    def enqueue_dtmf_digit(self, digit: str) -> None:
        self.dtmf_digits.append(digit)
        self._dtmf_queue.put(digit)

    def poll_audio_chunk(self, timeout: float | None = None):
        try:
            if timeout is None or timeout <= 0.0:
                return self._audio_queue.get_nowait()
            return self._audio_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def poll_dtmf_digit(self, timeout: float | None = None):
        try:
            if timeout is None or timeout <= 0.0:
                return self._dtmf_queue.get_nowait()
            return self._dtmf_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def send_output_audio(self, pcm16: bytes, *, sample_rate: int = 16000) -> None:
        self.sent_audio.append((pcm16, sample_rate))

    def flush_output_audio(self, pad_final_frame: bool = True) -> None:
        self.flush_calls.append(pad_final_frame)


class _PassThroughProcessor(FrameProcessor):
    def __init__(self, name: str) -> None:
        super().__init__(name=name, enable_direct_mode=True)

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        await self.push_frame(frame, direction)


class PipecatWorkerGreetingTest(unittest.TestCase):
    def _write_test_wav(self) -> str:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as handle:
            path = Path(handle.name)

        with wave.open(str(path), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(16000)
            wav_file.writeframes(b"\x01\x00" * 160)

        self.addCleanup(lambda: path.unlink(missing_ok=True))
        return str(path)

    def test_worker_plays_greeting_then_policy_requests_no_speech_hangup(self) -> None:
        hangups: list[tuple[str, str]] = []
        hangup_event = threading.Event()
        with tempfile.TemporaryDirectory() as artifact_dir:
            worker = PipecatCallWorker(
                session_id="sess-1",
                rx_port=41000,
                tx_port=41001,
                host="127.0.0.1",
                no_speech_timeout_s=0.2,
                max_duration_s=10.0,
                on_hangup_request=lambda session_id, reason: (
                    hangups.append((session_id, reason)),
                    hangup_event.set(),
                ),
                bridge_factory=_FakeBridge,
                greeting_enabled=True,
                greeting_audio_path=self._write_test_wav(),
                pipeline_builder=partial(
                    build_gateway_pipeline,
                    stt_factory=lambda: _PassThroughProcessor("fake-stt"),
                    llm_factory=lambda: _PassThroughProcessor("fake-llm"),
                    tts_factory=lambda: _PassThroughProcessor("fake-tts"),
                ),
                artifact_writer_factory=partial(
                    CallArtifactWriter,
                    base_dir=artifact_dir,
                ),
            )

            worker.start()
            stopped = False
            try:
                worker.notify_media_started()
                self.assertTrue(
                    hangup_event.wait(timeout=3.0),
                    "Expected policy-driven hangup after greeting and silence",
                )
                self.assertEqual(hangups, [("sess-1", "no_speech_timeout")])
                self.assertIsNotNone(worker.bridge)
                self.assertTrue(worker.bridge.sent_audio)
                worker.stop()
                stopped = True
                self.assertTrue(worker.bridge.stopped)
            finally:
                if not stopped:
                    worker.stop()
