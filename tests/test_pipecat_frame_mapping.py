from __future__ import annotations

import asyncio
import queue
import unittest

from pipecat.audio.dtmf.types import KeypadEntry
from pipecat.frames.frames import (
    BotStoppedSpeakingFrame,
    EndFrame,
    InputAudioRawFrame,
    InputDTMFFrame,
    InterruptionFrame,
    OutputAudioRawFrame,
    OutputTransportMessageUrgentFrame,
    TTSStoppedFrame,
)
from pipecat.processors.frame_processor import FrameDirection

from src.pipecat_runtime.hangup import DeferredHangupController
from src.pipecat_runtime.processors.egress import GatewayEgressProcessor
from src.pipecat_runtime.processors.ingress import GatewayIngressProcessor


class _FakeBridge:
    def __init__(self) -> None:
        self.session_id = "sess-1"
        self._audio_queue: queue.Queue[bytes] = queue.Queue()
        self._dtmf_queue: queue.Queue[str] = queue.Queue()
        self.sent_audio: list[tuple[bytes, int]] = []
        self.flush_calls: list[bool] = []

    def push_audio(self, chunk: bytes) -> None:
        self._audio_queue.put(chunk)

    def push_dtmf(self, digit: str) -> None:
        self._dtmf_queue.put(digit)

    def poll_audio_chunk(self, timeout: float | None = None) -> bytes | None:
        try:
            if timeout is None or timeout <= 0.0:
                return self._audio_queue.get_nowait()
            return self._audio_queue.get(timeout=timeout)
        except queue.Empty:
            return None

    def poll_dtmf_digit(self, timeout: float | None = None) -> str | None:
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

    def clear_output_audio(self) -> None:
        self.flush_calls.append(False)


class PipecatFrameMappingTest(unittest.TestCase):
    def test_ingress_maps_gateway_audio_and_dtmf_to_pipecat_frames(self) -> None:
        bridge = _FakeBridge()
        bridge.push_dtmf("1")
        bridge.push_audio(b"\x01\x00" * 8)
        observed_audio: list[tuple[bytes, int, int]] = []
        processor = GatewayIngressProcessor(
            bridge,
            on_audio_chunk=lambda audio, sample_rate, num_channels: observed_audio.append(
                (audio, sample_rate, num_channels)
            ),
        )

        frames = processor.drain_ready_frames(audio_timeout_s=0.0)

        self.assertEqual(len(frames), 2)
        self.assertIsInstance(frames[0], InputDTMFFrame)
        self.assertEqual(frames[0].button, KeypadEntry.ONE)
        self.assertIsInstance(frames[1], InputAudioRawFrame)
        self.assertEqual(frames[1].audio, b"\x01\x00" * 8)
        self.assertEqual(frames[1].sample_rate, 16000)
        self.assertEqual(frames[1].num_channels, 1)
        self.assertEqual(observed_audio, [(b"\x01\x00" * 8, 16000, 1)])

    def test_ingress_drains_buffered_audio_in_small_batches(self) -> None:
        bridge = _FakeBridge()
        bridge.push_audio(b"\x01\x00" * 4)
        bridge.push_audio(b"\x02\x00" * 4)
        bridge.push_audio(b"\x03\x00" * 4)
        processor = GatewayIngressProcessor(bridge, max_audio_frames_per_drain=3)

        frames = processor.drain_ready_frames(audio_timeout_s=0.0)

        self.assertEqual(len(frames), 3)
        self.assertTrue(all(isinstance(frame, InputAudioRawFrame) for frame in frames))
        self.assertEqual([frame.audio for frame in frames], [b"\x01\x00" * 4, b"\x02\x00" * 4, b"\x03\x00" * 4])

    def test_ingress_ignores_invalid_dtmf_digits(self) -> None:
        bridge = _FakeBridge()
        bridge.push_dtmf("A")
        processor = GatewayIngressProcessor(bridge)

        self.assertEqual(processor.drain_ready_frames(audio_timeout_s=0.0), [])

    def test_egress_maps_output_audio_to_gateway_sink(self) -> None:
        bridge = _FakeBridge()
        observed_audio: list[tuple[bytes, int, int]] = []
        processor = GatewayEgressProcessor(
            bridge,
            on_audio_chunk=lambda audio, sample_rate, num_channels: observed_audio.append(
                (audio, sample_rate, num_channels)
            ),
        )
        frame = OutputAudioRawFrame(
            audio=b"\x10\x20",
            sample_rate=8000,
            num_channels=1,
        )

        asyncio.run(processor.process_frame(frame, FrameDirection.DOWNSTREAM))

        self.assertEqual(bridge.sent_audio, [(b"\x10\x20", 8000)])
        self.assertEqual(observed_audio, [(b"\x10\x20", 8000, 1)])
        snapshot = processor.snapshot()
        self.assertEqual(snapshot.output_audio_frames, 1)
        self.assertEqual(snapshot.output_audio_bytes, 2)

    def test_egress_maps_urgent_hangup_message_to_callback(self) -> None:
        bridge = _FakeBridge()
        requests: list[str] = []
        processor = GatewayEgressProcessor(
            bridge,
            request_hangup_fn=lambda reason: requests.append(reason),
        )
        frame = OutputTransportMessageUrgentFrame(
            message={"type": "gateway_hangup", "reason": "no_speech_timeout"}
        )

        asyncio.run(processor.process_frame(frame, FrameDirection.DOWNSTREAM))

        self.assertEqual(requests, ["no_speech_timeout"])

    def test_egress_flushes_audio_stream_on_pipeline_end(self) -> None:
        bridge = _FakeBridge()
        processor = GatewayEgressProcessor(bridge)

        asyncio.run(
            processor.process_frame(EndFrame(reason="greeting_complete"), FrameDirection.DOWNSTREAM)
        )

        self.assertEqual(bridge.flush_calls, [True])

    def test_egress_flushes_audio_on_tts_stop_boundary(self) -> None:
        bridge = _FakeBridge()
        processor = GatewayEgressProcessor(bridge)

        asyncio.run(
            processor.process_frame(TTSStoppedFrame(context_id="ctx-1"), FrameDirection.DOWNSTREAM)
        )
        asyncio.run(
            processor.process_frame(BotStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
        )

        self.assertEqual(bridge.flush_calls, [True, True])
        self.assertEqual(processor.snapshot().speech_boundary_flushes, 2)

    def test_egress_triggers_deferred_hangup_after_bot_stops(self) -> None:
        bridge = _FakeBridge()
        requests: list[str] = []
        deferred_hangup = DeferredHangupController()
        deferred_hangup.schedule("assistant_completed")
        processor = GatewayEgressProcessor(
            bridge,
            request_hangup_fn=lambda reason: requests.append(reason),
            deferred_hangup=deferred_hangup,
        )

        asyncio.run(
            processor.process_frame(BotStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
        )

        self.assertEqual(requests, ["assistant_completed"])
        self.assertEqual(bridge.flush_calls, [True])

    def test_egress_clears_deferred_hangup_on_interruption(self) -> None:
        bridge = _FakeBridge()
        requests: list[str] = []
        deferred_hangup = DeferredHangupController()
        deferred_hangup.schedule("assistant_completed")
        processor = GatewayEgressProcessor(
            bridge,
            request_hangup_fn=lambda reason: requests.append(reason),
            deferred_hangup=deferred_hangup,
        )

        asyncio.run(
            processor.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
        )
        asyncio.run(
            processor.process_frame(BotStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
        )

        self.assertEqual(requests, [])
        self.assertEqual(bridge.flush_calls, [False, True])

    def test_egress_clears_audio_on_interruption(self) -> None:
        bridge = _FakeBridge()
        processor = GatewayEgressProcessor(bridge)

        asyncio.run(
            processor.process_frame(InterruptionFrame(), FrameDirection.DOWNSTREAM)
        )

        self.assertEqual(bridge.flush_calls, [False])
        self.assertEqual(processor.snapshot().interruption_count, 1)
