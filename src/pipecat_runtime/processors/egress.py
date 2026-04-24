from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Callable

from pipecat.frames.frames import (
    BotStoppedSpeakingFrame,
    CancelFrame,
    EndFrame,
    InterruptionFrame,
    OutputAudioRawFrame,
    OutputTransportMessageUrgentFrame,
    StopFrame,
    TTSStoppedFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

from src.pipecat_runtime.gateway_bridge import GatewaySessionBridge
from src.pipecat_runtime.hangup import DeferredHangupController


@dataclass(frozen=True)
class GatewayEgressSnapshot:
    output_audio_frames: int
    output_audio_bytes: int
    transport_hangups: int
    interruption_count: int
    speech_boundary_flushes: int
    terminal_flushes: int


class GatewayEgressProcessor(FrameProcessor):
    """Convert Pipecat output frames into the current gateway seam."""

    def __init__(
        self,
        bridge: GatewaySessionBridge,
        *,
        request_hangup_fn: Callable[[str], None] | None = None,
        deferred_hangup: DeferredHangupController | None = None,
        on_audio_chunk: Callable[[bytes, int, int], None] | None = None,
        event_recorder: Callable[[str, dict], None] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(enable_direct_mode=True, **kwargs)
        self._bridge = bridge
        self._request_hangup_fn = request_hangup_fn
        self._deferred_hangup = deferred_hangup
        self._on_audio_chunk = on_audio_chunk
        self._event_recorder = event_recorder
        self._stats_lock = threading.Lock()
        self._output_audio_frames = 0
        self._output_audio_bytes = 0
        self._transport_hangups = 0
        self._interruption_count = 0
        self._speech_boundary_flushes = 0
        self._terminal_flushes = 0

    def handle_audio_frame(self, frame: OutputAudioRawFrame) -> None:
        with self._stats_lock:
            self._output_audio_frames += 1
            self._output_audio_bytes += len(frame.audio)
        if self._on_audio_chunk is not None:
            try:
                self._on_audio_chunk(
                    frame.audio,
                    frame.sample_rate,
                    frame.num_channels,
                )
            except Exception as ex:
                print(
                    "gateway_egress_audio_observer_failed "
                    f"session_id={self._bridge.session_id} error={ex}"
                )
                self._record_event(
                    "audio_observer_failed",
                    {
                        "error": str(ex),
                    },
                )
        self._bridge.send_output_audio(frame.audio, sample_rate=frame.sample_rate)

    def handle_transport_message(self, message) -> bool:
        if not isinstance(message, dict):
            return False
        if message.get("type") != "gateway_hangup":
            return False
        reason = str(message.get("reason", "")).strip() or "unspecified"
        with self._stats_lock:
            self._transport_hangups += 1
        print(
            "pipecat_gateway_hangup_requested "
            f"session_id={self._bridge.session_id} reason={reason}"
        )
        self._record_event(
            "gateway_hangup_requested",
            {
                "reason": reason,
            },
        )
        if self._request_hangup_fn is not None:
            self._request_hangup_fn(reason)
        return True

    def snapshot(self) -> GatewayEgressSnapshot:
        with self._stats_lock:
            return GatewayEgressSnapshot(
                output_audio_frames=self._output_audio_frames,
                output_audio_bytes=self._output_audio_bytes,
                transport_hangups=self._transport_hangups,
                interruption_count=self._interruption_count,
                speech_boundary_flushes=self._speech_boundary_flushes,
                terminal_flushes=self._terminal_flushes,
            )

    def _trigger_deferred_hangup(self, *, trigger: str) -> None:
        if self._deferred_hangup is None:
            return
        reason = self._deferred_hangup.consume()
        if reason is None:
            return
        print(
            "pipecat_deferred_hangup_triggered "
            f"session_id={self._bridge.session_id} trigger={trigger} reason={reason}"
        )
        self._record_event(
            "deferred_hangup_triggered",
            {
                "trigger": trigger,
                "reason": reason,
            },
        )
        if self._request_hangup_fn is not None:
            self._request_hangup_fn(reason)

    def _clear_deferred_hangup(self, *, trigger: str) -> None:
        if self._deferred_hangup is None:
            return
        reason = self._deferred_hangup.clear()
        if reason is None:
            return
        print(
            "pipecat_deferred_hangup_cleared "
            f"session_id={self._bridge.session_id} trigger={trigger} reason={reason}"
        )
        self._record_event(
            "deferred_hangup_cleared",
            {
                "trigger": trigger,
                "reason": reason,
            },
        )

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if direction == FrameDirection.DOWNSTREAM:
            if isinstance(frame, OutputAudioRawFrame):
                self.handle_audio_frame(frame)
            elif isinstance(frame, OutputTransportMessageUrgentFrame):
                self.handle_transport_message(frame.message)
            elif isinstance(frame, (TTSStoppedFrame, BotStoppedSpeakingFrame)):
                with self._stats_lock:
                    self._speech_boundary_flushes += 1
                self._bridge.flush_output_audio(pad_final_frame=True)
                if isinstance(frame, BotStoppedSpeakingFrame):
                    self._trigger_deferred_hangup(trigger="bot_stopped")
            elif isinstance(frame, InterruptionFrame):
                with self._stats_lock:
                    self._interruption_count += 1
                print(
                    "pipecat_audio_interruption "
                    f"session_id={self._bridge.session_id}"
                )
                self._record_event("audio_interruption", {})
                self._clear_deferred_hangup(trigger="interruption")
                self._bridge.clear_output_audio()
            elif isinstance(frame, (EndFrame, CancelFrame, StopFrame)):
                with self._stats_lock:
                    self._terminal_flushes += 1
                self._bridge.flush_output_audio(pad_final_frame=True)
                self._trigger_deferred_hangup(trigger="terminal")

        await self.push_frame(frame, direction)

    def _record_event(self, event: str, data: dict) -> None:
        if self._event_recorder is None:
            return
        try:
            self._event_recorder(event, data)
        except Exception:
            return
