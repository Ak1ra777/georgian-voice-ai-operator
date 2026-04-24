from __future__ import annotations

from typing import Callable

from pipecat.audio.dtmf.types import KeypadEntry
from pipecat.frames.frames import InputAudioRawFrame, InputDTMFFrame
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

from src.pipecat_runtime.gateway_bridge import GatewaySessionBridge


class GatewayIngressProcessor(FrameProcessor):
    """Convert queued gateway bridge inputs into Pipecat frames."""

    def __init__(
        self,
        bridge: GatewaySessionBridge,
        *,
        audio_sample_rate: int = 16000,
        num_channels: int = 1,
        max_audio_frames_per_drain: int = 5,
        on_audio_chunk: Callable[[bytes, int, int], None] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(enable_direct_mode=True, **kwargs)
        self._bridge = bridge
        self._audio_sample_rate = audio_sample_rate
        self._num_channels = num_channels
        self._max_audio_frames_per_drain = max(1, int(max_audio_frames_per_drain))
        self._on_audio_chunk = on_audio_chunk

    def build_audio_frame(self, pcm16: bytes) -> InputAudioRawFrame:
        frame = InputAudioRawFrame(
            audio=pcm16,
            sample_rate=self._audio_sample_rate,
            num_channels=self._num_channels,
        )
        if self._on_audio_chunk is not None:
            try:
                self._on_audio_chunk(
                    frame.audio,
                    frame.sample_rate,
                    frame.num_channels,
                )
            except Exception as ex:
                print(
                    "gateway_ingress_audio_observer_failed "
                    f"session_id={self._bridge.session_id} error={ex}"
                )
        return frame

    @staticmethod
    def build_dtmf_frame(digit: str) -> InputDTMFFrame:
        return InputDTMFFrame(KeypadEntry(str(digit)))

    def drain_ready_frames(self, *, audio_timeout_s: float | None = 0.0) -> list:
        frames: list = []

        while True:
            digit = self._bridge.poll_dtmf_digit(timeout=0.0)
            if digit is None:
                break
            try:
                frames.append(self.build_dtmf_frame(digit))
            except ValueError:
                print(
                    "gateway_ingress_invalid_dtmf "
                    f"session_id={self._bridge.session_id} digit={digit}"
                )

        audio_chunk = self._bridge.poll_audio_chunk(timeout=audio_timeout_s)
        if audio_chunk is not None:
            frames.append(self.build_audio_frame(audio_chunk))
            for _ in range(self._max_audio_frames_per_drain - 1):
                buffered_chunk = self._bridge.poll_audio_chunk(timeout=0.0)
                if buffered_chunk is None:
                    break
                frames.append(self.build_audio_frame(buffered_chunk))

        return frames

    async def emit_ready_frames(
        self,
        *,
        audio_timeout_s: float | None = 0.0,
        direction: FrameDirection = FrameDirection.DOWNSTREAM,
    ) -> None:
        for frame in self.drain_ready_frames(audio_timeout_s=audio_timeout_s):
            await self.push_frame(frame, direction)

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        await self.push_frame(frame, direction)
