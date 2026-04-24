from __future__ import annotations

from collections.abc import Callable

from pipecat.frames.frames import (
    BotStoppedSpeakingFrame,
    CancelFrame,
    EndFrame,
    InterruptionFrame,
    LLMTextFrame,
    StopFrame,
    TTSStoppedFrame,
    TTSTextFrame,
    TranscriptionFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

from src.pipecat_runtime.artifacts import CallArtifactWriter


class UserTranscriptCaptureProcessor(FrameProcessor):
    """Persist finalized caller STT utterances into the call transcript."""

    def __init__(
        self,
        writer: CallArtifactWriter,
        *,
        flow_state_provider: Callable[[], dict[str, str | None]] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(enable_direct_mode=True, **kwargs)
        self._writer = writer
        self._flow_state_provider = flow_state_provider

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if direction == FrameDirection.DOWNSTREAM and isinstance(
            frame,
            TranscriptionFrame,
        ):
            self._writer.append_user_transcript(
                frame.text,
                timestamp=frame.timestamp,
                language=str(frame.language) if frame.language is not None else None,
                user_id=frame.user_id,
                **_flow_transcript_fields(self._flow_state_provider),
            )

        await self.push_frame(frame, direction)


class AssistantTranscriptCaptureProcessor(FrameProcessor):
    """Aggregate assistant text into one transcript entry per spoken response."""

    def __init__(
        self,
        writer: CallArtifactWriter,
        *,
        flow_state_provider: Callable[[], dict[str, str | None]] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(enable_direct_mode=True, **kwargs)
        self._writer = writer
        self._flow_state_provider = flow_state_provider
        self._tts_buffer = ""
        self._llm_buffer = ""

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if direction == FrameDirection.DOWNSTREAM:
            if isinstance(frame, TTSTextFrame):
                self._tts_buffer = _append_text_segment(self._tts_buffer, frame.text)
            elif isinstance(frame, LLMTextFrame):
                self._llm_buffer = _append_text_segment(self._llm_buffer, frame.text)
            elif isinstance(
                frame,
                (
                    TTSStoppedFrame,
                    BotStoppedSpeakingFrame,
                    InterruptionFrame,
                    EndFrame,
                    CancelFrame,
                    StopFrame,
                ),
            ):
                self._flush(context_id=getattr(frame, "context_id", None))

        await self.push_frame(frame, direction)

    def _flush(self, *, context_id: str | None = None) -> None:
        text = self._tts_buffer or self._llm_buffer
        self._tts_buffer = ""
        self._llm_buffer = ""
        if not text.strip():
            return
        self._writer.append_assistant_transcript(
            text,
            context_id=context_id,
            **_flow_transcript_fields(self._flow_state_provider),
        )


def _append_text_segment(buffer: str, segment: str) -> str:
    text = str(segment).strip()
    if not text:
        return buffer
    if not buffer:
        return text
    if text[0] in {".", ",", "!", "?", ";", ":"}:
        return f"{buffer}{text}"
    return f"{buffer} {text}"


def _flow_transcript_fields(
    flow_state_provider: Callable[[], dict[str, str | None]] | None,
) -> dict[str, str]:
    if flow_state_provider is None:
        return {}
    try:
        flow_state = flow_state_provider() or {}
    except Exception:
        return {}
    return {
        field_name: value
        for field_name, value in {
            "flow_current_node": flow_state.get("current_node"),
            "flow_branch": flow_state.get("branch"),
            "flow_active_dtmf_kind": flow_state.get("active_dtmf_kind"),
            "flow_last_tool_id": flow_state.get("last_tool_id"),
        }.items()
        if isinstance(value, str) and value.strip()
    }
