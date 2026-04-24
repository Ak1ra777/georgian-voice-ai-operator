from __future__ import annotations

import re
from collections.abc import Callable

from pipecat.frames.frames import InterimTranscriptionFrame, TranscriptionFrame
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

from src.pipecat_runtime.dtmf_capture import DtmfCollectionCoordinator


_HOLD_PATTERNS = (
    re.compile(r"\b(wait|hold on|one second|two seconds?|one minute)\b", re.IGNORECASE),
    re.compile(r"ორი წამი"),
    re.compile(r"ერთი წამი"),
    re.compile(r"ერთი წუთი"),
    re.compile(r"ორი წუთი"),
    re.compile(r"მოიცა"),
    re.compile(r"მოიცადე"),
    re.compile(r"მოიცადეთ"),
    re.compile(r"დამელოდე"),
    re.compile(r"დამელოდეთ"),
)

_READY_TO_ENTER_PATTERNS = (
    re.compile(r"\b(i found it|i.?ll enter it now|enter it now)\b", re.IGNORECASE),
    re.compile(r"ვნახე.*შევიყვან", re.IGNORECASE),
    re.compile(r"ახლა შევიყვან", re.IGNORECASE),
    re.compile(r"შევიყვან ახლა", re.IGNORECASE),
)


class DtmfSpeechGuardProcessor(FrameProcessor):
    """Ignore short spoken detours while keypad collection is active."""

    def __init__(
        self,
        coordinator: DtmfCollectionCoordinator,
        *,
        event_recorder: Callable[[str, dict[str, object]], None] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(enable_direct_mode=True, **kwargs)
        self._coordinator = coordinator
        self._event_recorder = event_recorder

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if (
            direction == FrameDirection.DOWNSTREAM
            and isinstance(frame, (TranscriptionFrame, InterimTranscriptionFrame))
        ):
            active_kind = self._coordinator.active_kind()
            reason = _classify_guard_reason(frame.text) if active_kind else None
            if active_kind and reason is not None:
                self._record_event(
                    "speech_ignored_during_dtmf_collection",
                    {
                        "kind": active_kind,
                        "reason": reason,
                        "is_final": isinstance(frame, TranscriptionFrame),
                        "chars": len(str(frame.text or "").strip()),
                    },
                )
                return

        await self.push_frame(frame, direction)

    def _record_event(self, event: str, data: dict[str, object]) -> None:
        if self._event_recorder is None:
            return
        try:
            self._event_recorder(event, data)
        except Exception:
            return


def _classify_guard_reason(text: str) -> str | None:
    normalized = str(text or "").strip()
    if not normalized:
        return None
    for pattern in _HOLD_PATTERNS:
        if pattern.search(normalized):
            return "hold_request"
    for pattern in _READY_TO_ENTER_PATTERNS:
        if pattern.search(normalized):
            return "ready_to_enter"
    return None
