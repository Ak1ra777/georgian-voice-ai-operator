from __future__ import annotations

from pipecat.frames.frames import (
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    Frame,
    InterimTranscriptionFrame,
    TranscriptionFrame,
)
from pipecat.turns.types import ProcessFrameResult
from pipecat.turns.user_start.base_user_turn_start_strategy import (
    BaseUserTurnStartStrategy,
)


class BotAwareTranscriptionUserTurnStartStrategy(BaseUserTurnStartStrategy):
    """Fallback transcription start strategy gated on active bot speech.

    Pipecat's stock transcription start strategy is documented as a fallback
    for cases where the user starts speaking while the bot is already talking.
    In practice it currently triggers on any transcription frame, which can
    reopen a completed user turn when a final transcript arrives slightly after
    turn stop. This implementation preserves the intended fallback behavior
    without allowing late STT frames to create phantom turns.
    """

    def __init__(self, *, use_interim: bool = True, **kwargs):
        super().__init__(**kwargs)
        self._use_interim = use_interim
        self._bot_speaking = False

    async def process_frame(self, frame: Frame) -> ProcessFrameResult:
        if isinstance(frame, BotStartedSpeakingFrame):
            self._bot_speaking = True
            return ProcessFrameResult.CONTINUE

        if isinstance(frame, BotStoppedSpeakingFrame):
            self._bot_speaking = False
            return ProcessFrameResult.CONTINUE

        if not self._bot_speaking:
            return ProcessFrameResult.CONTINUE

        if isinstance(frame, InterimTranscriptionFrame) and self._use_interim:
            if frame.text.strip():
                await self.trigger_user_turn_started()
                return ProcessFrameResult.STOP
            return ProcessFrameResult.CONTINUE

        if isinstance(frame, TranscriptionFrame) and frame.text.strip():
            await self.trigger_user_turn_started()
            return ProcessFrameResult.STOP

        return ProcessFrameResult.CONTINUE
