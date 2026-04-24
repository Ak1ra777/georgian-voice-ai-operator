from __future__ import annotations

from pipecat.frames.frames import (
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    InputDTMFFrame,
    InterimTranscriptionFrame,
    OutputAudioRawFrame,
    TranscriptionFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

from src.pipecat_runtime.policy import SessionPolicyController


class SessionActivityProcessor(FrameProcessor):
    """Update session policy state from Pipecat speech and transcript frames."""

    def __init__(self, policy_controller: SessionPolicyController, **kwargs) -> None:
        super().__init__(enable_direct_mode=True, **kwargs)
        self._policy_controller = policy_controller

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if isinstance(frame, (TranscriptionFrame, InterimTranscriptionFrame)):
            if frame.text.strip():
                self._policy_controller.record_user_activity()
        elif isinstance(frame, (UserStartedSpeakingFrame, UserStoppedSpeakingFrame)):
            self._policy_controller.record_user_activity()
        elif isinstance(frame, InputDTMFFrame):
            self._policy_controller.record_user_activity()
        elif isinstance(frame, BotStartedSpeakingFrame):
            self._policy_controller.record_bot_started()
        elif isinstance(frame, BotStoppedSpeakingFrame):
            self._policy_controller.record_bot_stopped()
        elif isinstance(frame, OutputAudioRawFrame):
            self._policy_controller.record_bot_activity()

        await self.push_frame(frame, direction)
