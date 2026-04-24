from __future__ import annotations

from pipecat.frames.frames import InputDTMFFrame
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

from src.pipecat_runtime.dtmf_capture import DtmfCollectionCoordinator


class SensitiveDtmfCollectorProcessor(FrameProcessor):
    """Handle active keypad collection without exposing sensitive digits to the LLM."""

    def __init__(self, coordinator: DtmfCollectionCoordinator, **kwargs) -> None:
        super().__init__(enable_direct_mode=True, **kwargs)
        self._coordinator = coordinator

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if isinstance(frame, InputDTMFFrame):
            _, should_interrupt = await self._coordinator.handle_digit(frame.button.value)
            if should_interrupt:
                await self.broadcast_interruption()

        await self.push_frame(frame, direction)
