from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock

from pipecat.frames.frames import (
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    InterimTranscriptionFrame,
    TranscriptionFrame,
)
from pipecat.turns.types import ProcessFrameResult

from src.pipecat_runtime.user_turn_start import BotAwareTranscriptionUserTurnStartStrategy


class BotAwareTranscriptionUserTurnStartStrategyTest(unittest.TestCase):
    def test_final_transcription_is_ignored_when_bot_is_not_speaking(self) -> None:
        strategy = BotAwareTranscriptionUserTurnStartStrategy()
        strategy.trigger_user_turn_started = AsyncMock()

        async def _run() -> ProcessFrameResult:
            return await strategy.process_frame(
                TranscriptionFrame(
                    text="გამარჯობა",
                    user_id="user-1",
                    timestamp="2026-04-22T15:17:26.365Z",
                )
            )

        result = asyncio.run(_run())

        self.assertEqual(result, ProcessFrameResult.CONTINUE)
        strategy.trigger_user_turn_started.assert_not_awaited()

    def test_final_transcription_can_interrupt_only_while_bot_is_speaking(self) -> None:
        strategy = BotAwareTranscriptionUserTurnStartStrategy()
        strategy.trigger_user_turn_started = AsyncMock()

        async def _run() -> ProcessFrameResult:
            await strategy.process_frame(BotStartedSpeakingFrame())
            return await strategy.process_frame(
                TranscriptionFrame(
                    text="მომისმინე",
                    user_id="user-1",
                    timestamp="2026-04-22T15:17:26.365Z",
                )
            )

        result = asyncio.run(_run())

        self.assertEqual(result, ProcessFrameResult.STOP)
        strategy.trigger_user_turn_started.assert_awaited_once()

    def test_interim_transcription_stops_triggering_after_bot_stops(self) -> None:
        strategy = BotAwareTranscriptionUserTurnStartStrategy()
        strategy.trigger_user_turn_started = AsyncMock()

        async def _run() -> ProcessFrameResult:
            await strategy.process_frame(BotStartedSpeakingFrame())
            await strategy.process_frame(BotStoppedSpeakingFrame())
            return await strategy.process_frame(
                InterimTranscriptionFrame(
                    text="გამარჯობა",
                    user_id="user-1",
                    timestamp="2026-04-22T15:17:26.365Z",
                )
            )

        result = asyncio.run(_run())

        self.assertEqual(result, ProcessFrameResult.CONTINUE)
        strategy.trigger_user_turn_started.assert_not_awaited()
