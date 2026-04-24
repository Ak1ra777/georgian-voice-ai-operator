from __future__ import annotations

import asyncio
import unittest

from pipecat.frames.frames import (
    AggregatedTextFrame,
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    EndFrame,
    InterruptionFrame,
    LLMFullResponseStartFrame,
    StartFrame,
    TTSAudioRawFrame,
    TTSStartedFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.utils.text.base_text_aggregator import AggregationType

from src.pipecat_runtime.processors.turn_timing import TurnTimingProcessor


class _FakeClock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class TurnTimingProcessorTest(unittest.TestCase):
    def test_turn_timing_processor_emits_compact_latency_markers(self) -> None:
        clock = _FakeClock()
        events: list[str] = []
        processor = TurnTimingProcessor(
            "sess-1",
            clock=clock,
            logger=events.append,
        )

        async def _run() -> None:
            await processor.process_frame(
                StartFrame(audio_in_sample_rate=16000, audio_out_sample_rate=16000),
                FrameDirection.DOWNSTREAM,
            )
            await processor.process_frame(
                UserStartedSpeakingFrame(),
                FrameDirection.DOWNSTREAM,
            )
            clock.advance(0.4)
            await processor.process_frame(
                UserStoppedSpeakingFrame(),
                FrameDirection.DOWNSTREAM,
            )
            clock.advance(0.2)
            await processor.process_frame(
                LLMFullResponseStartFrame(),
                FrameDirection.DOWNSTREAM,
            )
            clock.advance(0.1)
            await processor.process_frame(
                AggregatedTextFrame("გამარჯობა", AggregationType.TOKEN),
                FrameDirection.DOWNSTREAM,
            )
            clock.advance(0.05)
            await processor.process_frame(
                TTSStartedFrame(context_id="ctx-1"),
                FrameDirection.DOWNSTREAM,
            )
            clock.advance(0.05)
            await processor.process_frame(
                TTSAudioRawFrame(
                    audio=b"\x00\x00" * 20,
                    sample_rate=24000,
                    num_channels=1,
                    context_id="ctx-1",
                ),
                FrameDirection.DOWNSTREAM,
            )
            clock.advance(0.05)
            await processor.process_frame(
                BotStartedSpeakingFrame(),
                FrameDirection.DOWNSTREAM,
            )
            clock.advance(0.3)
            await processor.process_frame(
                BotStoppedSpeakingFrame(),
                FrameDirection.DOWNSTREAM,
            )

        asyncio.run(_run())

        self.assertEqual(
            [event.split("event=", 1)[1].split()[0] for event in events],
            [
                "user_started",
                "user_stopped",
                "llm_started",
                "first_tts_text",
                "tts_started",
                "first_audio",
                "bot_started",
                "bot_stopped",
            ],
        )
        self.assertIn("user_speech_ms=400.0", events[1])
        self.assertIn("since_user_stop_ms=200.0", events[2])
        self.assertIn("since_llm_start_ms=100.0", events[3])
        self.assertIn("since_first_tts_text_ms=100.0", events[5])
        self.assertIn("bot_speaking_ms=300.0", events[7])

        snapshot = processor.snapshot()
        self.assertEqual(snapshot.turns_started, 1)
        self.assertEqual(snapshot.turns_archived, 1)
        self.assertEqual(snapshot.turns_with_llm_start, 1)
        self.assertEqual(snapshot.turns_with_first_audio, 1)
        self.assertEqual(snapshot.interruptions, 0)
        self.assertEqual(snapshot.last_archive_reason, "bot_stopped")
        self.assertEqual(snapshot.last_user_speech_ms, 400)
        self.assertEqual(snapshot.last_llm_started_after_user_stop_ms, 200)
        self.assertEqual(snapshot.last_first_audio_after_user_stop_ms, 400)
        self.assertEqual(snapshot.last_first_audio_after_llm_start_ms, 200)
        self.assertEqual(snapshot.avg_first_audio_after_user_stop_ms, 400)
        self.assertEqual(snapshot.max_first_audio_after_user_stop_ms, 400)

    def test_interruption_does_not_archive_new_turn_and_terminal_frame_flushes_it(self) -> None:
        clock = _FakeClock()
        events: list[str] = []
        archived_turns = []
        processor = TurnTimingProcessor(
            "sess-1",
            clock=clock,
            logger=events.append,
            turn_archived_callback=archived_turns.append,
        )

        async def _run() -> None:
            await processor.process_frame(
                UserStartedSpeakingFrame(),
                FrameDirection.DOWNSTREAM,
            )
            clock.advance(0.1)
            await processor.process_frame(
                InterruptionFrame(),
                FrameDirection.DOWNSTREAM,
            )
            clock.advance(0.3)
            await processor.process_frame(
                UserStoppedSpeakingFrame(),
                FrameDirection.DOWNSTREAM,
            )
            clock.advance(0.2)
            await processor.process_frame(
                LLMFullResponseStartFrame(),
                FrameDirection.DOWNSTREAM,
            )
            clock.advance(0.1)
            await processor.process_frame(
                TTSAudioRawFrame(
                    audio=b"\x00\x00" * 20,
                    sample_rate=16000,
                    num_channels=1,
                ),
                FrameDirection.DOWNSTREAM,
            )
            clock.advance(0.1)
            await processor.process_frame(
                EndFrame(),
                FrameDirection.DOWNSTREAM,
            )

        asyncio.run(_run())

        snapshot = processor.snapshot()
        self.assertEqual(snapshot.turns_started, 1)
        self.assertEqual(snapshot.turns_archived, 1)
        self.assertEqual(snapshot.turns_with_llm_start, 1)
        self.assertEqual(snapshot.turns_with_first_audio, 1)
        self.assertEqual(snapshot.interruptions, 1)
        self.assertEqual(snapshot.last_archive_reason, "terminal")
        self.assertEqual(len(archived_turns), 1)
        self.assertTrue(archived_turns[0].spoke)
        self.assertTrue(archived_turns[0].had_llm_start)
        self.assertTrue(archived_turns[0].had_first_audio)
        self.assertEqual(
            [event.split("event=", 1)[1].split()[0] for event in events],
            [
                "user_started",
                "interruption",
                "user_stopped",
                "llm_started",
                "first_audio",
            ],
        )
