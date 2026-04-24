from __future__ import annotations

import asyncio
import unittest

from pipecat.frames.frames import (
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    InputDTMFFrame,
    InterimTranscriptionFrame,
    OutputAudioRawFrame,
    StartFrame,
    TranscriptionFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.audio.dtmf.types import KeypadEntry

from src.pipecat_runtime.policy import SessionPolicyController
from src.pipecat_runtime.processors.activity import SessionActivityProcessor
from src.runtime.calls.session.runtime import SessionRuntime


class _FakeClock:
    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class SessionActivityProcessorTest(unittest.TestCase):
    def test_activity_processor_updates_policy_from_core_frames(self) -> None:
        clock = _FakeClock()
        policy = SessionPolicyController(
            session_id="sess-1",
            runtime=SessionRuntime(session_id="sess-1"),
            no_speech_timeout_s=5.0,
            max_duration_s=30.0,
            clock=clock,
        )
        policy.mark_media_ready()
        processor = SessionActivityProcessor(policy)
        asyncio.run(
            processor.process_frame(
                StartFrame(audio_in_sample_rate=16000, audio_out_sample_rate=16000),
                FrameDirection.DOWNSTREAM,
            )
        )

        asyncio.run(
            processor.process_frame(
                InterimTranscriptionFrame(
                    text="გამარჯობა",
                    user_id="caller",
                    timestamp="2026-01-01T00:00:00Z",
                ),
                FrameDirection.DOWNSTREAM,
            )
        )
        first_snapshot = policy.snapshot()
        self.assertEqual(first_snapshot.last_user_activity_at_monotonic_s, 0.0)

        clock.advance(1.0)
        asyncio.run(
            processor.process_frame(BotStartedSpeakingFrame(), FrameDirection.DOWNSTREAM)
        )
        clock.advance(0.5)
        asyncio.run(
            processor.process_frame(
                OutputAudioRawFrame(
                    audio=b"\x01\x00" * 20,
                    sample_rate=16000,
                    num_channels=1,
                ),
                FrameDirection.DOWNSTREAM,
            )
        )
        clock.advance(0.5)
        asyncio.run(
            processor.process_frame(BotStoppedSpeakingFrame(), FrameDirection.DOWNSTREAM)
        )
        clock.advance(1.0)
        asyncio.run(
            processor.process_frame(
                InputDTMFFrame(KeypadEntry.ONE),
                FrameDirection.DOWNSTREAM,
            )
        )
        clock.advance(1.0)
        asyncio.run(
            processor.process_frame(
                TranscriptionFrame(
                    text="ინტერნეტი არ მაქვს",
                    user_id="caller",
                    timestamp="2026-01-01T00:00:03Z",
                    finalized=True,
                ),
                FrameDirection.DOWNSTREAM,
            )
        )

        snapshot = policy.snapshot()
        self.assertFalse(snapshot.bot_speaking)
        self.assertEqual(snapshot.last_bot_activity_at_monotonic_s, 2.0)
        self.assertEqual(snapshot.last_user_activity_at_monotonic_s, 4.0)
