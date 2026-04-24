from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock

from pipecat.frames.frames import TranscriptionFrame
from pipecat.processors.frame_processor import FrameDirection

from src.pipecat_runtime.dtmf_capture import DtmfCollectionCoordinator, DtmfCollectionRequest
from src.pipecat_runtime.processors.dtmf_speech_guard import DtmfSpeechGuardProcessor


async def _noop() -> None:
    return None


class DtmfSpeechGuardProcessorTest(unittest.TestCase):
    def test_guard_swallows_hold_request_during_active_collection(self) -> None:
        coordinator = DtmfCollectionCoordinator()
        asyncio.run(
            coordinator.start(
                DtmfCollectionRequest(
                    kind="subscriber_identifier",
                    submit_key="#",
                    on_complete=lambda value: _noop(),
                    on_cancel=_noop,
                )
            )
        )
        events: list[tuple[str, dict[str, object]]] = []
        processor = DtmfSpeechGuardProcessor(
            coordinator,
            event_recorder=lambda event, data: events.append((event, data)),
            name="dtmf-speech-guard",
        )
        processor.push_frame = AsyncMock()

        asyncio.run(
            processor.process_frame(
                TranscriptionFrame(
                    text="ორი წამი, ორი წამი",
                    user_id="caller-1",
                    timestamp="2026-04-20T10:00:00Z",
                    finalized=True,
                ),
                FrameDirection.DOWNSTREAM,
            )
        )

        processor.push_frame.assert_not_called()
        self.assertEqual(
            events,
            [
                (
                    "speech_ignored_during_dtmf_collection",
                    {
                        "kind": "subscriber_identifier",
                        "reason": "hold_request",
                        "is_final": True,
                        "chars": len("ორი წამი, ორი წამი"),
                    },
                )
            ],
        )

    def test_guard_allows_non_hold_transcription_through(self) -> None:
        coordinator = DtmfCollectionCoordinator()
        asyncio.run(
            coordinator.start(
                DtmfCollectionRequest(
                    kind="subscriber_identifier",
                    submit_key="#",
                    on_complete=lambda value: _noop(),
                    on_cancel=_noop,
                )
            )
        )
        processor = DtmfSpeechGuardProcessor(
            coordinator,
            name="dtmf-speech-guard",
        )
        processor.push_frame = AsyncMock()
        frame = TranscriptionFrame(
            text="ადამიანი მინდა",
            user_id="caller-1",
            timestamp="2026-04-20T10:00:00Z",
            finalized=True,
        )

        asyncio.run(
            processor.process_frame(
                frame,
                FrameDirection.DOWNSTREAM,
            )
        )

        processor.push_frame.assert_awaited_once_with(frame, FrameDirection.DOWNSTREAM)
