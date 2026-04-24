from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from pipecat.frames.frames import (
    AggregatedTextFrame,
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    CancelFrame,
    EndFrame,
    InterruptionFrame,
    LLMFullResponseStartFrame,
    StopFrame,
    TTSAudioRawFrame,
    TTSStartedFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor


@dataclass(frozen=True)
class TurnTimingSnapshot:
    turns_started: int
    turns_archived: int
    turns_with_llm_start: int
    turns_with_first_audio: int
    interruptions: int
    last_turn_id: int | None
    last_archive_reason: str | None
    last_user_speech_ms: int | None
    last_llm_started_after_user_stop_ms: int | None
    last_first_tts_text_after_user_stop_ms: int | None
    last_tts_started_after_user_stop_ms: int | None
    last_first_audio_after_user_stop_ms: int | None
    last_first_audio_after_llm_start_ms: int | None
    last_bot_started_after_user_stop_ms: int | None
    last_bot_speaking_ms: int | None
    avg_llm_started_after_user_stop_ms: int | None
    max_llm_started_after_user_stop_ms: int | None
    avg_first_audio_after_user_stop_ms: int | None
    max_first_audio_after_user_stop_ms: int | None


@dataclass(frozen=True)
class ArchivedTurn:
    turn_id: int
    reason: str
    spoke: bool
    user_speech_ms: int | None
    had_llm_start: bool
    had_first_audio: bool


class TurnTimingProcessor(FrameProcessor):
    """Emit concise per-turn timing markers for live-call latency triage."""

    def __init__(
        self,
        session_id: str,
        *,
        clock: Callable[[], float] | None = None,
        logger: Callable[[str], None] | None = None,
        event_recorder: Callable[[str, dict[str, Any]], None] | None = None,
        turn_archived_callback: Callable[[ArchivedTurn], None] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(enable_direct_mode=True, **kwargs)
        self._session_id = session_id
        self._now = clock or time.perf_counter
        self._logger = logger or print
        self._event_recorder = event_recorder
        self._turn_archived_callback = turn_archived_callback
        self._turn_id = 0
        self._active_turn_id: int | None = None
        self._user_started_at: float | None = None
        self._user_stopped_at: float | None = None
        self._llm_started_at: float | None = None
        self._first_tts_text_at: float | None = None
        self._tts_started_at: float | None = None
        self._first_audio_at: float | None = None
        self._bot_started_at: float | None = None
        self._turns_archived = 0
        self._turns_with_llm_start = 0
        self._turns_with_first_audio = 0
        self._interruptions = 0
        self._llm_started_after_user_stop_total_ms = 0
        self._first_audio_after_user_stop_total_ms = 0
        self._max_llm_started_after_user_stop_ms: int | None = None
        self._max_first_audio_after_user_stop_ms: int | None = None
        self._last_turn_id: int | None = None
        self._last_archive_reason: str | None = None
        self._last_user_speech_ms: int | None = None
        self._last_llm_started_after_user_stop_ms: int | None = None
        self._last_first_tts_text_after_user_stop_ms: int | None = None
        self._last_tts_started_after_user_stop_ms: int | None = None
        self._last_first_audio_after_user_stop_ms: int | None = None
        self._last_first_audio_after_llm_start_ms: int | None = None
        self._last_bot_started_after_user_stop_ms: int | None = None
        self._last_bot_speaking_ms: int | None = None

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if direction == FrameDirection.DOWNSTREAM:
            self._handle_frame(frame)

        await self.push_frame(frame, direction)

    def _handle_frame(self, frame) -> None:
        now = self._now()

        if isinstance(frame, UserStartedSpeakingFrame):
            if self._active_turn_id is not None:
                self._archive_current_turn("superseded", now)
            self._turn_id += 1
            self._active_turn_id = self._turn_id
            self._reset_turn_response_state()
            self._user_started_at = now
            self._log("user_started")
            return

        if isinstance(frame, UserStoppedSpeakingFrame):
            self._user_stopped_at = now
            self._llm_started_at = None
            self._first_tts_text_at = None
            self._tts_started_at = None
            self._first_audio_at = None
            self._bot_started_at = None
            self._log(
                "user_stopped",
                user_speech_ms=self._elapsed_ms(self._user_started_at, now),
            )
            return

        if isinstance(frame, LLMFullResponseStartFrame):
            if self._llm_started_at is None:
                self._llm_started_at = now
                self._log(
                    "llm_started",
                    since_user_stop_ms=self._elapsed_ms(self._user_stopped_at, now),
                )
            return

        if isinstance(frame, AggregatedTextFrame):
            if self._first_tts_text_at is None and frame.text.strip():
                self._first_tts_text_at = now
                self._log(
                    "first_tts_text",
                    since_user_stop_ms=self._elapsed_ms(self._user_stopped_at, now),
                    since_llm_start_ms=self._elapsed_ms(self._llm_started_at, now),
                    aggregation=str(frame.aggregated_by),
                    chars=len(frame.text),
                )
            return

        if isinstance(frame, TTSStartedFrame):
            if self._tts_started_at is None:
                self._tts_started_at = now
                self._log(
                    "tts_started",
                    since_user_stop_ms=self._elapsed_ms(self._user_stopped_at, now),
                    since_llm_start_ms=self._elapsed_ms(self._llm_started_at, now),
                )
            return

        if isinstance(frame, TTSAudioRawFrame):
            if self._first_audio_at is None and frame.audio:
                self._first_audio_at = now
                self._log(
                    "first_audio",
                    since_user_stop_ms=self._elapsed_ms(self._user_stopped_at, now),
                    since_llm_start_ms=self._elapsed_ms(self._llm_started_at, now),
                    since_first_tts_text_ms=self._elapsed_ms(
                        self._first_tts_text_at, now
                    ),
                    bytes=len(frame.audio),
                    sample_rate=frame.sample_rate,
                )
            return

        if isinstance(frame, BotStartedSpeakingFrame):
            if self._bot_started_at is None:
                self._bot_started_at = now
                self._log(
                    "bot_started",
                    since_user_stop_ms=self._elapsed_ms(self._user_stopped_at, now),
                    since_first_audio_ms=self._elapsed_ms(self._first_audio_at, now),
                )
            return

        if isinstance(frame, BotStoppedSpeakingFrame):
            if self._bot_started_at is not None:
                self._log(
                    "bot_stopped",
                    bot_speaking_ms=self._elapsed_ms(self._bot_started_at, now),
                )
            self._archive_current_turn("bot_stopped", now)
            return

        if isinstance(frame, InterruptionFrame):
            self._interruptions += 1
            self._log(
                "interruption",
                since_user_stop_ms=self._elapsed_ms(self._user_stopped_at, now),
            )
            return

        if isinstance(frame, (EndFrame, CancelFrame, StopFrame)):
            self._archive_current_turn("terminal", now)

    def set_turn_archived_callback(
        self,
        callback: Callable[[ArchivedTurn], None] | None,
    ) -> None:
        self._turn_archived_callback = callback

    def snapshot(self) -> TurnTimingSnapshot:
        return TurnTimingSnapshot(
            turns_started=self._turn_id,
            turns_archived=self._turns_archived,
            turns_with_llm_start=self._turns_with_llm_start,
            turns_with_first_audio=self._turns_with_first_audio,
            interruptions=self._interruptions,
            last_turn_id=self._last_turn_id,
            last_archive_reason=self._last_archive_reason,
            last_user_speech_ms=self._last_user_speech_ms,
            last_llm_started_after_user_stop_ms=self._last_llm_started_after_user_stop_ms,
            last_first_tts_text_after_user_stop_ms=self._last_first_tts_text_after_user_stop_ms,
            last_tts_started_after_user_stop_ms=self._last_tts_started_after_user_stop_ms,
            last_first_audio_after_user_stop_ms=self._last_first_audio_after_user_stop_ms,
            last_first_audio_after_llm_start_ms=self._last_first_audio_after_llm_start_ms,
            last_bot_started_after_user_stop_ms=self._last_bot_started_after_user_stop_ms,
            last_bot_speaking_ms=self._last_bot_speaking_ms,
            avg_llm_started_after_user_stop_ms=self._avg_or_none(
                self._llm_started_after_user_stop_total_ms,
                self._turns_with_llm_start,
            ),
            max_llm_started_after_user_stop_ms=self._max_llm_started_after_user_stop_ms,
            avg_first_audio_after_user_stop_ms=self._avg_or_none(
                self._first_audio_after_user_stop_total_ms,
                self._turns_with_first_audio,
            ),
            max_first_audio_after_user_stop_ms=self._max_first_audio_after_user_stop_ms,
        )

    def _reset_turn_response_state(self) -> None:
        self._user_started_at = None
        self._user_stopped_at = None
        self._llm_started_at = None
        self._first_tts_text_at = None
        self._tts_started_at = None
        self._first_audio_at = None
        self._bot_started_at = None

    def _archive_current_turn(self, reason: str, now: float) -> None:
        if self._active_turn_id is None:
            self._reset_turn_response_state()
            return

        user_speech_ms = self._elapsed_int_ms(self._user_started_at, self._user_stopped_at)
        had_llm_start = self._llm_started_at is not None
        had_first_audio = self._first_audio_at is not None
        llm_started_after_user_stop_ms = self._elapsed_int_ms(
            self._user_stopped_at,
            self._llm_started_at,
        )
        first_tts_text_after_user_stop_ms = self._elapsed_int_ms(
            self._user_stopped_at,
            self._first_tts_text_at,
        )
        tts_started_after_user_stop_ms = self._elapsed_int_ms(
            self._user_stopped_at,
            self._tts_started_at,
        )
        first_audio_after_user_stop_ms = self._elapsed_int_ms(
            self._user_stopped_at,
            self._first_audio_at,
        )
        first_audio_after_llm_start_ms = self._elapsed_int_ms(
            self._llm_started_at,
            self._first_audio_at,
        )
        bot_started_after_user_stop_ms = self._elapsed_int_ms(
            self._user_stopped_at,
            self._bot_started_at,
        )
        bot_speaking_ms = self._elapsed_int_ms(self._bot_started_at, now)

        self._turns_archived += 1
        self._last_turn_id = self._active_turn_id
        self._last_archive_reason = reason
        self._last_user_speech_ms = user_speech_ms
        self._last_llm_started_after_user_stop_ms = llm_started_after_user_stop_ms
        self._last_first_tts_text_after_user_stop_ms = (
            first_tts_text_after_user_stop_ms
        )
        self._last_tts_started_after_user_stop_ms = tts_started_after_user_stop_ms
        self._last_first_audio_after_user_stop_ms = first_audio_after_user_stop_ms
        self._last_first_audio_after_llm_start_ms = first_audio_after_llm_start_ms
        self._last_bot_started_after_user_stop_ms = bot_started_after_user_stop_ms
        self._last_bot_speaking_ms = bot_speaking_ms

        if llm_started_after_user_stop_ms is not None:
            self._turns_with_llm_start += 1
            self._llm_started_after_user_stop_total_ms += llm_started_after_user_stop_ms
            self._max_llm_started_after_user_stop_ms = self._max_metric(
                self._max_llm_started_after_user_stop_ms,
                llm_started_after_user_stop_ms,
            )

        if first_audio_after_user_stop_ms is not None:
            self._turns_with_first_audio += 1
            self._first_audio_after_user_stop_total_ms += (
                first_audio_after_user_stop_ms
            )
            self._max_first_audio_after_user_stop_ms = self._max_metric(
                self._max_first_audio_after_user_stop_ms,
                first_audio_after_user_stop_ms,
            )

        if self._turn_archived_callback is not None:
            try:
                self._turn_archived_callback(
                    ArchivedTurn(
                        turn_id=self._active_turn_id,
                        reason=reason,
                        spoke=(user_speech_ms or 0) > 0,
                        user_speech_ms=user_speech_ms,
                        had_llm_start=had_llm_start,
                        had_first_audio=had_first_audio,
                    )
                )
            except Exception:
                pass

        self._active_turn_id = None
        self._reset_turn_response_state()

    def _log(self, event: str, **fields) -> None:
        parts = [
            "pipecat_turn_timing",
            f"session_id={self._session_id}",
            f"turn_id={self._turn_id}",
            f"event={event}",
        ]
        for key, value in fields.items():
            if value is None:
                continue
            if isinstance(value, float):
                parts.append(f"{key}={value:.1f}")
            else:
                parts.append(f"{key}={value}")
        if self._event_recorder is not None:
            payload: dict[str, Any] = {}
            for key, value in fields.items():
                if value is None:
                    continue
                payload[key] = round(value, 1) if isinstance(value, float) else value
            try:
                self._event_recorder(
                    event,
                    {
                        "turn_id": self._turn_id,
                        **payload,
                    },
                )
            except Exception:
                pass
        self._logger(" ".join(parts))

    @staticmethod
    def _elapsed_ms(start: float | None, end: float) -> float | None:
        if start is None:
            return None
        return max(0.0, (end - start) * 1000.0)

    @classmethod
    def _elapsed_int_ms(cls, start: float | None, end: float | None) -> int | None:
        if start is None or end is None:
            return None
        elapsed = cls._elapsed_ms(start, end)
        if elapsed is None:
            return None
        return int(round(elapsed))

    @staticmethod
    def _avg_or_none(total: int, count: int) -> int | None:
        if count <= 0:
            return None
        return int(round(total / count))

    @staticmethod
    def _max_metric(current: int | None, candidate: int) -> int:
        if current is None:
            return candidate
        return max(current, candidate)
