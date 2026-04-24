from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass
from typing import Callable

from src.runtime.calls.session.runtime import SessionRuntime


@dataclass(frozen=True)
class SessionPolicySnapshot:
    media_ready_at_monotonic_s: float | None
    last_user_activity_at_monotonic_s: float | None
    last_bot_activity_at_monotonic_s: float | None
    bot_speaking: bool
    pending_hangup_reason: str | None


class SessionPolicyController:
    """Track session activity and request hangup when call policies are exceeded."""

    def __init__(
        self,
        *,
        session_id: str,
        runtime: SessionRuntime,
        no_speech_timeout_s: float | None,
        max_duration_s: float | None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.session_id = session_id
        self._runtime = runtime
        self._no_speech_timeout_s = no_speech_timeout_s
        self._max_duration_s = max_duration_s
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        self._media_ready_at: float | None = None
        self._last_user_activity_at: float | None = None
        self._last_bot_activity_at: float | None = None
        self._bot_speaking = False
        self._pending_hangup_reason: str | None = None

    def mark_media_ready(self) -> None:
        with self._lock:
            if self._media_ready_at is None:
                self._media_ready_at = self._clock()
        self._runtime.mark_active()

    def record_user_activity(self) -> None:
        with self._lock:
            self._last_user_activity_at = self._clock()

    def record_bot_started(self) -> None:
        with self._lock:
            now = self._clock()
            self._bot_speaking = True
            self._last_bot_activity_at = now

    def record_bot_activity(self) -> None:
        with self._lock:
            self._last_bot_activity_at = self._clock()

    def record_bot_stopped(self) -> None:
        with self._lock:
            now = self._clock()
            self._bot_speaking = False
            self._last_bot_activity_at = now

    def snapshot(self) -> SessionPolicySnapshot:
        with self._lock:
            return SessionPolicySnapshot(
                media_ready_at_monotonic_s=self._media_ready_at,
                last_user_activity_at_monotonic_s=self._last_user_activity_at,
                last_bot_activity_at_monotonic_s=self._last_bot_activity_at,
                bot_speaking=self._bot_speaking,
                pending_hangup_reason=self._pending_hangup_reason,
            )

    def check_timeouts(self, now: float | None = None) -> str | None:
        current = self._clock() if now is None else now
        with self._lock:
            if self._pending_hangup_reason is not None:
                return self._pending_hangup_reason

            if self._media_ready_at is None:
                return None

            if (
                self._max_duration_s is not None
                and self._max_duration_s > 0.0
                and (current - self._media_ready_at) >= self._max_duration_s
            ):
                self._pending_hangup_reason = "max_duration_timeout"
                return self._pending_hangup_reason

            if (
                self._no_speech_timeout_s is None
                or self._no_speech_timeout_s <= 0.0
                or self._bot_speaking
            ):
                return None

            last_activity_at = self._media_ready_at
            if self._last_user_activity_at is not None:
                last_activity_at = max(last_activity_at, self._last_user_activity_at)
            if self._last_bot_activity_at is not None:
                last_activity_at = max(last_activity_at, self._last_bot_activity_at)

            if (current - last_activity_at) >= self._no_speech_timeout_s:
                self._pending_hangup_reason = "no_speech_timeout"
                return self._pending_hangup_reason

            return None

    async def monitor(
        self,
        *,
        request_hangup_fn: Callable[[str], None],
        should_stop_fn: Callable[[], bool],
        poll_interval_s: float = 0.1,
    ) -> str | None:
        while not should_stop_fn():
            reason = self.check_timeouts()
            if reason is not None:
                request_hangup_fn(reason)
                return reason
            await asyncio.sleep(poll_interval_s)
        return None
