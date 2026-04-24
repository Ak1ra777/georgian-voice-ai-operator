from __future__ import annotations

"""Live session lifecycle and turn-loop execution helpers for call workers."""

import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, TypeVar


class SessionRuntimeState(str, Enum):
    """Observable lifecycle states for one call session."""

    ALLOCATED = "allocated"
    READY = "ready"
    IDENTIFIER_ENTRY = "identifier_entry"
    VERIFICATION = "verification"
    CONTEXT_LOAD = "context_load"
    ACTIVE = "active"
    ENDING = "ending"
    ENDED = "ended"


@dataclass(frozen=True)
class SessionRuntimeSnapshot:
    session_id: str
    state: SessionRuntimeState
    cancel_reason: str | None
    is_cancelled: bool
    state_changed_at_monotonic_s: float
    turn_index: int
    spoken_turns: int
    silent_turns: int


@dataclass(frozen=True)
class TurnExecutionOutcome:
    turn_number: int
    completed_turn: int
    spoke: bool
    retries: int


@dataclass(frozen=True)
class TurnLoopResult:
    exit_reason: str
    turn_index: int
    spoken_turns: int
    silent_turns: int


class TurnExecutionAborted(Exception):
    """Raised when the outer runtime aborts a turn loop on purpose."""


TSessionResources = TypeVar("TSessionResources")


class CancellationToken:
    """Thin wrapper around `threading.Event` used by the session runtime."""

    def __init__(self) -> None:
        self._event = threading.Event()

    @property
    def event(self) -> threading.Event:
        return self._event

    def cancel(self) -> None:
        self._event.set()

    def clear(self) -> None:
        self._event.clear()

    def is_cancelled(self) -> bool:
        return self._event.is_set()

    def wait(self, timeout: float | None = None) -> bool:
        return self._event.wait(timeout=timeout)


class SessionRuntime:
    """Owns one live call session's lifecycle, turn counters, and stop state."""

    def __init__(self, session_id: str) -> None:
        self.session_id = session_id
        self._state = SessionRuntimeState.ALLOCATED
        self._cancel_reason: str | None = None
        self._state_lock = threading.Lock()
        self._cancellation = CancellationToken()
        self._state_changed_at = time.monotonic()
        self._turn_index = 0
        self._spoken_turns = 0
        self._silent_turns = 0

    @property
    def stop_event(self) -> threading.Event:
        return self._cancellation.event

    @property
    def state(self) -> SessionRuntimeState:
        with self._state_lock:
            return self._state

    @property
    def cancel_reason(self) -> str | None:
        with self._state_lock:
            return self._cancel_reason

    @property
    def state_changed_at_monotonic_s(self) -> float:
        with self._state_lock:
            return self._state_changed_at

    def snapshot(self) -> SessionRuntimeSnapshot:
        with self._state_lock:
            return SessionRuntimeSnapshot(
                session_id=self.session_id,
                state=self._state,
                cancel_reason=self._cancel_reason,
                is_cancelled=self._cancellation.is_cancelled(),
                state_changed_at_monotonic_s=self._state_changed_at,
                turn_index=self._turn_index,
                spoken_turns=self._spoken_turns,
                silent_turns=self._silent_turns,
            )

    def _set_state(self, next_state: SessionRuntimeState) -> None:
        if self._state != next_state:
            self._state = next_state
            self._state_changed_at = time.monotonic()

    @property
    def turn_index(self) -> int:
        with self._state_lock:
            return self._turn_index

    @property
    def spoken_turns(self) -> int:
        with self._state_lock:
            return self._spoken_turns

    @property
    def silent_turns(self) -> int:
        with self._state_lock:
            return self._silent_turns

    def next_turn_number(self) -> int:
        with self._state_lock:
            return self._turn_index + 1

    def record_turn(self, spoke: bool) -> int:
        with self._state_lock:
            self._turn_index += 1
            if spoke:
                self._spoken_turns += 1
            else:
                self._silent_turns += 1
            return self._turn_index

    def execute_turn_with_retry(
        self,
        turn_fn: Callable[[], bool],
        classify_error_fn: Callable[[Exception], tuple[bool, str]],
        max_retry_attempts: int,
        retry_backoff_base_s: float,
        retry_backoff_max_s: float,
        should_abort_fn: Callable[[], bool],
        wait_abort_fn: Callable[[float], bool],
        on_retry_fn: Callable[[int, int, int, float, str, Exception], None] | None = None,
        on_recovered_fn: Callable[[int, int], None] | None = None,
        on_failure_fn: Callable[[int, int, bool, str, Exception], None] | None = None,
        passthrough_exceptions: tuple[type[BaseException], ...] = (),
    ) -> TurnExecutionOutcome:
        retry_limit = max(0, int(max_retry_attempts))
        retry_base_s = max(0.0, float(retry_backoff_base_s))
        retry_max_s = max(retry_base_s, float(retry_backoff_max_s))
        turn_number = self.next_turn_number()
        attempt = 0

        while True:
            if should_abort_fn():
                raise TurnExecutionAborted(
                    f"turn_execution_aborted:turn={turn_number}:phase=pre_attempt"
                )

            try:
                spoke = turn_fn()
                if attempt > 0 and on_recovered_fn is not None:
                    on_recovered_fn(turn_number, attempt)
                completed_turn = self.record_turn(spoke)
                return TurnExecutionOutcome(
                    turn_number=turn_number,
                    completed_turn=completed_turn,
                    spoke=spoke,
                    retries=attempt,
                )
            except Exception as ex:
                if passthrough_exceptions and isinstance(ex, passthrough_exceptions):
                    raise

                transient, error_class = classify_error_fn(ex)
                can_retry = transient and attempt < retry_limit
                if can_retry and not should_abort_fn():
                    attempt += 1
                    backoff_s = min(
                        retry_max_s,
                        retry_base_s * (2 ** (attempt - 1)),
                    )
                    if on_retry_fn is not None:
                        on_retry_fn(
                            turn_number,
                            attempt,
                            retry_limit,
                            backoff_s,
                            error_class,
                            ex,
                        )
                    if backoff_s > 0.0 and wait_abort_fn(backoff_s):
                        raise TurnExecutionAborted(
                            f"turn_execution_aborted:turn={turn_number}:phase=retry_backoff"
                        ) from ex
                    continue

                if on_failure_fn is not None:
                    on_failure_fn(
                        turn_number,
                        attempt,
                        transient,
                        error_class,
                        ex,
                    )
                raise

    def run_session(
        self,
        build_resources_fn: Callable[[], TSessionResources],
        run_resources_fn: Callable[[TSessionResources], None],
        cleanup_resources_fn: Callable[[TSessionResources], None],
        post_cleanup_fn: Callable[[TSessionResources], None] | None = None,
    ) -> None:
        resources: TSessionResources | None = None
        resources_built = False
        try:
            resources = build_resources_fn()
            resources_built = True
            try:
                run_resources_fn(resources)
            except TurnExecutionAborted:
                return
        finally:
            try:
                if resources_built and resources is not None:
                    cleanup_resources_fn(resources)
            finally:
                try:
                    if (
                        resources_built
                        and resources is not None
                        and post_cleanup_fn is not None
                    ):
                        post_cleanup_fn(resources)
                finally:
                    self.mark_ended("run_loop_exit")

    def run_turn_loop(
        self,
        should_stop_fn: Callable[[], bool],
        execute_turn_fn: Callable[[], TurnExecutionOutcome],
        max_duration_s: float | None,
        no_speech_timeout_s: float | None,
        on_max_duration_timeout_fn: Callable[[], None] | None = None,
        on_no_speech_timeout_fn: Callable[[], None] | None = None,
        on_periodic_fn: Callable[[], None] | None = None,
        periodic_interval_s: float = 30.0,
        monotonic_fn: Callable[[], float] = time.monotonic,
    ) -> TurnLoopResult:
        periodic_interval = max(0.1, float(periodic_interval_s))
        start_ts = monotonic_fn()
        last_activity_ts = start_ts
        next_periodic_at = start_ts + periodic_interval
        exit_reason = "stop_requested"
        self.mark_active()

        while not should_stop_fn():
            if max_duration_s is not None and monotonic_fn() - start_ts > max_duration_s:
                exit_reason = "max_duration_timeout"
                if on_max_duration_timeout_fn is not None:
                    on_max_duration_timeout_fn()
                break

            outcome = execute_turn_fn()
            if outcome.spoke:
                last_activity_ts = monotonic_fn()
            elif no_speech_timeout_s is not None:
                if monotonic_fn() - last_activity_ts > no_speech_timeout_s:
                    exit_reason = "no_speech_timeout"
                    if on_no_speech_timeout_fn is not None:
                        on_no_speech_timeout_fn()
                    break

            now = monotonic_fn()
            if on_periodic_fn is not None and now >= next_periodic_at:
                on_periodic_fn()
                next_periodic_at = now + periodic_interval

        return TurnLoopResult(
            exit_reason=exit_reason,
            turn_index=self.turn_index,
            spoken_turns=self.spoken_turns,
            silent_turns=self.silent_turns,
        )

    def begin(self) -> None:
        self.mark_ready()

    def mark_ready(self) -> None:
        with self._state_lock:
            self._set_state(SessionRuntimeState.READY)
            self._cancel_reason = None
            self._cancellation.clear()

    def mark_identifier_entry(self) -> None:
        with self._state_lock:
            if self._state == SessionRuntimeState.ENDED:
                return
            self._set_state(SessionRuntimeState.IDENTIFIER_ENTRY)

    def mark_verification(self) -> None:
        with self._state_lock:
            if self._state == SessionRuntimeState.ENDED:
                return
            self._set_state(SessionRuntimeState.VERIFICATION)

    def mark_context_load(self) -> None:
        with self._state_lock:
            if self._state == SessionRuntimeState.ENDED:
                return
            self._set_state(SessionRuntimeState.CONTEXT_LOAD)

    def mark_active(self) -> None:
        with self._state_lock:
            if self._state == SessionRuntimeState.ENDED:
                return
            self._set_state(SessionRuntimeState.ACTIVE)

    def request_stop(self, reason: str | None = None) -> None:
        with self._state_lock:
            if self._state != SessionRuntimeState.ENDED:
                self._set_state(SessionRuntimeState.ENDING)
            if reason and self._cancel_reason is None:
                self._cancel_reason = reason
            self._cancellation.cancel()

    def mark_ended(self, reason: str | None = None) -> None:
        with self._state_lock:
            self._set_state(SessionRuntimeState.ENDED)
            if reason and self._cancel_reason is None:
                self._cancel_reason = reason
            self._cancellation.cancel()

    def is_cancelled(self) -> bool:
        return self._cancellation.is_cancelled()

    def cancellation_check(self) -> bool:
        return self.is_cancelled()

    def wait_cancelled(self, timeout: float | None = None) -> bool:
        return self._cancellation.wait(timeout=timeout)
