from __future__ import annotations

import threading
import time
import unittest

from src.runtime.calls.session.runtime import (
    SessionRuntime,
    SessionRuntimeSnapshot,
    SessionRuntimeState,
    TurnExecutionAborted,
    TurnExecutionOutcome,
    TurnLoopResult,
)


class SessionRuntimeTest(unittest.TestCase):
    def test_lifecycle_state_and_cancellation(self) -> None:
        runtime = SessionRuntime(session_id="s1")
        self.assertEqual(runtime.state, SessionRuntimeState.ALLOCATED)
        self.assertFalse(runtime.is_cancelled())
        self.assertIsNone(runtime.cancel_reason)

        runtime.begin()
        self.assertEqual(runtime.state, SessionRuntimeState.READY)
        self.assertFalse(runtime.is_cancelled())

        runtime.mark_active()
        self.assertEqual(runtime.state, SessionRuntimeState.ACTIVE)
        self.assertFalse(runtime.is_cancelled())

        runtime.request_stop("manual_stop")
        self.assertEqual(runtime.state, SessionRuntimeState.ENDING)
        self.assertTrue(runtime.is_cancelled())
        self.assertEqual(runtime.cancel_reason, "manual_stop")

        runtime.mark_ended("done")
        self.assertEqual(runtime.state, SessionRuntimeState.ENDED)
        self.assertTrue(runtime.is_cancelled())
        self.assertEqual(
            runtime.cancel_reason,
            "manual_stop",
            "first cancellation reason should be preserved",
        )

    def test_wait_cancelled_unblocks_after_request_stop(self) -> None:
        runtime = SessionRuntime(session_id="s2")
        runtime.begin()
        runtime.mark_active()

        def _stop_later() -> None:
            time.sleep(0.05)
            runtime.request_stop("timeout")

        stopper = threading.Thread(target=_stop_later, daemon=True)
        stopper.start()
        self.assertTrue(runtime.wait_cancelled(timeout=1.0))
        stopper.join(timeout=1.0)
        self.assertFalse(stopper.is_alive())
        self.assertEqual(runtime.cancel_reason, "timeout")

    def test_begin_resets_cancellation_for_restart(self) -> None:
        runtime = SessionRuntime(session_id="s3")
        runtime.begin()
        runtime.request_stop("first_stop")
        runtime.mark_ended()

        runtime.begin()
        self.assertEqual(runtime.state, SessionRuntimeState.READY)
        self.assertFalse(runtime.is_cancelled())
        self.assertIsNone(runtime.cancel_reason)

    def test_snapshot_reports_latest_state(self) -> None:
        runtime = SessionRuntime(session_id="s4")
        runtime.begin()
        runtime.mark_active()
        runtime.record_turn(spoke=True)
        runtime.record_turn(spoke=False)
        runtime.request_stop("end_call")
        snapshot = runtime.snapshot()
        self.assertIsInstance(snapshot, SessionRuntimeSnapshot)
        self.assertEqual(snapshot.session_id, "s4")
        self.assertEqual(snapshot.state, SessionRuntimeState.ENDING)
        self.assertTrue(snapshot.is_cancelled)
        self.assertEqual(snapshot.cancel_reason, "end_call")
        self.assertEqual(snapshot.turn_index, 2)
        self.assertEqual(snapshot.spoken_turns, 1)
        self.assertEqual(snapshot.silent_turns, 1)

    def test_runtime_tracks_turn_counters(self) -> None:
        runtime = SessionRuntime(session_id="s5")
        self.assertEqual(runtime.next_turn_number(), 1)
        self.assertEqual(runtime.record_turn(spoke=False), 1)
        self.assertEqual(runtime.next_turn_number(), 2)
        self.assertEqual(runtime.record_turn(spoke=True), 2)
        self.assertEqual(runtime.turn_index, 2)
        self.assertEqual(runtime.spoken_turns, 1)
        self.assertEqual(runtime.silent_turns, 1)

    def test_execute_turn_with_retry_recovers_and_records_turn(self) -> None:
        runtime = SessionRuntime(session_id="s6")
        calls = {"count": 0}
        retries: list[tuple[int, int, str]] = []
        recoveries: list[tuple[int, int]] = []

        def _turn_fn() -> bool:
            calls["count"] += 1
            if calls["count"] < 3:
                raise TimeoutError("transient timeout")
            return True

        def _classify(ex: Exception) -> tuple[bool, str]:
            return isinstance(ex, TimeoutError), ex.__class__.__name__

        outcome = runtime.execute_turn_with_retry(
            turn_fn=_turn_fn,
            classify_error_fn=_classify,
            max_retry_attempts=3,
            retry_backoff_base_s=0.0,
            retry_backoff_max_s=0.0,
            should_abort_fn=lambda: False,
            wait_abort_fn=lambda _timeout: False,
            on_retry_fn=lambda turn, attempt, _max_attempts, _backoff_s, error_class, _ex: retries.append(
                (turn, attempt, error_class)
            ),
            on_recovered_fn=lambda turn, retry_count: recoveries.append(
                (turn, retry_count)
            ),
        )

        self.assertIsInstance(outcome, TurnExecutionOutcome)
        self.assertEqual(outcome.turn_number, 1)
        self.assertEqual(outcome.completed_turn, 1)
        self.assertTrue(outcome.spoke)
        self.assertEqual(outcome.retries, 2)
        self.assertEqual(calls["count"], 3)
        self.assertEqual(retries, [(1, 1, "TimeoutError"), (1, 2, "TimeoutError")])
        self.assertEqual(recoveries, [(1, 2)])
        self.assertEqual(runtime.turn_index, 1)
        self.assertEqual(runtime.spoken_turns, 1)
        self.assertEqual(runtime.silent_turns, 0)

    def test_execute_turn_with_retry_aborts_during_backoff(self) -> None:
        runtime = SessionRuntime(session_id="s7")

        with self.assertRaises(TurnExecutionAborted):
            runtime.execute_turn_with_retry(
                turn_fn=lambda: (_ for _ in ()).throw(TimeoutError("retryable")),
                classify_error_fn=lambda ex: (isinstance(ex, TimeoutError), ex.__class__.__name__),
                max_retry_attempts=2,
                retry_backoff_base_s=0.1,
                retry_backoff_max_s=0.1,
                should_abort_fn=lambda: False,
                wait_abort_fn=lambda _timeout: True,
            )

        self.assertEqual(runtime.turn_index, 0)
        self.assertEqual(runtime.spoken_turns, 0)
        self.assertEqual(runtime.silent_turns, 0)

    def test_execute_turn_with_retry_passthrough_exception(self) -> None:
        runtime = SessionRuntime(session_id="s8")

        class _PassthroughError(RuntimeError):
            pass

        with self.assertRaises(_PassthroughError):
            runtime.execute_turn_with_retry(
                turn_fn=lambda: (_ for _ in ()).throw(_PassthroughError("stop")),
                classify_error_fn=lambda ex: (True, ex.__class__.__name__),
                max_retry_attempts=5,
                retry_backoff_base_s=0.0,
                retry_backoff_max_s=0.0,
                should_abort_fn=lambda: False,
                wait_abort_fn=lambda _timeout: False,
                passthrough_exceptions=(_PassthroughError,),
            )

        self.assertEqual(runtime.turn_index, 0)

    def test_run_turn_loop_stops_on_max_duration(self) -> None:
        runtime = SessionRuntime(session_id="s9")
        max_duration_hit = {"value": False}
        tick = {"t": 0.0}

        def _mono() -> float:
            tick["t"] += 0.25
            return tick["t"]

        def _execute_turn() -> TurnExecutionOutcome:
            turn_number = runtime.next_turn_number()
            completed = runtime.record_turn(spoke=False)
            return TurnExecutionOutcome(
                turn_number=turn_number,
                completed_turn=completed,
                spoke=False,
                retries=0,
            )

        result = runtime.run_turn_loop(
            should_stop_fn=lambda: False,
            execute_turn_fn=_execute_turn,
            max_duration_s=1.0,
            no_speech_timeout_s=None,
            on_max_duration_timeout_fn=lambda: max_duration_hit.__setitem__("value", True),
            monotonic_fn=_mono,
        )

        self.assertIsInstance(result, TurnLoopResult)
        self.assertEqual(result.exit_reason, "max_duration_timeout")
        self.assertTrue(max_duration_hit["value"])
        self.assertGreaterEqual(result.turn_index, 1)

    def test_run_turn_loop_stops_on_no_speech_timeout(self) -> None:
        runtime = SessionRuntime(session_id="s10")
        no_speech_hit = {"value": False}
        tick = {"t": 0.0}

        def _mono() -> float:
            tick["t"] += 0.2
            return tick["t"]

        def _execute_turn() -> TurnExecutionOutcome:
            turn_number = runtime.next_turn_number()
            completed = runtime.record_turn(spoke=False)
            return TurnExecutionOutcome(
                turn_number=turn_number,
                completed_turn=completed,
                spoke=False,
                retries=0,
            )

        result = runtime.run_turn_loop(
            should_stop_fn=lambda: False,
            execute_turn_fn=_execute_turn,
            max_duration_s=None,
            no_speech_timeout_s=0.3,
            on_no_speech_timeout_fn=lambda: no_speech_hit.__setitem__("value", True),
            monotonic_fn=_mono,
        )

        self.assertEqual(result.exit_reason, "no_speech_timeout")
        self.assertTrue(no_speech_hit["value"])
        self.assertGreaterEqual(result.silent_turns, 1)

    def test_run_turn_loop_stops_when_requested_without_executing_turn(self) -> None:
        runtime = SessionRuntime(session_id="s11")
        calls = {"count": 0}

        def _execute_turn() -> TurnExecutionOutcome:
            calls["count"] += 1
            return TurnExecutionOutcome(
                turn_number=1,
                completed_turn=1,
                spoke=False,
                retries=0,
            )

        result = runtime.run_turn_loop(
            should_stop_fn=lambda: True,
            execute_turn_fn=_execute_turn,
            max_duration_s=None,
            no_speech_timeout_s=None,
        )

        self.assertEqual(result.exit_reason, "stop_requested")
        self.assertEqual(calls["count"], 0)
        self.assertEqual(result.turn_index, 0)

    def test_run_turn_loop_emits_periodic_callback(self) -> None:
        runtime = SessionRuntime(session_id="s12")
        periodic_calls = {"count": 0}
        tick = {"t": 0.0}

        def _mono() -> float:
            tick["t"] += 0.6
            return tick["t"]

        def _execute_turn() -> TurnExecutionOutcome:
            turn_number = runtime.next_turn_number()
            completed = runtime.record_turn(spoke=True)
            return TurnExecutionOutcome(
                turn_number=turn_number,
                completed_turn=completed,
                spoke=True,
                retries=0,
            )

        result = runtime.run_turn_loop(
            should_stop_fn=lambda: runtime.turn_index >= 3,
            execute_turn_fn=_execute_turn,
            max_duration_s=None,
            no_speech_timeout_s=None,
            on_periodic_fn=lambda: periodic_calls.__setitem__("count", periodic_calls["count"] + 1),
            periodic_interval_s=1.0,
            monotonic_fn=_mono,
        )

        self.assertEqual(result.exit_reason, "stop_requested")
        self.assertEqual(result.turn_index, 3)
        self.assertGreaterEqual(periodic_calls["count"], 1)

    def test_run_session_success_orders_cleanup_and_marks_ended(self) -> None:
        runtime = SessionRuntime(session_id="s13")
        call_order: list[str] = []

        def _build() -> dict[str, int]:
            call_order.append("build")
            return {"v": 1}

        def _run(resources: dict[str, int]) -> None:
            _ = resources
            call_order.append("run")

        def _cleanup(resources: dict[str, int]) -> None:
            _ = resources
            call_order.append("cleanup")

        def _post_cleanup(resources: dict[str, int]) -> None:
            _ = resources
            call_order.append("post_cleanup")

        runtime.run_session(
            build_resources_fn=_build,
            run_resources_fn=_run,
            cleanup_resources_fn=_cleanup,
            post_cleanup_fn=_post_cleanup,
        )

        self.assertEqual(call_order, ["build", "run", "cleanup", "post_cleanup"])
        self.assertEqual(runtime.state, SessionRuntimeState.ENDED)

    def test_run_session_aborted_still_cleans_up_and_marks_ended(self) -> None:
        runtime = SessionRuntime(session_id="s14")
        call_order: list[str] = []

        def _build() -> object:
            call_order.append("build")
            return object()

        def _run(resources: object) -> None:
            _ = resources
            call_order.append("run")
            raise TurnExecutionAborted("stop")

        def _cleanup(resources: object) -> None:
            _ = resources
            call_order.append("cleanup")

        runtime.run_session(
            build_resources_fn=_build,
            run_resources_fn=_run,
            cleanup_resources_fn=_cleanup,
        )

        self.assertEqual(call_order, ["build", "run", "cleanup"])
        self.assertEqual(runtime.state, SessionRuntimeState.ENDED)

    def test_run_session_propagates_error_after_cleanup(self) -> None:
        runtime = SessionRuntime(session_id="s15")
        cleanup_called = {"value": False}

        def _build() -> object:
            return object()

        def _run(resources: object) -> None:
            _ = resources
            raise ValueError("boom")

        def _cleanup(resources: object) -> None:
            _ = resources
            cleanup_called["value"] = True

        with self.assertRaises(ValueError):
            runtime.run_session(
                build_resources_fn=_build,
                run_resources_fn=_run,
                cleanup_resources_fn=_cleanup,
            )

        self.assertTrue(cleanup_called["value"])
        self.assertEqual(runtime.state, SessionRuntimeState.ENDED)

    def test_run_session_build_failure_marks_ended(self) -> None:
        runtime = SessionRuntime(session_id="s16")

        def _build() -> object:
            raise RuntimeError("build failed")

        with self.assertRaises(RuntimeError):
            runtime.run_session(
                build_resources_fn=_build,
                run_resources_fn=lambda _resources: None,
                cleanup_resources_fn=lambda _resources: None,
            )

        self.assertEqual(runtime.state, SessionRuntimeState.ENDED)


if __name__ == "__main__":
    unittest.main()
