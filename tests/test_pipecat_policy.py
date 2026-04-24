from __future__ import annotations

import unittest

from src.pipecat_runtime.policy import SessionPolicyController
from src.runtime.calls.session.runtime import SessionRuntime, SessionRuntimeState


class _FakeClock:
    def __init__(self, now: float = 0.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class SessionPolicyControllerTest(unittest.TestCase):
    def _new_controller(
        self,
        *,
        no_speech_timeout_s: float | None = 2.0,
        max_duration_s: float | None = 30.0,
    ) -> tuple[SessionPolicyController, SessionRuntime, _FakeClock]:
        runtime = SessionRuntime(session_id="sess-1")
        clock = _FakeClock()
        controller = SessionPolicyController(
            session_id="sess-1",
            runtime=runtime,
            no_speech_timeout_s=no_speech_timeout_s,
            max_duration_s=max_duration_s,
            clock=clock,
        )
        return controller, runtime, clock

    def test_mark_media_ready_marks_runtime_active(self) -> None:
        controller, runtime, _clock = self._new_controller()

        controller.mark_media_ready()

        self.assertEqual(runtime.state, SessionRuntimeState.ACTIVE)
        self.assertIsNotNone(controller.snapshot().media_ready_at_monotonic_s)

    def test_max_duration_timeout_is_measured_from_media_ready(self) -> None:
        controller, _runtime, clock = self._new_controller(
            no_speech_timeout_s=None,
            max_duration_s=5.0,
        )
        controller.mark_media_ready()

        clock.advance(4.9)
        self.assertIsNone(controller.check_timeouts())

        clock.advance(0.1)
        self.assertEqual(controller.check_timeouts(), "max_duration_timeout")

    def test_no_speech_timeout_waits_for_bot_to_finish(self) -> None:
        controller, _runtime, clock = self._new_controller(
            no_speech_timeout_s=2.0,
            max_duration_s=None,
        )
        controller.mark_media_ready()
        controller.record_bot_started()

        clock.advance(10.0)
        self.assertIsNone(controller.check_timeouts())

        controller.record_bot_stopped()
        clock.advance(1.9)
        self.assertIsNone(controller.check_timeouts())

        clock.advance(0.1)
        self.assertEqual(controller.check_timeouts(), "no_speech_timeout")

    def test_user_activity_resets_no_speech_deadline(self) -> None:
        controller, _runtime, clock = self._new_controller(
            no_speech_timeout_s=2.0,
            max_duration_s=None,
        )
        controller.mark_media_ready()

        clock.advance(1.5)
        controller.record_user_activity()
        clock.advance(1.5)
        self.assertIsNone(controller.check_timeouts())

        clock.advance(0.5)
        self.assertEqual(controller.check_timeouts(), "no_speech_timeout")
