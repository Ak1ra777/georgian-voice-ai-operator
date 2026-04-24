"""Tests for DTMF key event parsing and routing."""

from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock

from src.runtime.calls.manager.protocol import (
    DtmfKeyEvent,
    GatewayTraceEvent,
    parse_event_line,
)
from src.runtime.calls.manager.event_router import ManagerEventRouter


class TestDtmfKeyEventParsing(unittest.TestCase):
    """parse_event_line handles dtmf_key messages correctly."""

    def test_valid_dtmf_key(self):
        line = json.dumps({"type": "dtmf_key", "session_id": "s1", "digit": "5"})
        event = parse_event_line(line)
        self.assertIsInstance(event, DtmfKeyEvent)
        self.assertEqual(event.session_id, "s1")
        self.assertEqual(event.digit, "5")

    def test_star_digit(self):
        line = json.dumps({"type": "dtmf_key", "session_id": "s1", "digit": "*"})
        event = parse_event_line(line)
        self.assertIsInstance(event, DtmfKeyEvent)
        self.assertEqual(event.digit, "*")

    def test_hash_digit(self):
        line = json.dumps({"type": "dtmf_key", "session_id": "s1", "digit": "#"})
        event = parse_event_line(line)
        self.assertIsInstance(event, DtmfKeyEvent)
        self.assertEqual(event.digit, "#")

    def test_missing_session_id(self):
        line = json.dumps({"type": "dtmf_key", "digit": "5"})
        event = parse_event_line(line)
        self.assertIsNone(event)

    def test_missing_digit(self):
        line = json.dumps({"type": "dtmf_key", "session_id": "s1"})
        event = parse_event_line(line)
        self.assertIsNone(event)

    def test_empty_digit(self):
        line = json.dumps({"type": "dtmf_key", "session_id": "s1", "digit": ""})
        event = parse_event_line(line)
        self.assertIsNone(event)

    def test_empty_session_id(self):
        line = json.dumps({"type": "dtmf_key", "session_id": "", "digit": "5"})
        event = parse_event_line(line)
        self.assertIsNone(event)

    def test_gateway_trace_event(self):
        line = json.dumps(
            {
                "type": "gateway_trace",
                "session_id": "s1",
                "event": "call_end_requested",
                "level": "warning",
                "call_id": 9,
                "ts_ms": 1234,
                "data": {"reason": "python_hangup:no_speech_timeout"},
            }
        )
        event = parse_event_line(line)
        self.assertIsInstance(event, GatewayTraceEvent)
        self.assertEqual(event.session_id, "s1")
        self.assertEqual(event.event, "call_end_requested")
        self.assertEqual(event.level, "warning")
        self.assertEqual(event.call_id, 9)
        self.assertEqual(event.ts_ms, 1234)
        self.assertEqual(event.data, {"reason": "python_hangup:no_speech_timeout"})


class TestDtmfEventRouting(unittest.TestCase):
    """ManagerEventRouter forwards dtmf_key to the correct worker."""

    def _build_router(self, *, workers: dict[str, object] | None = None):
        registry = MagicMock()
        registry.get_worker.side_effect = lambda sid: (workers or {}).get(sid)
        command_queue = MagicMock()
        settings = MagicMock()
        return ManagerEventRouter(
            settings=settings,
            registry=registry,
            command_queue=command_queue,
        )

    def test_routes_to_worker(self):
        worker = MagicMock()
        router = self._build_router(workers={"s1": worker})
        line = json.dumps({"type": "dtmf_key", "session_id": "s1", "digit": "7"})
        router.handle_line(line)
        worker.on_dtmf_digit.assert_called_once_with("7")

    def test_ignores_unknown_session(self):
        router = self._build_router(workers={})
        line = json.dumps({"type": "dtmf_key", "session_id": "unknown", "digit": "1"})
        # Should not raise
        router.handle_line(line)

    def test_worker_without_on_dtmf_digit(self):
        worker = object()  # no on_dtmf_digit attr
        router = self._build_router(workers={"s1": worker})
        line = json.dumps({"type": "dtmf_key", "session_id": "s1", "digit": "1"})
        # Should not raise
        router.handle_line(line)

    def test_routes_gateway_trace_to_worker(self):
        worker = MagicMock()
        router = self._build_router(workers={"s1": worker})
        line = json.dumps(
            {
                "type": "gateway_trace",
                "session_id": "s1",
                "event": "call_ready",
                "call_id": 42,
                "ts_ms": 1000,
            }
        )
        router.handle_line(line)
        worker.record_gateway_trace.assert_called_once_with(
            event="call_ready",
            level="info",
            ts_ms=1000,
            call_id=42,
            data=None,
        )


if __name__ == "__main__":
    unittest.main()
