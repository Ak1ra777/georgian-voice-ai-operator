from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch

from src.pipecat_runtime.call_manager import PipecatCallManager
from src.runtime.calls.session.runtime import SessionRuntime


class _FakePipecatCallWorker:
    def __init__(
        self,
        session_id: str,
        rx_port: int,
        tx_port: int,
        host: str,
        no_speech_timeout_s,
        max_duration_s,
        on_hangup_request,
        runtime=None,
    ) -> None:
        self.session_id = session_id
        self.rx_port = rx_port
        self.tx_port = tx_port
        self.host = host
        self.no_speech_timeout_s = no_speech_timeout_s
        self.max_duration_s = max_duration_s
        self.on_hangup_request = on_hangup_request
        self.runtime = runtime
        self.started = False
        self.stopped = False
        self.media_started_notified = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True

    def notify_media_started(self) -> None:
        self.media_started_notified = True


class PipecatCallManagerContractTest(unittest.TestCase):
    def test_call_allocated_starts_pipecat_worker_and_sends_ready(self) -> None:
        with patch(
            "src.pipecat_runtime.call_manager.PipecatCallWorker",
            _FakePipecatCallWorker,
        ):
            manager = PipecatCallManager(socket_path="/tmp/test.sock")
            manager._send_control = MagicMock(return_value=True)
            manager._handle_event(
                json.dumps(
                    {
                        "type": "call_allocated",
                        "session_id": "sess-1",
                        "rx_port": 41000,
                        "tx_port": 41001,
                    }
                )
            )

        self.assertIn("sess-1", manager._workers)
        self.assertIn("sess-1", manager._runtimes)
        worker = manager._workers["sess-1"]
        self.assertTrue(worker.started)
        self.assertIs(worker.runtime, manager._runtimes["sess-1"])
        self.assertEqual(worker.rx_port, 41000)
        self.assertEqual(worker.tx_port, 41001)
        snapshot = manager.runtime_snapshot("sess-1")
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot.session_id, "sess-1")
        manager._send_control.assert_called_once()
        sent = manager._send_control.call_args.args[0]
        self.assertEqual(sent["type"], "call_ready")
        self.assertEqual(sent["session_id"], "sess-1")

    def test_call_media_started_and_call_end_follow_same_worker_contract(self) -> None:
        manager = PipecatCallManager(socket_path="/tmp/test.sock")
        worker = _FakePipecatCallWorker(
            session_id="sess-1",
            rx_port=41000,
            tx_port=41001,
            host="127.0.0.1",
            no_speech_timeout_s=10.0,
            max_duration_s=3600.0,
            on_hangup_request=None,
            runtime=SessionRuntime(session_id="sess-1"),
        )
        manager._workers["sess-1"] = worker
        manager._runtimes["sess-1"] = worker.runtime

        manager._handle_event(
            json.dumps({"type": "call_media_started", "session_id": "sess-1"})
        )
        self.assertTrue(worker.media_started_notified)

        manager._handle_event(json.dumps({"type": "call_end", "session_id": "sess-1"}))
        self.assertTrue(worker.stopped)
        self.assertNotIn("sess-1", manager._workers)
        self.assertNotIn("sess-1", manager._runtimes)
