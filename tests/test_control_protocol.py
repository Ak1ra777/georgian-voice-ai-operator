from __future__ import annotations

import json
import os
import socket
import struct
import time
import unittest
from unittest.mock import MagicMock, patch

from src.runtime.calls.call_manager import CallManager
from src.runtime.calls.session.runtime import SessionRuntime


class _FakeConn:
    def __init__(
        self,
        recv_chunks: list[bytes] | None = None,
        peer_pid: int = 1234,
        peer_uid: int | None = None,
        peer_gid: int = 1234,
    ) -> None:
        self._recv_chunks = list(recv_chunks or [])
        self.closed = False
        self.sent_payloads: list[bytes] = []
        self.raise_on_send = False
        self.timeout_values: list[float] = []
        self.peer_pid = peer_pid
        self.peer_uid = os.getuid() if peer_uid is None else peer_uid
        self.peer_gid = peer_gid

    def recv(self, size: int) -> bytes:
        _ = size
        if not self._recv_chunks:
            return b""
        return self._recv_chunks.pop(0)

    def sendall(self, data: bytes) -> None:
        if self.raise_on_send:
            raise OSError("send failed")
        self.sent_payloads.append(data)

    def settimeout(self, value: float) -> None:
        self.timeout_values.append(value)

    def getsockopt(self, level: int, optname: int, buflen: int = 0) -> bytes:
        _ = level
        _ = buflen
        if hasattr(socket, "SO_PEERCRED") and optname == socket.SO_PEERCRED:
            return struct.pack("3i", self.peer_pid, self.peer_uid, self.peer_gid)
        raise OSError("unsupported getsockopt")

    def close(self) -> None:
        self.closed = True


class _FakeServerSocket:
    def __init__(self, accept_actions: list[object]) -> None:
        self.accept_actions = list(accept_actions)
        self.bind_addr = None
        self.listen_backlog = None
        self.timeout_values: list[float | None] = []
        self.accept_calls = 0
        self.closed = False

    def bind(self, addr: str) -> None:
        self.bind_addr = addr

    def listen(self, backlog: int) -> None:
        self.listen_backlog = backlog

    def settimeout(self, value) -> None:
        self.timeout_values.append(value)

    def close(self) -> None:
        self.closed = True

    def accept(self):
        self.accept_calls += 1
        if not self.accept_actions:
            raise RuntimeError("Unexpected accept()")
        action = self.accept_actions.pop(0)
        if isinstance(action, BaseException):
            raise action
        return action


class _FakeCallWorker:
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


class CallManagerControlProtocolTest(unittest.TestCase):
    def _decode_sent_payloads(self, conn: _FakeConn) -> list[dict]:
        return [json.loads(payload.decode("utf-8").strip()) for payload in conn.sent_payloads]

    def test_handle_event_call_allocated_starts_worker_and_sends_ready(self) -> None:
        manager = CallManager(
            socket_path="/tmp/test.sock",
            worker_factory_provider=lambda: _FakeCallWorker,
        )
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
        self.assertEqual(sent["seq"], "1")

    def test_handle_event_duplicate_call_allocated_is_ignored(self) -> None:
        worker_ctor = MagicMock(side_effect=_FakeCallWorker)
        manager = CallManager(
            socket_path="/tmp/test.sock",
            worker_factory_provider=lambda: worker_ctor,
        )
        manager._send_control = MagicMock(return_value=True)
        payload = json.dumps(
            {
                "type": "call_allocated",
                "session_id": "sess-1",
                "rx_port": 41000,
                "tx_port": 41001,
            }
        )
        manager._handle_event(payload)
        manager._handle_event(payload)
        self.assertEqual(worker_ctor.call_count, 1)
        manager._send_control.assert_called_once()

    def test_handle_event_call_end_stops_worker(self) -> None:
        manager = CallManager(socket_path="/tmp/test.sock")
        worker = _FakeCallWorker(
            session_id="sess-1",
            rx_port=41000,
            tx_port=41001,
            host="127.0.0.1",
            no_speech_timeout_s=10,
            max_duration_s=3600,
            on_hangup_request=None,
        )
        manager._runtimes["sess-1"] = SessionRuntime(session_id="sess-1")
        manager._workers["sess-1"] = worker

        manager._handle_event(json.dumps({"type": "call_end", "session_id": "sess-1"}))

        self.assertTrue(worker.stopped)
        self.assertNotIn("sess-1", manager._workers)
        self.assertNotIn("sess-1", manager._runtimes)

    def test_handle_event_call_media_started_notifies_worker(self) -> None:
        manager = CallManager(socket_path="/tmp/test.sock")
        worker = _FakeCallWorker(
            session_id="sess-1",
            rx_port=41000,
            tx_port=41001,
            host="127.0.0.1",
            no_speech_timeout_s=10,
            max_duration_s=3600,
            on_hangup_request=None,
        )
        manager._workers["sess-1"] = worker

        manager._handle_event(
            json.dumps({"type": "call_media_started", "session_id": "sess-1"})
        )

        self.assertTrue(worker.media_started_notified)

    def test_handle_event_invalid_json_is_ignored(self) -> None:
        worker_ctor = MagicMock(side_effect=_FakeCallWorker)
        manager = CallManager(
            socket_path="/tmp/test.sock",
            worker_factory_provider=lambda: worker_ctor,
        )
        manager._send_control = MagicMock(return_value=True)
        manager._handle_event("not-json")
        self.assertEqual(worker_ctor.call_count, 0)
        manager._send_control.assert_not_called()

    def test_handle_connection_splits_fragmented_newline_delimited_messages(self) -> None:
        manager = CallManager(socket_path="/tmp/test.sock")
        conn = _FakeConn(
            recv_chunks=[
                b'{"type":"a"',
                b'}\n{"type":"b"}\n',
                b"",
            ]
        )
        manager._handle_event = MagicMock()

        manager._handle_connection(conn)

        manager._handle_event.assert_any_call('{"type":"a"}')
        manager._handle_event.assert_any_call('{"type":"b"}')
        self.assertEqual(manager._handle_event.call_count, 2)
        self.assertIsNone(manager._conn)
        self.assertTrue(conn.timeout_values)

    def test_authorized_peer_is_accepted_by_uid(self) -> None:
        allowed_uid = 2001
        manager = CallManager(
            socket_path="/tmp/test.sock",
            control_allowed_peer_uids={allowed_uid},
        )
        conn = _FakeConn(peer_uid=allowed_uid)
        self.assertTrue(manager._is_authorized_control_peer(conn))

    def test_unauthorized_peer_is_rejected_by_uid(self) -> None:
        manager = CallManager(
            socket_path="/tmp/test.sock",
            control_allowed_peer_uids={2001},
        )
        conn = _FakeConn(peer_uid=2002)
        self.assertFalse(manager._is_authorized_control_peer(conn))

    def test_wait_for_reconnect_skips_unauthorized_peer(self) -> None:
        manager = CallManager(
            socket_path="/tmp/test.sock",
            control_allowed_peer_uids={2001},
        )
        bad_conn = _FakeConn(peer_uid=2002)
        good_conn = _FakeConn(peer_uid=2001)
        server = _FakeServerSocket([(bad_conn, None), (good_conn, None)])

        conn = manager._wait_for_reconnect(server, grace_seconds=1.0)

        self.assertIs(conn, good_conn)
        self.assertTrue(bad_conn.closed)

    def test_wait_for_reconnect_keeps_waiting_until_grace_window_expires(self) -> None:
        manager = CallManager(socket_path="/tmp/test.sock")
        reconnect_conn = _FakeConn()
        server = _FakeServerSocket([socket.timeout(), (reconnect_conn, None)])

        conn = manager._wait_for_reconnect(server, grace_seconds=1.0)

        self.assertIs(conn, reconnect_conn)
        self.assertEqual(server.accept_calls, 2)

    def test_call_ready_retries_until_ack(self) -> None:
        manager = CallManager(
            socket_path="/tmp/test.sock",
            control_cmd_ack_timeout_s=0.5,
            control_cmd_max_retries=2,
            worker_factory_provider=lambda: _FakeCallWorker,
        )
        conn = _FakeConn()
        manager._conn = conn

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

        sent = self._decode_sent_payloads(conn)
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0]["type"], "call_ready")
        self.assertEqual(sent[0]["session_id"], "sess-1")

        seq = sent[0]["seq"]
        manager._handle_event(
            json.dumps(
                {
                    "type": "control_ack",
                    "command": "call_ready",
                    "session_id": "sess-1",
                    "seq": seq,
                }
            )
        )
        manager._flush_pending_control(now=time.monotonic() + 2.0)
        self.assertEqual(len(conn.sent_payloads), 1)
        self.assertEqual(manager._control_pending, {})

    def test_call_hangup_retries_and_fails_after_retry_budget(self) -> None:
        manager = CallManager(
            socket_path="/tmp/test.sock",
            control_cmd_ack_timeout_s=0.5,
            control_cmd_max_retries=2,
        )
        conn = _FakeConn()
        manager._conn = conn

        manager._handle_worker_hangup_request("sess-1", "timeout")
        base = time.monotonic()
        manager._flush_pending_control(now=base + 1.0)
        manager._flush_pending_control(now=base + 2.0)
        manager._flush_pending_control(now=base + 3.0)

        sent = self._decode_sent_payloads(conn)
        self.assertEqual(len(sent), 3)
        for payload in sent:
            self.assertEqual(payload["type"], "call_hangup")
            self.assertEqual(payload["session_id"], "sess-1")
            self.assertEqual(payload["reason"], "timeout")
            self.assertEqual(payload["seq"], sent[0]["seq"])

        manager._flush_pending_control(now=base + 4.0)
        self.assertEqual(manager._control_pending, {})

    def test_call_end_drops_pending_control_commands_for_session(self) -> None:
        manager = CallManager(socket_path="/tmp/test.sock")
        manager._handle_worker_hangup_request("sess-1", "deadline")
        self.assertEqual(len(manager._control_pending), 1)

        manager._handle_event(json.dumps({"type": "call_end", "session_id": "sess-1"}))
        self.assertEqual(manager._control_pending, {})

    def test_runtime_snapshot_returns_none_for_unknown_session(self) -> None:
        manager = CallManager(socket_path="/tmp/test.sock")
        self.assertIsNone(manager.runtime_snapshot("missing-session"))

    def test_request_stop_sets_stop_reason_once(self) -> None:
        manager = CallManager(socket_path="/tmp/test.sock")

        manager.request_stop("signal_sigterm")
        manager.request_stop("override_attempt")

        self.assertTrue(manager.stop_requested)
        self.assertEqual(manager.stop_reason, "signal_sigterm")

    def test_send_control_clears_connection_on_send_error(self) -> None:
        manager = CallManager(socket_path="/tmp/test.sock")
        conn = _FakeConn()
        conn.raise_on_send = True
        manager._conn = conn

        ok = manager._send_control({"type": "call_ready", "session_id": "s1"})

        self.assertFalse(ok)
        self.assertTrue(conn.closed)
        self.assertIsNone(manager._conn)

    def test_wait_for_reconnect_returns_connection_when_accept_succeeds(self) -> None:
        manager = CallManager(socket_path="/tmp/test.sock")
        reconnect_conn = _FakeConn()
        server = _FakeServerSocket([(reconnect_conn, None)])

        conn = manager._wait_for_reconnect(server, grace_seconds=1.0)

        self.assertIs(conn, reconnect_conn)
        self.assertGreaterEqual(len(server.timeout_values), 2)
        self.assertIsNone(server.timeout_values[-1])

    def test_wait_for_reconnect_returns_none_on_timeout(self) -> None:
        manager = CallManager(socket_path="/tmp/test.sock")
        server = _FakeServerSocket([socket.timeout()])

        with patch(
            "src.runtime.calls.manager.socket_server.time.monotonic",
            side_effect=[0.0, 0.0, 0.0, 0.0, 1.1],
        ):
            conn = manager._wait_for_reconnect(server, grace_seconds=1.0)

        self.assertIsNone(conn)

    def test_run_stops_all_workers_if_reconnect_times_out(self) -> None:
        first_conn = _FakeConn()
        server = _FakeServerSocket([(first_conn, None)])
        manager = CallManager(socket_path="/tmp/test.sock")
        manager._workers["sess-1"] = object()

        with patch("src.runtime.calls.call_manager.socket.socket", return_value=server), patch(
            "src.runtime.calls.call_manager.os.path.exists", return_value=False
        ), patch("src.runtime.calls.call_manager.os.chmod"), patch.object(
            manager, "_handle_connection", return_value=None
        ), patch.object(
            manager, "_wait_for_reconnect", return_value=None
        ), patch.object(manager, "_stop_all", side_effect=KeyboardInterrupt) as stop_all:
            with self.assertRaises(KeyboardInterrupt):
                manager.run()
            stop_all.assert_called_once()

    def test_run_uses_pending_reconnect_connection_without_stopping_workers(self) -> None:
        first_conn = _FakeConn()
        reconnect_conn = _FakeConn()
        server = _FakeServerSocket([(first_conn, None)])
        manager = CallManager(socket_path="/tmp/test.sock")
        manager._workers["sess-1"] = object()

        handle_calls = {"count": 0}

        def _handle_connection(conn):
            _ = conn
            handle_calls["count"] += 1
            if handle_calls["count"] == 2:
                raise KeyboardInterrupt

        with patch("src.runtime.calls.call_manager.socket.socket", return_value=server), patch(
            "src.runtime.calls.call_manager.os.path.exists", return_value=False
        ), patch("src.runtime.calls.call_manager.os.chmod"), patch.object(
            manager, "_handle_connection", side_effect=_handle_connection
        ), patch.object(
            manager, "_wait_for_reconnect", return_value=reconnect_conn
        ) as wait_reconnect, patch.object(manager, "_stop_all") as stop_all:
            with self.assertRaises(KeyboardInterrupt):
                manager.run()

            self.assertEqual(server.accept_calls, 1)
            self.assertEqual(handle_calls["count"], 2)
            wait_reconnect.assert_called_once()
            stop_all.assert_not_called()

    def test_run_applies_socket_permissions(self) -> None:
        first_conn = _FakeConn()
        server = _FakeServerSocket([(first_conn, None)])
        manager = CallManager(
            socket_path="/tmp/test.sock",
            control_socket_perms=0o640,
        )

        with patch("src.runtime.calls.call_manager.socket.socket", return_value=server), patch(
            "src.runtime.calls.call_manager.os.path.exists", return_value=False
        ), patch("src.runtime.calls.call_manager.os.chmod") as chmod, patch.object(
            manager, "_handle_connection", side_effect=KeyboardInterrupt
        ):
            with self.assertRaises(KeyboardInterrupt):
                manager.run()

        chmod.assert_called_once_with("/tmp/test.sock", 0o640)

    def test_run_returns_immediately_when_stop_requested(self) -> None:
        server = _FakeServerSocket([])
        manager = CallManager(socket_path="/tmp/test.sock")
        manager.request_stop("test_shutdown")

        with patch("src.runtime.calls.call_manager.socket.socket", return_value=server), patch(
            "src.runtime.calls.call_manager.os.path.exists", return_value=False
        ), patch("src.runtime.calls.call_manager.os.chmod"), patch.object(
            manager, "_stop_all"
        ) as stop_all:
            manager.run()

        self.assertEqual(server.accept_calls, 0)
        self.assertTrue(server.closed)
        stop_all.assert_not_called()

    def test_run_stop_request_stops_active_workers(self) -> None:
        server = _FakeServerSocket([])
        manager = CallManager(socket_path="/tmp/test.sock")
        manager._workers["sess-1"] = object()
        manager.request_stop("signal_sigint")

        with patch("src.runtime.calls.call_manager.socket.socket", return_value=server), patch(
            "src.runtime.calls.call_manager.os.path.exists", return_value=False
        ), patch("src.runtime.calls.call_manager.os.chmod"), patch.object(
            manager, "_stop_all"
        ) as stop_all:
            manager.run()

        stop_all.assert_called_once()


if __name__ == "__main__":
    unittest.main()
