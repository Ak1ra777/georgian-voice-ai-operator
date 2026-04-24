from __future__ import annotations

import os
import socket
import threading
from typing import Callable

from .manager.command_queue import ControlCommandQueue, PendingControlCommand
from .manager.event_router import ManagerEventRouter
from .manager.protocol import ControlAckEvent, build_control_payload
from .manager.session_registry import CallSessionRegistry
from .manager.settings import CallManagerSettings
from .manager.socket_server import ControlSocketServer
from .session.runtime import SessionRuntime, SessionRuntimeSnapshot


def _load_default_call_worker() -> Callable[..., object]:
    from src.pipecat_runtime.call_worker import PipecatCallWorker

    return PipecatCallWorker


class CallManager:
    """Thin orchestration facade for the call control plane."""

    def __init__(
        self,
        socket_path: str | None = None,
        control_cmd_ack_timeout_s: float | None = None,
        control_cmd_max_retries: int | None = None,
        control_socket_perms: int | None = None,
        control_allowed_peer_uids: set[int] | None = None,
        worker_factory_provider: Callable[[], Callable[..., object]] | None = None,
    ) -> None:
        self._settings = CallManagerSettings.from_inputs(
            socket_path=socket_path,
            control_cmd_ack_timeout_s=control_cmd_ack_timeout_s,
            control_cmd_max_retries=control_cmd_max_retries,
            control_socket_perms=control_socket_perms,
            control_allowed_peer_uids=control_allowed_peer_uids,
        )

        # Keep the familiar top-level attributes so existing callers and tests
        # do not need to know about the internal collaborator split.
        self.socket_path = self._settings.socket_path
        self.audio_host = self._settings.audio_host
        self.no_speech_timeout_s = self._settings.no_speech_timeout_s
        self.max_duration_s = self._settings.max_duration_s
        self.control_reconnect_grace_s = self._settings.control_reconnect_grace_s
        self.control_socket_perms = self._settings.control_socket_perms
        self.control_allowed_peer_uids = self._settings.control_allowed_peer_uids
        self.control_cmd_ack_timeout_s = self._settings.control_cmd_ack_timeout_s
        self.control_cmd_max_retries = self._settings.control_cmd_max_retries
        self._control_cmd_max_attempts = self._settings.control_cmd_max_attempts

        self._socket_server = ControlSocketServer(
            socket_path=self.socket_path,
            control_socket_perms=self.control_socket_perms,
            control_allowed_peer_uids=self.control_allowed_peer_uids,
        )
        self._command_queue = self._build_command_queue()
        self._registry = CallSessionRegistry(
            worker_factory_provider=worker_factory_provider
            or _load_default_call_worker
        )
        self._router = ManagerEventRouter(
            settings=self._settings,
            registry=self._registry,
            command_queue=self._command_queue,
        )
        self._stop_event = threading.Event()
        self._stop_reason_lock = threading.Lock()
        self._stop_reason: str | None = None

    @property
    def _workers(self) -> dict[str, object]:
        return self._registry.workers

    @property
    def _runtimes(self) -> dict[str, SessionRuntime]:
        return self._registry.runtimes

    @property
    def _conn(self) -> socket.socket | None:
        return self._socket_server.connection

    @_conn.setter
    def _conn(self, conn: socket.socket | None) -> None:
        self._socket_server.connection = conn

    @property
    def _control_pending(self) -> dict[str, PendingControlCommand]:
        return self._command_queue.pending

    @property
    def stop_requested(self) -> bool:
        return self._stop_event.is_set()

    @property
    def stop_reason(self) -> str | None:
        with self._stop_reason_lock:
            return self._stop_reason

    def _build_command_queue(self):
        # Use a lambda so tests that monkey-patch `manager._send_control` still
        # intercept the real send path after initialization.
        return ControlCommandQueue(
            control_cmd_ack_timeout_s=self.control_cmd_ack_timeout_s,
            control_cmd_max_retries=self.control_cmd_max_retries,
            send_payload_fn=lambda payload: self._send_control(payload),
            event_recorder=lambda session_id, event, level, seq, data: self._record_control_queue_event(
                session_id=session_id,
                event=event,
                level=level,
                seq=seq,
                data=data,
            ),
        )

    def request_stop(self, reason: str | None = None) -> None:
        with self._stop_reason_lock:
            if reason and self._stop_reason is None:
                self._stop_reason = reason
        if not self._stop_event.is_set():
            print(
                f"call_manager_stop_requested socket={self.socket_path} "
                f"reason={self.stop_reason or 'unspecified'} "
                f"active_calls={len(self._workers)}"
            )
        self._stop_event.set()
        conn = self._conn
        if conn is not None:
            try:
                conn.close()
            except OSError:
                pass

    def run(self) -> None:
        self._prepare_socket_path()

        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(self.socket_path)
        self._apply_socket_permissions()
        server.listen(1)
        print(
            f"control_socket_listening socket={self.socket_path} "
            f"mode={oct(self.control_socket_perms)}"
        )
        pending_conn: socket.socket | None = None
        try:
            while not self._stop_event.is_set():
                if pending_conn is not None:
                    conn = pending_conn
                    pending_conn = None
                    print(
                        f"control_reconnected socket={self.socket_path} "
                        f"active_calls={len(self._workers)}"
                    )
                else:
                    conn = self._accept_authorized_connection(server, timeout_s=0.5)
                    if conn is None:
                        continue
                    print(f"control_connected socket={self.socket_path}")
                try:
                    self._handle_connection(conn)
                finally:
                    conn.close()

                if self._stop_event.is_set():
                    break

                if not self._workers:
                    continue

                print(
                    f"control_disconnected socket={self.socket_path} "
                    f"active_calls={len(self._workers)} "
                    f"grace_s={self.control_reconnect_grace_s}"
                )
                pending_conn = self._wait_for_reconnect(
                    server,
                    self.control_reconnect_grace_s,
                    stop_check_fn=self._stop_event.is_set,
                )
                if pending_conn is None:
                    if self._stop_event.is_set():
                        break
                    print(
                        f"control_reconnect_timeout socket={self.socket_path} "
                        f"active_calls={len(self._workers)} action=stop_all"
                    )
                    self._stop_all()
            if self._stop_event.is_set() and self._workers:
                print(
                    f"call_manager_stopping_active_calls socket={self.socket_path} "
                    f"active_calls={len(self._workers)} "
                    f"reason={self.stop_reason or 'unspecified'}"
                )
                self._stop_all()
        finally:
            conn = self._conn
            self._conn = None
            if conn is not None:
                try:
                    conn.close()
                except OSError:
                    pass
            try:
                server.close()
            except OSError:
                pass

    def _apply_socket_permissions(self) -> None:
        os.chmod(self.socket_path, self.control_socket_perms)

    def _prepare_socket_path(self) -> None:
        self._socket_server.prepare_socket_path()

    def _is_authorized_control_peer(self, conn: socket.socket) -> bool:
        return self._socket_server.is_authorized_control_peer(conn)

    def _accept_authorized_connection(
        self,
        server: socket.socket,
        *,
        timeout_s: float | None = None,
    ) -> socket.socket | None:
        return self._socket_server.accept_authorized_connection(
            server,
            timeout_s=timeout_s,
        )

    def _wait_for_reconnect(
        self,
        server: socket.socket,
        grace_seconds: float,
        stop_check_fn=None,
    ) -> socket.socket | None:
        return self._socket_server.wait_for_reconnect(
            server,
            grace_seconds,
            stop_check_fn=stop_check_fn,
        )

    def _handle_connection(self, conn: socket.socket) -> None:
        self._socket_server.handle_connection(
            conn,
            ack_timeout_s=self.control_cmd_ack_timeout_s,
            should_stop_fn=self._stop_event.is_set,
            on_line=lambda line: self._handle_event(line),
            on_idle=lambda force=False: self._flush_pending_control(force=force),
        )

    def _send_control(self, payload: dict) -> bool:
        return self._socket_server.send_payload(payload)

    def _next_control_seq(self) -> str:
        return self._command_queue.next_control_seq()

    def _build_control_payload(self, cmd: PendingControlCommand) -> dict:
        return build_control_payload(
            cmd.command,
            cmd.session_id,
            cmd.seq,
            reason=cmd.reason,
            extra_payload=cmd.extra_payload,
        )

    def _queue_control_command(
        self,
        command: str,
        session_id: str,
        reason: str | None = None,
        extra_payload: dict[str, object] | None = None,
    ) -> None:
        self._command_queue.queue_control_command(
            command,
            session_id,
            reason=reason,
            extra_payload=extra_payload,
        )

    def _drop_pending_for_session(self, session_id: str, context: str) -> None:
        self._command_queue.drop_pending_for_session(session_id, context)

    def _handle_control_ack(self, payload: dict) -> None:
        seq = str(payload.get("seq", "")).strip()
        if not seq:
            return
        ack = ControlAckEvent(
            seq=seq,
            command=str(payload.get("command", "")).strip() or None,
            session_id=str(payload.get("session_id", "")).strip() or None,
        )
        self._command_queue.handle_control_ack(ack)

    def _flush_pending_control(
        self,
        force: bool = False,
        now: float | None = None,
    ) -> None:
        self._command_queue.flush_pending_control(force=force, now=now)

    def _handle_worker_hangup_request(self, session_id: str, reason: str) -> None:
        self._router.handle_worker_hangup_request(session_id, reason)

    def _handle_event(self, line: str) -> None:
        self._router.handle_line(line)

    def _record_control_queue_event(
        self,
        *,
        session_id: str,
        event: str,
        level: str,
        seq: str | None,
        data: dict[str, object] | None,
    ) -> None:
        worker = self._registry.get_worker(session_id)
        if worker is None or not hasattr(worker, "record_control_event"):
            return
        worker.record_control_event(
            source="manager",
            event=event,
            level=level,
            seq=seq,
            data=data,
        )

    def _stop_worker(self, session_id: str) -> None:
        self._registry.stop_worker(session_id)
        self._drop_pending_for_session(session_id, context="worker_stop")

    def _stop_all(self) -> None:
        for session_id in list(self._workers.keys()):
            self._stop_worker(session_id)

    def runtime_snapshot(self, session_id: str) -> SessionRuntimeSnapshot | None:
        return self._registry.runtime_snapshot(session_id)
