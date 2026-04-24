from __future__ import annotations

import json
import os
import socket
import stat
import struct
import threading
import time
from typing import Callable


class ControlSocketServer:
    """Owns the Unix socket transport used by the manager control loop."""

    def __init__(
        self,
        *,
        socket_path: str,
        control_socket_perms: int,
        control_allowed_peer_uids: set[int],
    ) -> None:
        self.socket_path = socket_path
        self.control_socket_perms = control_socket_perms
        self.control_allowed_peer_uids = set(control_allowed_peer_uids)
        self._conn: socket.socket | None = None
        self._conn_lock = threading.Lock()

    @property
    def connection(self) -> socket.socket | None:
        with self._conn_lock:
            return self._conn

    @connection.setter
    def connection(self, conn: socket.socket | None) -> None:
        with self._conn_lock:
            self._conn = conn

    def prepare_socket_path(self) -> None:
        """Prepare the socket path and only unlink safe, owned socket files."""

        parent_dir = os.path.dirname(self.socket_path) or "."
        os.makedirs(parent_dir, exist_ok=True)

        if not os.path.exists(self.socket_path):
            return

        st = os.lstat(self.socket_path)
        if not stat.S_ISSOCK(st.st_mode):
            raise RuntimeError(
                f"Refusing to unlink non-socket path: {self.socket_path}"
            )
        if hasattr(os, "getuid") and st.st_uid != os.getuid():
            raise RuntimeError(
                "Refusing to unlink socket not owned by current uid: "
                f"{self.socket_path}"
            )
        os.unlink(self.socket_path)

    def is_authorized_control_peer(self, conn: socket.socket) -> bool:
        """Validate the connecting peer by UID when SO_PEERCRED is available."""

        if not hasattr(socket, "SO_PEERCRED"):
            print(
                f"control_peer_validation_unavailable socket={self.socket_path} "
                "action=allow"
            )
            return True

        try:
            raw = conn.getsockopt(
                socket.SOL_SOCKET,
                socket.SO_PEERCRED,
                struct.calcsize("3i"),
            )
            pid, uid, gid = struct.unpack("3i", raw)
        except (AttributeError, OSError, struct.error) as ex:
            print(
                f"control_peer_validation_failed socket={self.socket_path} "
                f"error={ex}"
            )
            return False

        if self.control_allowed_peer_uids and uid not in self.control_allowed_peer_uids:
            allowed = ",".join(str(v) for v in sorted(self.control_allowed_peer_uids))
            print(
                f"control_peer_rejected socket={self.socket_path} pid={pid} uid={uid} "
                f"gid={gid} allowed_uids={allowed}"
            )
            return False

        print(
            f"control_peer_authenticated socket={self.socket_path} pid={pid} "
            f"uid={uid} gid={gid}"
        )
        return True

    def accept_authorized_connection(
        self,
        server: socket.socket,
        *,
        timeout_s: float | None = None,
    ) -> socket.socket | None:
        """Accept one authorized peer connection from the gateway."""

        deadline = (
            None if timeout_s is None else (time.monotonic() + max(0.0, timeout_s))
        )
        while True:
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                server.settimeout(remaining)
            try:
                conn, _ = server.accept()
            except socket.timeout:
                return None
            finally:
                if deadline is not None:
                    server.settimeout(None)

            if self.is_authorized_control_peer(conn):
                return conn

            try:
                conn.close()
            except OSError:
                pass

    def wait_for_reconnect(
        self,
        server: socket.socket,
        grace_seconds: float,
        stop_check_fn: Callable[[], bool] | None = None,
    ) -> socket.socket | None:
        """Wait for a reconnect within the configured grace window."""

        if grace_seconds <= 0:
            return None

        deadline = time.monotonic() + grace_seconds
        while True:
            if stop_check_fn and stop_check_fn():
                return None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                conn = self.accept_authorized_connection(
                    server,
                    timeout_s=min(remaining, 0.5),
                )
                if conn is None:
                    continue
                return conn
            except socket.timeout:
                continue
            except OSError as ex:
                print(
                    f"control_reconnect_accept_error socket={self.socket_path} "
                    f"error={ex}"
                )
                return None

    def handle_connection(
        self,
        conn: socket.socket,
        *,
        ack_timeout_s: float,
        should_stop_fn: Callable[[], bool],
        on_line: Callable[[str], None],
        on_idle: Callable[[bool], None] | None = None,
    ) -> None:
        """Read newline-delimited control events from the active gateway connection."""

        self.connection = conn
        conn.settimeout(min(0.25, ack_timeout_s))
        if on_idle is not None:
            on_idle(True)
        buffer = ""
        try:
            while not should_stop_fn():
                try:
                    data = conn.recv(4096)
                except socket.timeout:
                    if on_idle is not None:
                        on_idle(False)
                    continue
                except OSError as ex:
                    print(
                        f"control_recv_error socket={self.socket_path} error={ex}"
                    )
                    return
                if not data:
                    return
                buffer += data.decode("utf-8", errors="ignore")
                while "\n" in buffer:
                    line, buffer = buffer.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    on_line(line)
                if on_idle is not None:
                    on_idle(False)
        finally:
            with self._conn_lock:
                if self._conn is conn:
                    self._conn = None

    def send_payload(self, payload: dict) -> bool:
        """Send one JSON command to the active gateway connection."""

        message = json.dumps(payload) + "\n"
        data = message.encode("utf-8")
        with self._conn_lock:
            if self._conn is None:
                return False
            try:
                self._conn.sendall(data)
                return True
            except OSError:
                try:
                    self._conn.close()
                except OSError:
                    pass
                self._conn = None
                return False
