from __future__ import annotations

import inspect
from typing import Callable

from ..session.runtime import SessionRuntime, SessionRuntimeSnapshot


class CallSessionRegistry:
    """Owns the live worker/runtime objects keyed by session id."""

    def __init__(
        self,
        *,
        worker_factory_provider: Callable[[], Callable[..., object]],
    ) -> None:
        self._worker_factory_provider = worker_factory_provider
        self._workers: dict[str, object] = {}
        self._runtimes: dict[str, SessionRuntime] = {}

    @property
    def workers(self) -> dict[str, object]:
        return self._workers

    @property
    def runtimes(self) -> dict[str, SessionRuntime]:
        return self._runtimes

    def has_worker(self, session_id: str) -> bool:
        return session_id in self._workers

    def get_worker(self, session_id: str) -> object | None:
        return self._workers.get(session_id)

    def start_session(
        self,
        *,
        session_id: str,
        rx_port: int,
        tx_port: int,
        remote_uri: str | None,
        host: str,
        no_speech_timeout_s: float,
        max_duration_s: float,
        on_hangup_request: Callable[[str, str], None],
    ) -> object:
        runtime = SessionRuntime(session_id=session_id)
        worker_cls = self._worker_factory_provider()
        worker_kwargs = {
            "session_id": session_id,
            "rx_port": rx_port,
            "tx_port": tx_port,
            "host": host,
            "no_speech_timeout_s": no_speech_timeout_s,
            "max_duration_s": max_duration_s,
            "on_hangup_request": on_hangup_request,
            "runtime": runtime,
        }
        try:
            parameters = inspect.signature(worker_cls).parameters
        except (TypeError, ValueError):
            parameters = {}
        if "remote_uri" in parameters:
            worker_kwargs["remote_uri"] = remote_uri

        worker = worker_cls(**worker_kwargs)
        self._runtimes[session_id] = runtime
        self._workers[session_id] = worker
        try:
            worker.start()
        except Exception:
            self._workers.pop(session_id, None)
            self._runtimes.pop(session_id, None)
            raise
        return worker

    def stop_worker(self, session_id: str) -> None:
        worker = self._workers.pop(session_id, None)
        if worker:
            worker.stop()
        self._runtimes.pop(session_id, None)

    def stop_all(self) -> None:
        for session_id in list(self._workers.keys()):
            self.stop_worker(session_id)

    def runtime_snapshot(self, session_id: str) -> SessionRuntimeSnapshot | None:
        runtime = self._runtimes.get(session_id)
        if runtime is None:
            return None
        return runtime.snapshot()
