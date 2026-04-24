from __future__ import annotations

from threading import Lock


class DeferredHangupController:
    """Coordinate hangup requests that should fire after the bot finishes speaking."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._pending_reason: str | None = None

    def schedule(self, reason: str) -> None:
        normalized = str(reason).strip() or "assistant_completed"
        with self._lock:
            self._pending_reason = normalized

    def clear(self) -> str | None:
        with self._lock:
            reason = self._pending_reason
            self._pending_reason = None
            return reason

    def consume(self) -> str | None:
        return self.clear()

    def pending_reason(self) -> str | None:
        with self._lock:
            return self._pending_reason
