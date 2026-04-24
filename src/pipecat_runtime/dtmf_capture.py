from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable


CollectionCompleteHandler = Callable[[str], Awaitable[None]]
CollectionCancelHandler = Callable[[], Awaitable[None]]


@dataclass
class DtmfCollectionRequest:
    kind: str
    submit_key: str
    on_complete: CollectionCompleteHandler
    on_cancel: CollectionCancelHandler | None = None


class DtmfCollectionCoordinator:
    """Track one active keypad collection request at a time."""

    def __init__(self) -> None:
        self._request: DtmfCollectionRequest | None = None
        self._buffer = ""
        self._started = False

    def active_kind(self) -> str | None:
        if self._request is None:
            return None
        return self._request.kind

    async def start(self, request: DtmfCollectionRequest) -> None:
        self._request = request
        self._buffer = ""
        self._started = False

    async def clear(self) -> None:
        request = self._request
        self._request = None
        self._buffer = ""
        self._started = False
        if request is not None and request.on_cancel is not None:
            await request.on_cancel()

    async def handle_digit(self, digit: str) -> tuple[bool, bool]:
        request = self._request
        if request is None:
            return False, False

        normalized = str(digit).strip()
        if not normalized:
            return False, False

        if normalized == request.submit_key:
            if not self._buffer:
                return True, False

            value = self._buffer
            self._request = None
            self._buffer = ""
            self._started = False
            await request.on_complete(value)
            return True, False

        if normalized not in "0123456789":
            return True, False

        should_interrupt = not self._started
        self._started = True
        self._buffer += normalized
        return True, should_interrupt
