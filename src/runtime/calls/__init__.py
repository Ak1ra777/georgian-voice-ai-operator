"""Call-control entrypoints for the Pipecat-backed runtime."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .call_manager import CallManager


__all__ = ["CallManager"]


def __getattr__(name: str):
    if name == "CallManager":
        from .call_manager import CallManager

        globals()[name] = CallManager
        return CallManager
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
