"""Live call-session execution state and lifecycle helpers."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .runtime import (
        CancellationToken,
        SessionRuntime,
        SessionRuntimeSnapshot,
        SessionRuntimeState,
        TurnExecutionAborted,
        TurnExecutionOutcome,
        TurnLoopResult,
    )


__all__ = [
    "CancellationToken",
    "SessionRuntime",
    "SessionRuntimeSnapshot",
    "SessionRuntimeState",
    "TurnExecutionAborted",
    "TurnExecutionOutcome",
    "TurnLoopResult",
]


_EXPORTS = {
    "CancellationToken": (".runtime", "CancellationToken"),
    "SessionRuntime": (".runtime", "SessionRuntime"),
    "SessionRuntimeSnapshot": (".runtime", "SessionRuntimeSnapshot"),
    "SessionRuntimeState": (".runtime", "SessionRuntimeState"),
    "TurnExecutionAborted": (".runtime", "TurnExecutionAborted"),
    "TurnExecutionOutcome": (".runtime", "TurnExecutionOutcome"),
    "TurnLoopResult": (".runtime", "TurnLoopResult"),
}


def __getattr__(name: str):
    try:
        module_name, attr_name = _EXPORTS[name]
    except KeyError as ex:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        ) from ex

    if module_name.startswith("."):
        module = import_module(module_name, __name__)
    else:
        module = import_module(module_name)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
