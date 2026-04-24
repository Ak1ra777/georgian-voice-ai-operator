"""Runtime helpers exposed at the package boundary."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .calls.session.runtime import SessionRuntime, SessionRuntimeSnapshot, SessionRuntimeState


__all__ = [
    "SessionRuntime",
    "SessionRuntimeSnapshot",
    "SessionRuntimeState",
]


_EXPORTS = {
    "SessionRuntime": (".calls.session.runtime", "SessionRuntime"),
    "SessionRuntimeSnapshot": (".calls.session.runtime", "SessionRuntimeSnapshot"),
    "SessionRuntimeState": (".calls.session.runtime", "SessionRuntimeState"),
}


def __getattr__(name: str):
    try:
        module_name, attr_name = _EXPORTS[name]
    except KeyError as ex:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        ) from ex

    module = import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
