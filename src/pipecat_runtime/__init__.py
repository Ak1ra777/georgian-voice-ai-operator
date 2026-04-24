"""Pipecat-backed runtime entrypoints for the SIP gateway seam."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .call_manager import PipecatCallManager
    from .call_worker import PipecatCallWorker
    from .gateway_bridge import GatewaySessionBridge
    from .policy import SessionPolicyController
    from .tools import PipecatToolRuntime
    from .validation import validate_pipecat_runtime

__all__ = [
    "GatewaySessionBridge",
    "PipecatCallManager",
    "PipecatCallWorker",
    "PipecatToolRuntime",
    "SessionPolicyController",
    "validate_pipecat_runtime",
]


def __getattr__(name: str):
    if name == "GatewaySessionBridge":
        from .gateway_bridge import GatewaySessionBridge

        value = GatewaySessionBridge
    elif name == "PipecatCallManager":
        from .call_manager import PipecatCallManager

        value = PipecatCallManager
    elif name == "PipecatCallWorker":
        from .call_worker import PipecatCallWorker

        value = PipecatCallWorker
    elif name == "PipecatToolRuntime":
        from .tools import PipecatToolRuntime

        value = PipecatToolRuntime
    elif name == "SessionPolicyController":
        from .policy import SessionPolicyController

        value = SessionPolicyController
    elif name == "validate_pipecat_runtime":
        from .validation import validate_pipecat_runtime

        value = validate_pipecat_runtime
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    globals()[name] = value
    return value
