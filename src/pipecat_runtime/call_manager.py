from __future__ import annotations

from src.runtime.calls.call_manager import CallManager

from .call_worker import PipecatCallWorker


class PipecatCallManager(CallManager):
    """Manager entrypoint for the Pipecat-backed Python runtime."""

    def __init__(
        self,
        socket_path: str | None = None,
        control_cmd_ack_timeout_s: float | None = None,
        control_cmd_max_retries: int | None = None,
        control_socket_perms: int | None = None,
        control_allowed_peer_uids: set[int] | None = None,
    ) -> None:
        super().__init__(
            socket_path=socket_path,
            control_cmd_ack_timeout_s=control_cmd_ack_timeout_s,
            control_cmd_max_retries=control_cmd_max_retries,
            control_socket_perms=control_socket_perms,
            control_allowed_peer_uids=control_allowed_peer_uids,
            worker_factory_provider=lambda: PipecatCallWorker,
        )
