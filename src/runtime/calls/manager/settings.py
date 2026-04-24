from __future__ import annotations

from dataclasses import dataclass
import os

from src.config import (
    CALL_MAX_DURATION_S,
    CALL_NO_SPEECH_TIMEOUT_S,
    CONTROL_ALLOWED_PEER_UIDS,
    CONTROL_CMD_ACK_TIMEOUT_S,
    CONTROL_CMD_MAX_RETRIES,
    CONTROL_RECONNECT_GRACE_S,
    CONTROL_SOCKET_PERMS,
    SIP_AUDIO_HOST,
    SIP_CONTROL_SOCKET,
)


@dataclass(frozen=True)
class CallManagerSettings:
    """Normalized configuration for the manager-side call control loop."""

    socket_path: str
    audio_host: str
    no_speech_timeout_s: float
    max_duration_s: float
    control_reconnect_grace_s: float
    control_socket_perms: int
    control_allowed_peer_uids: set[int]
    control_cmd_ack_timeout_s: float
    control_cmd_max_retries: int

    @property
    def control_cmd_max_attempts(self) -> int:
        return 1 + self.control_cmd_max_retries

    @classmethod
    def from_inputs(
        cls,
        *,
        socket_path: str | None = None,
        control_cmd_ack_timeout_s: float | None = None,
        control_cmd_max_retries: int | None = None,
        control_socket_perms: int | None = None,
        control_allowed_peer_uids: set[int] | None = None,
    ) -> "CallManagerSettings":
        allowed_uids = (
            set(CONTROL_ALLOWED_PEER_UIDS)
            if control_allowed_peer_uids is None
            else {int(uid) for uid in control_allowed_peer_uids}
        )
        if not allowed_uids and hasattr(os, "getuid"):
            allowed_uids.add(os.getuid())

        return cls(
            socket_path=socket_path or SIP_CONTROL_SOCKET,
            audio_host=SIP_AUDIO_HOST,
            no_speech_timeout_s=CALL_NO_SPEECH_TIMEOUT_S,
            max_duration_s=CALL_MAX_DURATION_S,
            control_reconnect_grace_s=max(0.0, CONTROL_RECONNECT_GRACE_S),
            control_socket_perms=max(
                0,
                min(
                    0o777,
                    CONTROL_SOCKET_PERMS
                    if control_socket_perms is None
                    else int(control_socket_perms),
                ),
            ),
            control_allowed_peer_uids=allowed_uids,
            control_cmd_ack_timeout_s=max(
                0.1,
                CONTROL_CMD_ACK_TIMEOUT_S
                if control_cmd_ack_timeout_s is None
                else float(control_cmd_ack_timeout_s),
            ),
            control_cmd_max_retries=max(
                0,
                CONTROL_CMD_MAX_RETRIES
                if control_cmd_max_retries is None
                else int(control_cmd_max_retries),
            ),
        )
