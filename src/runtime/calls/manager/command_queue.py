from __future__ import annotations

from dataclasses import dataclass, field
import threading
import time
from typing import Callable

from .protocol import ControlAckEvent, build_control_payload


@dataclass
class PendingControlCommand:
    """Queued command waiting for a gateway ACK or a future retry."""

    seq: str
    command: str
    session_id: str
    reason: str | None
    extra_payload: dict[str, object] = field(default_factory=dict)
    attempts: int = 0
    next_attempt_at: float = 0.0


class ControlCommandQueue:
    """Tracks manager-issued control commands and resends them until ACKed."""

    def __init__(
        self,
        *,
        control_cmd_ack_timeout_s: float,
        control_cmd_max_retries: int,
        send_payload_fn: Callable[[dict], bool],
        event_recorder: Callable[[str, str, str, str | None, dict[str, object] | None], None]
        | None = None,
    ) -> None:
        self.control_cmd_ack_timeout_s = control_cmd_ack_timeout_s
        self.control_cmd_max_retries = control_cmd_max_retries
        self.control_cmd_max_attempts = 1 + control_cmd_max_retries
        self._send_payload_fn = send_payload_fn
        self._event_recorder = event_recorder
        self._control_seq = 0
        self._control_pending: dict[str, PendingControlCommand] = {}
        self._control_pending_lock = threading.Lock()
        self._control_flush_lock = threading.Lock()

    @property
    def pending(self) -> dict[str, PendingControlCommand]:
        return self._control_pending

    def next_control_seq(self) -> str:
        with self._control_pending_lock:
            self._control_seq += 1
            return str(self._control_seq)

    def build_payload(self, cmd: PendingControlCommand) -> dict:
        return build_control_payload(
            cmd.command,
            cmd.session_id,
            cmd.seq,
            reason=cmd.reason,
            extra_payload=cmd.extra_payload,
        )

    def queue_control_command(
        self,
        command: str,
        session_id: str,
        reason: str | None = None,
        extra_payload: dict[str, object] | None = None,
    ) -> None:
        pending = PendingControlCommand(
            seq=self.next_control_seq(),
            command=command,
            session_id=session_id,
            reason=reason,
            extra_payload=dict(extra_payload or {}),
        )
        with self._control_pending_lock:
            self._control_pending[pending.seq] = pending
        print(
            f"control_command_queued command={command} "
            f"session_id={session_id} seq={pending.seq}"
        )
        self._record_event(
            session_id,
            "control_command_queued",
            seq=pending.seq,
            data={"command": command},
        )
        self.flush_pending_control(force=True)

    def drop_pending_for_session(self, session_id: str, context: str) -> None:
        with self._control_pending_lock:
            drop_seqs = [
                seq
                for seq, cmd in self._control_pending.items()
                if cmd.session_id == session_id
            ]
            for seq in drop_seqs:
                self._control_pending.pop(seq, None)
        if drop_seqs:
            print(
                f"control_command_dropped session_id={session_id} "
                f"context={context} count={len(drop_seqs)}"
            )
            self._record_event(
                session_id,
                "control_command_dropped",
                level="warning",
                data={"context": context, "count": len(drop_seqs)},
            )

    def handle_control_ack(self, ack: ControlAckEvent) -> None:
        with self._control_pending_lock:
            pending = self._control_pending.get(ack.seq)
            if pending is None:
                return
            if ack.command and ack.command != pending.command:
                print(
                    f"control_ack_mismatch seq={ack.seq} expected={pending.command} "
                    f"actual={ack.command}"
                )
                self._record_event(
                    pending.session_id,
                    "control_ack_mismatch",
                    level="warning",
                    seq=ack.seq,
                    data={
                        "expected_command": pending.command,
                        "actual_command": ack.command,
                    },
                )
                return
            if ack.session_id and ack.session_id != pending.session_id:
                print(
                    f"control_ack_mismatch seq={ack.seq} "
                    f"expected_session={pending.session_id} "
                    f"actual_session={ack.session_id}"
                )
                self._record_event(
                    pending.session_id,
                    "control_ack_mismatch",
                    level="warning",
                    seq=ack.seq,
                    data={
                        "expected_session": pending.session_id,
                        "actual_session": ack.session_id,
                    },
                )
                return
            self._control_pending.pop(ack.seq, None)

        print(
            f"control_command_acked command={pending.command} "
            f"session_id={pending.session_id} seq={pending.seq} "
            f"attempts={pending.attempts}"
        )
        self._record_event(
            pending.session_id,
            "control_command_acked",
            seq=pending.seq,
            data={
                "command": pending.command,
                "attempts": pending.attempts,
            },
        )

    def flush_pending_control(
        self,
        force: bool = False,
        now: float | None = None,
    ) -> None:
        if not self._control_flush_lock.acquire(blocking=False):
            return
        try:
            current = time.monotonic() if now is None else float(now)
            with self._control_pending_lock:
                pending_items = sorted(
                    self._control_pending.values(),
                    key=lambda cmd: int(cmd.seq),
                )

            for queued in pending_items:
                with self._control_pending_lock:
                    cmd = self._control_pending.get(queued.seq)
                    if cmd is None:
                        continue
                    if not force and current < cmd.next_attempt_at:
                        continue
                    if cmd.attempts >= self.control_cmd_max_attempts:
                        self._control_pending.pop(cmd.seq, None)
                        print(
                            f"control_command_failed command={cmd.command} "
                            f"session_id={cmd.session_id} seq={cmd.seq} "
                            f"attempts={cmd.attempts}"
                        )
                        self._record_event(
                            cmd.session_id,
                            "control_command_failed",
                            level="error",
                            seq=cmd.seq,
                            data={
                                "command": cmd.command,
                                "attempts": cmd.attempts,
                            },
                        )
                        continue
                    payload = self.build_payload(cmd)

                sent = self._send_payload_fn(payload)
                with self._control_pending_lock:
                    cmd = self._control_pending.get(queued.seq)
                    if cmd is None:
                        continue
                    cmd.next_attempt_at = current + self.control_cmd_ack_timeout_s
                    if sent:
                        cmd.attempts += 1
                        print(
                            f"control_command_sent command={cmd.command} "
                            f"session_id={cmd.session_id} seq={cmd.seq} "
                            f"attempt={cmd.attempts}"
                        )
                        self._record_event(
                            cmd.session_id,
                            "control_command_sent",
                            seq=cmd.seq,
                            data={
                                "command": cmd.command,
                                "attempt": cmd.attempts,
                            },
                        )
                    else:
                        print(
                            "control_command_send_deferred "
                            f"command={cmd.command} session_id={cmd.session_id} "
                            f"seq={cmd.seq}"
                        )
                        self._record_event(
                            cmd.session_id,
                            "control_command_send_deferred",
                            level="warning",
                            seq=cmd.seq,
                            data={
                                "command": cmd.command,
                            },
                        )
        finally:
            self._control_flush_lock.release()

    def _record_event(
        self,
        session_id: str,
        event: str,
        *,
        level: str = "info",
        seq: str | None = None,
        data: dict[str, object] | None = None,
    ) -> None:
        if self._event_recorder is None:
            return
        try:
            self._event_recorder(session_id, event, level, seq, data)
        except Exception:
            return
