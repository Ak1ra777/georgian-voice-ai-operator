from __future__ import annotations

from dataclasses import dataclass
import json
from typing import TypeAlias


@dataclass(frozen=True)
class ControlAckEvent:
    """Acknowledgement for a manager-issued control command."""

    seq: str
    command: str | None = None
    session_id: str | None = None


@dataclass(frozen=True)
class CallAllocatedEvent:
    """Gateway notification that a new call session has media ports assigned."""

    session_id: str
    rx_port: int
    tx_port: int
    remote_uri: str | None = None
    call_id: int | None = None
    start_ts_ms: int | None = None


@dataclass(frozen=True)
class CallMediaStartedEvent:
    """Gateway notification that media is flowing for a session."""

    session_id: str
    call_id: int | None = None
    ts_ms: int | None = None


@dataclass(frozen=True)
class CallEndEvent:
    """Gateway notification that a session should be torn down."""

    session_id: str
    reason: str | None = None
    call_id: int | None = None
    ts_ms: int | None = None


@dataclass(frozen=True)
class DtmfKeyEvent:
    """Gateway notification of a single DTMF key press."""

    session_id: str
    digit: str
    call_id: int | None = None
    ts_ms: int | None = None


@dataclass(frozen=True)
class GatewayTraceEvent:
    """Gateway-emitted structured trace associated with one live session."""

    session_id: str
    event: str
    level: str | None = None
    call_id: int | None = None
    ts_ms: int | None = None
    data: dict[str, object] | None = None


ManagerEvent: TypeAlias = (
    ControlAckEvent
    | CallAllocatedEvent
    | CallMediaStartedEvent
    | CallEndEvent
    | DtmfKeyEvent
    | GatewayTraceEvent
)


def parse_event_line(line: str) -> ManagerEvent | None:
    """Parse one newline-delimited control-plane JSON event."""

    try:
        payload = json.loads(line)
    except json.JSONDecodeError:
        return None

    event_type = payload.get("type")
    if event_type == "control_ack":
        seq = str(payload.get("seq", "")).strip()
        if not seq:
            return None
        command = str(payload.get("command", "")).strip() or None
        session_id = str(payload.get("session_id", "")).strip() or None
        return ControlAckEvent(seq=seq, command=command, session_id=session_id)

    if event_type == "call_allocated":
        session_id = str(payload.get("session_id", "")).strip()
        rx_port = payload.get("rx_port")
        tx_port = payload.get("tx_port")
        if not session_id or rx_port in {None, ""} or tx_port in {None, ""}:
            return None
        try:
            return CallAllocatedEvent(
                session_id=session_id,
                rx_port=int(rx_port),
                tx_port=int(tx_port),
                remote_uri=str(payload.get("remote_uri", "")).strip() or None,
                call_id=_parse_optional_int(payload.get("call_id")),
                start_ts_ms=_parse_optional_int(payload.get("start_ts_ms")),
            )
        except (TypeError, ValueError):
            return None

    if event_type == "call_media_started":
        session_id = str(payload.get("session_id", "")).strip()
        if not session_id:
            return None
        return CallMediaStartedEvent(
            session_id=session_id,
            call_id=_parse_optional_int(payload.get("call_id")),
            ts_ms=_parse_optional_int(payload.get("ts_ms")),
        )

    if event_type == "call_end":
        session_id = str(payload.get("session_id", "")).strip()
        if not session_id:
            return None
        return CallEndEvent(
            session_id=session_id,
            reason=str(payload.get("reason", "")).strip() or None,
            call_id=_parse_optional_int(payload.get("call_id")),
            ts_ms=_parse_optional_int(payload.get("ts_ms")),
        )

    if event_type == "dtmf_key":
        session_id = str(payload.get("session_id", "")).strip()
        digit = str(payload.get("digit", "")).strip()
        if not session_id or not digit:
            return None
        return DtmfKeyEvent(
            session_id=session_id,
            digit=digit,
            call_id=_parse_optional_int(payload.get("call_id")),
            ts_ms=_parse_optional_int(payload.get("ts_ms")),
        )

    if event_type == "gateway_trace":
        session_id = str(payload.get("session_id", "")).strip()
        event = str(payload.get("event", "")).strip()
        if not session_id or not event:
            return None
        data = payload.get("data")
        if not isinstance(data, dict):
            data = {
                key: value
                for key, value in payload.items()
                if key
                not in {"type", "session_id", "event", "level", "call_id", "ts_ms"}
            }
        return GatewayTraceEvent(
            session_id=session_id,
            event=event,
            level=str(payload.get("level", "")).strip() or None,
            call_id=_parse_optional_int(payload.get("call_id")),
            ts_ms=_parse_optional_int(payload.get("ts_ms")),
            data=data or None,
        )

    return None


def build_control_payload(
    command: str,
    session_id: str,
    seq: str,
    *,
    reason: str | None = None,
    extra_payload: dict | None = None,
) -> dict:
    """Build the wire-format payload for a manager-issued control command."""

    payload = {
        "type": command,
        "session_id": session_id,
        "seq": seq,
    }
    if reason is not None:
        payload["reason"] = reason
    if extra_payload:
        payload.update(dict(extra_payload))
    return payload


def _parse_optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
