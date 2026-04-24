from __future__ import annotations

from .command_queue import ControlCommandQueue
from .protocol import (
    CallAllocatedEvent,
    CallEndEvent,
    CallMediaStartedEvent,
    ControlAckEvent,
    DtmfKeyEvent,
    GatewayTraceEvent,
    parse_event_line,
)
from .session_registry import CallSessionRegistry
from .settings import CallManagerSettings


class ManagerEventRouter:
    """Routes parsed gateway events to the appropriate manager collaborators."""

    def __init__(
        self,
        *,
        settings: CallManagerSettings,
        registry: CallSessionRegistry,
        command_queue: ControlCommandQueue,
    ) -> None:
        self._settings = settings
        self._registry = registry
        self._command_queue = command_queue

    def handle_worker_hangup_request(self, session_id: str, reason: str) -> None:
        self._record_worker_control_event(
            session_id,
            event="call_hangup_requested",
            source="manager",
            data={
                "reason": reason,
            },
        )
        self._command_queue.queue_control_command(
            "call_hangup",
            session_id,
            reason=reason,
        )
        print(f"call_hangup_requested session_id={session_id} reason={reason}")

    def handle_line(self, line: str) -> None:
        event = parse_event_line(line)
        if event is None:
            return

        if isinstance(event, ControlAckEvent):
            self._command_queue.handle_control_ack(event)
            return

        if isinstance(event, CallAllocatedEvent):
            if self._registry.has_worker(event.session_id):
                return
            self._registry.start_session(
                session_id=event.session_id,
                rx_port=event.rx_port,
                tx_port=event.tx_port,
                remote_uri=event.remote_uri,
                host=self._settings.audio_host,
                no_speech_timeout_s=self._settings.no_speech_timeout_s,
                max_duration_s=self._settings.max_duration_s,
                on_hangup_request=self.handle_worker_hangup_request,
            )
            self._record_worker_control_event(
                event.session_id,
                event="call_allocated",
                source="gateway",
                timestamp=None,
                ts_ms=event.start_ts_ms,
                call_id=event.call_id,
                data={
                    "rx_port": event.rx_port,
                    "tx_port": event.tx_port,
                    "remote_uri": event.remote_uri,
                },
            )
            print(
                f"call_allocated session_id={event.session_id} "
                f"rx_port={event.rx_port} tx_port={event.tx_port}"
            )
            self._command_queue.queue_control_command("call_ready", event.session_id)
            return

        if isinstance(event, CallMediaStartedEvent):
            worker = self._registry.get_worker(event.session_id)
            if worker is None:
                print(
                    f"call_media_started_ignored session_id={event.session_id} "
                    "reason=unknown_session"
                )
                return
            self._record_worker_control_event(
                event.session_id,
                event="call_media_started",
                source="gateway",
                ts_ms=event.ts_ms,
                call_id=event.call_id,
            )
            worker.notify_media_started()
            print(f"call_media_started session_id={event.session_id}")
            return

        if isinstance(event, DtmfKeyEvent):
            worker = self._registry.get_worker(event.session_id)
            if worker is None:
                return
            self._record_worker_control_event(
                event.session_id,
                event="dtmf_received",
                source="gateway",
                ts_ms=event.ts_ms,
                call_id=event.call_id,
                data={
                    "digit_masked": "*",
                    "digit_count": len(event.digit),
                },
            )
            if hasattr(worker, "on_dtmf_digit"):
                worker.on_dtmf_digit(event.digit)
            return

        if isinstance(event, CallEndEvent):
            self._record_worker_control_event(
                event.session_id,
                event="call_end",
                source="gateway",
                ts_ms=event.ts_ms,
                call_id=event.call_id,
                data={
                    "reason": event.reason,
                },
            )
            self._command_queue.drop_pending_for_session(
                event.session_id,
                context="worker_stop",
            )
            self._registry.stop_worker(event.session_id)
            print(f"call_end session_id={event.session_id}")
            return

        if isinstance(event, GatewayTraceEvent):
            worker = self._registry.get_worker(event.session_id)
            if worker is None or not hasattr(worker, "record_gateway_trace"):
                return
            worker.record_gateway_trace(
                event=event.event,
                level=event.level or "info",
                ts_ms=event.ts_ms,
                call_id=event.call_id,
                data=event.data,
            )

    def _record_worker_control_event(
        self,
        session_id: str,
        *,
        event: str,
        source: str,
        level: str = "info",
        timestamp: str | None = None,
        ts_ms: int | None = None,
        call_id: int | None = None,
        turn_id: int | None = None,
        seq: str | None = None,
        data: dict[str, object] | None = None,
    ) -> None:
        worker = self._registry.get_worker(session_id)
        if worker is None or not hasattr(worker, "record_control_event"):
            return
        worker.record_control_event(
            source=source,
            event=event,
            level=level,
            timestamp=timestamp,
            ts_ms=ts_ms,
            call_id=call_id,
            turn_id=turn_id,
            seq=seq,
            data=data,
        )
