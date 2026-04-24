from __future__ import annotations

import json
import os
import wave
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock
from typing import Any

from pydantic import BaseModel, Field
from src.config import CALL_ARTIFACT_SYNC_WRITES
from src.runtime.media.audio.udp_audio import (
    _LinearResampleState,
    _resample_pcm16_mono_linear,
)


DEFAULT_AUDIO_SAMPLE_RATE = 16000
DEFAULT_AUDIO_CHANNELS = 1


class LocalCallTicketPayload(BaseModel):
    session_id: str
    call_status: str
    disposition: str
    started_at: str
    updated_at: str
    finalized_at: str | None = None
    end_reason: str | None = None
    current_node: str | None = None
    branch: str | None = None
    active_dtmf_kind: str | None = None
    follow_up_required: bool = False
    formal_ticket_created: bool = False
    formal_ticket_id: str | None = None
    formal_ticket_summary: str | None = None
    caller_uri: str | None = None
    caller_number: str | None = None
    callback_number: str | None = None
    contact_phone: str | None = None
    customer_name: str | None = None
    company_name: str | None = None
    #avtandila isp specific
    subscriber_identifier: str | None = None
    customer_id: str | None = None
    account_id: str | None = None
    service_id: str | None = None
    service_type: str | None = None
    service_address: str | None = None
    lookup_status: str | None = None
    lookup_message: str | None = None
    verification_status: str | None = None
    verification_reason: str | None = None
    outage_status: str | None = None
    outage_summary: str | None = None
    balance_verified: bool = False


class LocalCallMetadata(BaseModel):
    session_id: str
    status: str
    started_at: str
    updated_at: str
    finalized_at: str | None = None
    caller_uri: str | None = None
    caller_number: str | None = None
    runtime: dict[str, Any] = Field(default_factory=dict)
    flow: dict[str, Any] = Field(default_factory=dict)
    latency: dict[str, Any] = Field(default_factory=dict)
    bridge: dict[str, Any] = Field(default_factory=dict)
    egress: dict[str, Any] = Field(default_factory=dict)
    tools: dict[str, Any] = Field(default_factory=dict)
    gateway: dict[str, Any] = Field(default_factory=dict)
    event_counts: dict[str, Any] = Field(default_factory=dict)
    counters: dict[str, Any] = Field(default_factory=dict)
    artifacts: dict[str, str] = Field(default_factory=dict)


class CallArtifactWriter:
    """Persist per-call local artifacts that survive orderly hangups."""

    def __init__(
        self,
        *,
        session_id: str,
        base_dir: Path | str = Path("outputs/calls"),
        caller_uri: str | None = None,
        caller_number: str | None = None,
        sync_writes: bool = CALL_ARTIFACT_SYNC_WRITES,
    ) -> None:
        self.session_id = session_id
        self.base_dir = Path(base_dir)
        self.call_dir = self.base_dir / session_id
        self.call_dir.mkdir(parents=True, exist_ok=True)
        self.telemetry_dir = self.call_dir / "telemetry"
        self.telemetry_dir.mkdir(parents=True, exist_ok=True)

        self.transcript_jsonl_path = self.call_dir / "transcript.jsonl"
        self.transcript_txt_path = self.call_dir / "transcript.txt"
        self.ticket_path = self.call_dir / "ticket.json"
        self.ticket_payload_path = self.call_dir / "ticket_payload.json"
        self.metadata_path = self.call_dir / "metadata.json"
        self.caller_audio_path = self.call_dir / "caller.wav"
        self.assistant_audio_path = self.call_dir / "assistant.wav"
        self.timeline_path = self.telemetry_dir / "timeline.jsonl"
        self.control_path = self.telemetry_dir / "control.jsonl"
        self.media_path = self.telemetry_dir / "media.jsonl"
        self.turns_path = self.telemetry_dir / "turns.jsonl"
        self.tools_path = self.telemetry_dir / "tools.jsonl"

        self._caller_uri = _normalize_optional_str(caller_uri)
        self._caller_number = _normalize_optional_str(caller_number)
        self._sync_writes = bool(sync_writes)
        self._started_at_dt = datetime.now(UTC)
        self._started_at = _isoformat_utc(self._started_at_dt)
        self._started_at_epoch_ms = int(self._started_at_dt.timestamp() * 1000)
        self._updated_at = self._started_at
        self._transcript_events = 0
        self._caller_audio_bytes = 0
        self._assistant_audio_bytes = 0
        self._caller_audio_rate: int | None = None
        self._assistant_audio_rate: int | None = None
        self._caller_resample_state: _LinearResampleState | None = None
        self._assistant_resample_state: _LinearResampleState | None = None
        self._caller_resample_source_rate: int | None = None
        self._assistant_resample_source_rate: int | None = None
        self._category_counts = {
            "timeline": 0,
            "control": 0,
            "media": 0,
            "turns": 0,
            "tools": 0,
        }
        self._source_counts: dict[str, int] = {}
        self._last_gateway_event: str | None = None
        self._gateway_call_id: int | None = None
        self._gateway_end_reason: str | None = None
        self._gateway_media_bridge_stats: dict[str, Any] = {}
        self._lock = Lock()
        self._finalized = False

        self._transcript_handle = open(
            self.transcript_jsonl_path,
            "w",
            encoding="utf-8",
        )
        self._caller_audio_handle = None
        self._caller_wave = None
        self._assistant_audio_handle = None
        self._assistant_wave = None
        self._telemetry_handles = {
            "timeline": open(self.timeline_path, "w", encoding="utf-8"),
            "control": open(self.control_path, "w", encoding="utf-8"),
            "media": open(self.media_path, "w", encoding="utf-8"),
            "turns": open(self.turns_path, "w", encoding="utf-8"),
            "tools": open(self.tools_path, "w", encoding="utf-8"),
        }

        self._persist_status(call_status="in_progress", disposition="in_progress")

    def append_user_transcript(
        self,
        text: str,
        *,
        timestamp: str | None = None,
        language: str | None = None,
        user_id: str | None = None,
        flow_current_node: str | None = None,
        flow_branch: str | None = None,
        flow_active_dtmf_kind: str | None = None,
        flow_last_tool_id: str | None = None,
    ) -> None:
        self._append_transcript_event(
            role="caller",
            channel="stt",
            text=text,
            timestamp=timestamp,
            language=language,
            user_id=user_id,
            flow_current_node=flow_current_node,
            flow_branch=flow_branch,
            flow_active_dtmf_kind=flow_active_dtmf_kind,
            flow_last_tool_id=flow_last_tool_id,
        )

    def append_assistant_transcript(
        self,
        text: str,
        *,
        context_id: str | None = None,
        flow_current_node: str | None = None,
        flow_branch: str | None = None,
        flow_active_dtmf_kind: str | None = None,
        flow_last_tool_id: str | None = None,
    ) -> None:
        self._append_transcript_event(
            role="assistant",
            channel="tts",
            text=text,
            context_id=context_id,
            flow_current_node=flow_current_node,
            flow_branch=flow_branch,
            flow_active_dtmf_kind=flow_active_dtmf_kind,
            flow_last_tool_id=flow_last_tool_id,
        )

    def append_caller_audio(
        self,
        audio: bytes,
        *,
        sample_rate: int,
        num_channels: int = DEFAULT_AUDIO_CHANNELS,
    ) -> None:
        self._append_audio(
            side="caller",
            audio=audio,
            sample_rate=sample_rate,
            num_channels=num_channels,
        )

    def append_assistant_audio(
        self,
        audio: bytes,
        *,
        sample_rate: int,
        num_channels: int = DEFAULT_AUDIO_CHANNELS,
    ) -> None:
        self._append_audio(
            side="assistant",
            audio=audio,
            sample_rate=sample_rate,
            num_channels=num_channels,
        )

    def record_control_event(
        self,
        *,
        source: str,
        event: str,
        level: str = "info",
        timestamp: str | None = None,
        ts_ms: int | None = None,
        call_id: int | None = None,
        turn_id: int | None = None,
        seq: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        self.record_event(
            source=source,
            category="control",
            event=event,
            level=level,
            timestamp=timestamp,
            ts_ms=ts_ms,
            call_id=call_id,
            turn_id=turn_id,
            seq=seq,
            data=data,
        )

    def record_media_event(
        self,
        *,
        source: str,
        event: str,
        level: str = "info",
        timestamp: str | None = None,
        ts_ms: int | None = None,
        call_id: int | None = None,
        turn_id: int | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        self.record_event(
            source=source,
            category="media",
            event=event,
            level=level,
            timestamp=timestamp,
            ts_ms=ts_ms,
            call_id=call_id,
            turn_id=turn_id,
            data=data,
        )

    def record_turn_event(
        self,
        *,
        source: str,
        event: str,
        level: str = "info",
        timestamp: str | None = None,
        ts_ms: int | None = None,
        call_id: int | None = None,
        turn_id: int | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        self.record_event(
            source=source,
            category="turns",
            event=event,
            level=level,
            timestamp=timestamp,
            ts_ms=ts_ms,
            call_id=call_id,
            turn_id=turn_id,
            data=data,
        )

    def record_tool_event(
        self,
        *,
        source: str,
        event: str,
        level: str = "info",
        timestamp: str | None = None,
        ts_ms: int | None = None,
        call_id: int | None = None,
        turn_id: int | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        self.record_event(
            source=source,
            category="tools",
            event=event,
            level=level,
            timestamp=timestamp,
            ts_ms=ts_ms,
            call_id=call_id,
            turn_id=turn_id,
            data=data,
        )

    def record_gateway_trace(
        self,
        *,
        event: str,
        level: str = "info",
        timestamp: str | None = None,
        ts_ms: int | None = None,
        call_id: int | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        record_event = self.record_control_event
        if event == "call_media_bridge_stats":
            record_event = self.record_media_event
        record_event(
            source="gateway",
            event=event,
            level=level,
            timestamp=timestamp,
            ts_ms=ts_ms,
            call_id=call_id,
            data=data,
        )

    def record_event(
        self,
        *,
        source: str,
        category: str,
        event: str,
        level: str = "info",
        timestamp: str | None = None,
        ts_ms: int | None = None,
        call_id: int | None = None,
        turn_id: int | None = None,
        seq: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        normalized_source = _normalize_optional_str(source)
        normalized_category = _normalize_optional_str(category)
        normalized_event = _normalize_optional_str(event)
        if normalized_source is None or normalized_category is None or normalized_event is None:
            return

        event_ts, event_epoch_ms = _resolve_event_time(
            timestamp=timestamp,
            ts_ms=ts_ms,
        )
        payload = {
            "ts": event_ts,
            "offset_ms": max(0, event_epoch_ms - self._started_at_epoch_ms),
            "session_id": self.session_id,
            "source": normalized_source,
            "category": normalized_category,
            "event": normalized_event,
            "level": _normalize_optional_str(level) or "info",
        }
        if call_id is not None:
            payload["call_id"] = int(call_id)
        if turn_id is not None:
            payload["turn_id"] = int(turn_id)
        normalized_seq = _normalize_optional_str(seq)
        if normalized_seq is not None:
            payload["seq"] = normalized_seq
        sanitized_data = _json_safe(data or {})
        if sanitized_data:
            payload["data"] = sanitized_data

        with self._lock:
            if self._finalized:
                return
            self._updated_at = event_ts
            self._write_telemetry_event("timeline", payload)
            if normalized_category in self._telemetry_handles:
                self._write_telemetry_event(normalized_category, payload)
            self._source_counts[normalized_source] = (
                self._source_counts.get(normalized_source, 0) + 1
            )
            if normalized_source == "gateway":
                self._last_gateway_event = normalized_event
                if call_id is not None:
                    self._gateway_call_id = int(call_id)
                if normalized_event == "call_end":
                    self._gateway_end_reason = _normalize_optional_str(
                        (sanitized_data or {}).get("reason")
                    )
                if normalized_event == "call_media_bridge_stats":
                    self._gateway_media_bridge_stats = dict(sanitized_data)

    def persist_ticket_payload(self, payload: dict[str, Any]) -> Path:
        sanitized_payload = _json_safe(payload)
        if not isinstance(sanitized_payload, dict):
            sanitized_payload = {}
        with self._lock:
            _write_json_atomic(self.ticket_payload_path, sanitized_payload)
            self._updated_at = _utc_now()
        return self.ticket_payload_path

    def finalize(
        self,
        *,
        runtime_snapshot: Any = None,
        flow_state: dict[str, Any] | None = None,
        tool_state: dict[str, Any] | None = None,
        latency_state: Any = None,
        bridge_state: Any = None,
        egress_state: Any = None,
        tool_snapshot: Any = None,
    ) -> Path:
        with self._lock:
            if self._finalized:
                return self.call_dir

            finalized_at = _utc_now()
            self._updated_at = finalized_at

            self._flush_resampler(side="caller")
            self._flush_resampler(side="assistant")
            self._ensure_audio_writer(
                side="caller",
                sample_rate=self._caller_audio_rate or DEFAULT_AUDIO_SAMPLE_RATE,
                num_channels=DEFAULT_AUDIO_CHANNELS,
            )
            self._ensure_audio_writer(
                side="assistant",
                sample_rate=self._assistant_audio_rate or DEFAULT_AUDIO_SAMPLE_RATE,
                num_channels=DEFAULT_AUDIO_CHANNELS,
            )
            self._close_wav_writer(side="caller")
            self._close_wav_writer(side="assistant")
            self._close_transcript_handle()
            self._close_telemetry_handles()

            ticket_payload = self._build_ticket_payload(
                call_status="finalized",
                finalized_at=finalized_at,
                runtime_snapshot=runtime_snapshot,
                flow_state=flow_state,
                tool_state=tool_state,
            )
            metadata_payload = self._build_metadata_payload(
                status="finalized",
                finalized_at=finalized_at,
                runtime_snapshot=runtime_snapshot,
                flow_state=flow_state,
                tool_state=tool_state,
                latency_state=latency_state,
                bridge_state=bridge_state,
                egress_state=egress_state,
                tool_snapshot=tool_snapshot,
            )

            _write_json_atomic(
                self.ticket_path,
                ticket_payload.model_dump(mode="json"),
            )
            _write_json_atomic(
                self.metadata_path,
                metadata_payload.model_dump(mode="json"),
            )
            self._write_transcript_text_file()
            self._finalized = True
            return self.call_dir

    def _append_transcript_event(
        self,
        *,
        role: str,
        channel: str,
        text: str,
        **metadata: Any,
    ) -> None:
        normalized_text = str(text).strip()
        if not normalized_text:
            return

        event = {
            "timestamp": _utc_now(),
            "role": role,
            "channel": channel,
            "text": normalized_text,
        }
        for key, value in metadata.items():
            normalized_value = _normalize_optional_str(value)
            if normalized_value is not None:
                event[key] = normalized_value

        with self._lock:
            if self._finalized:
                return
            self._updated_at = event["timestamp"]
            self._transcript_handle.write(json.dumps(event, ensure_ascii=False) + "\n")
            if self._sync_writes:
                self._transcript_handle.flush()
                os.fsync(self._transcript_handle.fileno())
            self._transcript_events += 1

    def _append_audio(
        self,
        *,
        side: str,
        audio: bytes,
        sample_rate: int,
        num_channels: int,
    ) -> None:
        if not audio:
            return

        with self._lock:
            if self._finalized:
                return
            self._updated_at = _utc_now()
            target_rate = self._audio_rate_for_side(side) or sample_rate
            writer = self._ensure_audio_writer(
                side=side,
                sample_rate=target_rate,
                num_channels=num_channels,
            )
            normalized_audio = self._normalize_audio_for_storage(
                side=side,
                audio=audio,
                sample_rate=sample_rate,
                target_rate=target_rate,
            )
            if not normalized_audio:
                return
            writer.writeframesraw(normalized_audio)
            handle = (
                self._caller_audio_handle
                if side == "caller"
                else self._assistant_audio_handle
            )
            if handle is not None and self._sync_writes:
                handle.flush()

            if side == "caller":
                self._caller_audio_bytes += len(normalized_audio)
            else:
                self._assistant_audio_bytes += len(normalized_audio)

    def _ensure_audio_writer(
        self,
        *,
        side: str,
        sample_rate: int,
        num_channels: int,
    ):
        if side == "caller":
            wave_writer = self._caller_wave
            handle = self._caller_audio_handle
            path = self.caller_audio_path
        else:
            wave_writer = self._assistant_wave
            handle = self._assistant_audio_handle
            path = self.assistant_audio_path

        if wave_writer is not None and handle is not None:
            return wave_writer

        handle = open(path, "wb")
        wave_writer = wave.open(handle, "wb")
        wave_writer.setnchannels(num_channels)
        wave_writer.setsampwidth(2)
        wave_writer.setframerate(sample_rate)

        if side == "caller":
            self._caller_audio_handle = handle
            self._caller_wave = wave_writer
            self._caller_audio_rate = sample_rate
        else:
            self._assistant_audio_handle = handle
            self._assistant_wave = wave_writer
            self._assistant_audio_rate = sample_rate
        return wave_writer

    def _normalize_audio_for_storage(
        self,
        *,
        side: str,
        audio: bytes,
        sample_rate: int,
        target_rate: int,
    ) -> bytes:
        state = self._resample_state_for_side(side)
        previous_source_rate = self._resample_source_rate_for_side(side)
        prefix = b""

        if sample_rate == target_rate:
            if state is not None and previous_source_rate is not None:
                prefix, _ = _resample_pcm16_mono_linear(
                    b"",
                    src_rate=previous_source_rate,
                    dst_rate=target_rate,
                    state=state,
                    finalize=True,
                )
            self._clear_resample_state(side=side)
            return prefix + audio

        if (
            state is not None
            and previous_source_rate is not None
            and previous_source_rate != sample_rate
        ):
            prefix, _ = _resample_pcm16_mono_linear(
                b"",
                src_rate=previous_source_rate,
                dst_rate=target_rate,
                state=state,
                finalize=True,
            )
            state = None

        normalized_audio, state = _resample_pcm16_mono_linear(
            audio,
            src_rate=sample_rate,
            dst_rate=target_rate,
            state=state,
            finalize=False,
        )
        self._set_resample_state(
            side=side,
            state=state,
            source_rate=sample_rate,
        )
        return prefix + normalized_audio

    def _close_wav_writer(self, *, side: str) -> None:
        if side == "caller":
            wave_writer = self._caller_wave
            handle = self._caller_audio_handle
            self._caller_wave = None
            self._caller_audio_handle = None
        else:
            wave_writer = self._assistant_wave
            handle = self._assistant_audio_handle
            self._assistant_wave = None
            self._assistant_audio_handle = None

        if wave_writer is not None:
            wave_writer.close()
        if handle is not None:
            handle.flush()
            os.fsync(handle.fileno())
            handle.close()

    def _close_transcript_handle(self) -> None:
        if self._transcript_handle.closed:
            return
        self._transcript_handle.flush()
        os.fsync(self._transcript_handle.fileno())
        self._transcript_handle.close()

    def _close_telemetry_handles(self) -> None:
        for handle in self._telemetry_handles.values():
            if handle.closed:
                continue
            handle.flush()
            os.fsync(handle.fileno())
            handle.close()

    def _write_telemetry_event(self, channel: str, payload: dict[str, Any]) -> None:
        handle = self._telemetry_handles.get(channel)
        if handle is None:
            return
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
        if self._sync_writes:
            handle.flush()
            os.fsync(handle.fileno())
        self._category_counts[channel] = self._category_counts.get(channel, 0) + 1

    def _flush_resampler(self, *, side: str) -> None:
        state = self._resample_state_for_side(side)
        source_rate = self._resample_source_rate_for_side(side)
        target_rate = self._audio_rate_for_side(side)
        if state is None or source_rate is None or target_rate is None:
            return

        flushed_audio, _ = _resample_pcm16_mono_linear(
            b"",
            src_rate=source_rate,
            dst_rate=target_rate,
            state=state,
            finalize=True,
        )
        self._clear_resample_state(side=side)
        if not flushed_audio:
            return

        writer = self._ensure_audio_writer(
            side=side,
            sample_rate=target_rate,
            num_channels=DEFAULT_AUDIO_CHANNELS,
        )
        writer.writeframesraw(flushed_audio)
        handle = (
            self._caller_audio_handle
            if side == "caller"
            else self._assistant_audio_handle
        )
        if handle is not None and self._sync_writes:
            handle.flush()
        if side == "caller":
            self._caller_audio_bytes += len(flushed_audio)
        else:
            self._assistant_audio_bytes += len(flushed_audio)

    def _audio_rate_for_side(self, side: str) -> int | None:
        if side == "caller":
            return self._caller_audio_rate
        return self._assistant_audio_rate

    def _resample_state_for_side(self, side: str) -> _LinearResampleState | None:
        if side == "caller":
            return self._caller_resample_state
        return self._assistant_resample_state

    def _resample_source_rate_for_side(self, side: str) -> int | None:
        if side == "caller":
            return self._caller_resample_source_rate
        return self._assistant_resample_source_rate

    def _set_resample_state(
        self,
        *,
        side: str,
        state: _LinearResampleState | None,
        source_rate: int | None,
    ) -> None:
        if side == "caller":
            self._caller_resample_state = state
            self._caller_resample_source_rate = source_rate
        else:
            self._assistant_resample_state = state
            self._assistant_resample_source_rate = source_rate

    def _clear_resample_state(self, *, side: str) -> None:
        self._set_resample_state(side=side, state=None, source_rate=None)

    def _build_ticket_payload(
        self,
        *,
        call_status: str,
        finalized_at: str | None = None,
        runtime_snapshot: Any = None,
        flow_state: dict[str, Any] | None = None,
        tool_state: dict[str, Any] | None = None,
    ) -> LocalCallTicketPayload:
        flow_payload = dict(flow_state or {})
        tool_payload = dict(tool_state or {})
        end_reason = _normalize_optional_str(
            getattr(runtime_snapshot, "cancel_reason", None)
        )
        formal_ticket_id = _normalize_optional_str(tool_payload.get("ticket_id"))
        formal_ticket_summary = _normalize_optional_str(
            tool_payload.get("last_ticket_summary")
        )
        branch = _normalize_optional_str(flow_payload.get("branch"))
        active_dtmf_kind = _normalize_optional_str(flow_payload.get("active_dtmf_kind"))
        lookup_status = _normalize_optional_str(tool_payload.get("lookup_status"))
        verification_status = _normalize_optional_str(
            tool_payload.get("verification_status")
        )
        balance_verified = bool(tool_payload.get("balance_verified"))
        formal_ticket_created = bool(formal_ticket_id)

        disposition = (
            "in_progress"
            if call_status != "finalized"
            else _derive_disposition(
                end_reason=end_reason,
                branch=branch,
                active_dtmf_kind=active_dtmf_kind,
                formal_ticket_created=formal_ticket_created,
                lookup_status=lookup_status,
                verification_status=verification_status,
                balance_verified=balance_verified,
            )
        )

        return LocalCallTicketPayload(
            session_id=self.session_id,
            call_status=call_status,
            disposition=disposition,
            started_at=self._started_at,
            updated_at=self._updated_at,
            finalized_at=finalized_at,
            end_reason=end_reason,
            current_node=_normalize_optional_str(flow_payload.get("current_node")),
            branch=branch,
            active_dtmf_kind=active_dtmf_kind,
            follow_up_required=formal_ticket_created or branch == "human_handoff",
            formal_ticket_created=formal_ticket_created,
            formal_ticket_id=formal_ticket_id,
            formal_ticket_summary=formal_ticket_summary,
            caller_uri=_normalize_optional_str(tool_payload.get("caller_uri"))
            or self._caller_uri,
            caller_number=_normalize_optional_str(tool_payload.get("caller_number"))
            or self._caller_number,
            callback_number=_normalize_optional_str(tool_payload.get("callback_number")),
            contact_phone=_normalize_optional_str(tool_payload.get("contact_phone")),
            subscriber_identifier=_normalize_optional_str(
                tool_payload.get("subscriber_identifier")
            ),
            customer_id=_normalize_optional_str(tool_payload.get("customer_id")),
            customer_name=_normalize_optional_str(tool_payload.get("customer_name")),
            company_name=_normalize_optional_str(tool_payload.get("company_name")),
            account_id=_normalize_optional_str(tool_payload.get("account_id")),
            service_id=_normalize_optional_str(tool_payload.get("service_id")),
            service_type=_normalize_optional_str(tool_payload.get("service_type")),
            service_address=_normalize_optional_str(tool_payload.get("service_address")),
            lookup_status=lookup_status,
            lookup_message=_normalize_optional_str(tool_payload.get("lookup_message")),
            verification_status=verification_status,
            verification_reason=_normalize_optional_str(
                tool_payload.get("verification_reason")
            ),
            outage_status=_normalize_optional_str(tool_payload.get("outage_status")),
            outage_summary=_normalize_optional_str(tool_payload.get("outage_summary")),
            balance_verified=balance_verified,
        )

    def _build_metadata_payload(
        self,
        *,
        status: str,
        finalized_at: str | None = None,
        runtime_snapshot: Any = None,
        flow_state: dict[str, Any] | None = None,
        tool_state: dict[str, Any] | None = None,
        latency_state: Any = None,
        bridge_state: Any = None,
        egress_state: Any = None,
        tool_snapshot: Any = None,
    ) -> LocalCallMetadata:
        runtime_payload = {
            "state": _normalize_optional_str(getattr(runtime_snapshot, "state", None)),
            "cancel_reason": _normalize_optional_str(
                getattr(runtime_snapshot, "cancel_reason", None)
            ),
            "turn_index": int(getattr(runtime_snapshot, "turn_index", 0) or 0),
            "spoken_turns": int(getattr(runtime_snapshot, "spoken_turns", 0) or 0),
            "silent_turns": int(getattr(runtime_snapshot, "silent_turns", 0) or 0),
        }
        runtime_payload = {
            key: value
            for key, value in runtime_payload.items()
            if value not in (None, "")
        }

        flow_payload = {
            "current_node": _normalize_optional_str((flow_state or {}).get("current_node")),
            "branch": _normalize_optional_str((flow_state or {}).get("branch")),
            "active_dtmf_kind": _normalize_optional_str(
                (flow_state or {}).get("active_dtmf_kind")
            ),
            "last_tool_id": _normalize_optional_str((flow_state or {}).get("last_tool_id")),
        }
        flow_payload = {
            key: value
            for key, value in flow_payload.items()
            if value not in (None, "")
        }
        latency_payload = _build_latency_payload(latency_state)

        caller_uri = _normalize_optional_str((tool_state or {}).get("caller_uri")) or self._caller_uri
        caller_number = (
            _normalize_optional_str((tool_state or {}).get("caller_number"))
            or self._caller_number
        )
        artifacts = {
            "transcript_jsonl": self.transcript_jsonl_path.name,
            "transcript_txt": self.transcript_txt_path.name,
            "ticket_json": self.ticket_path.name,
            "metadata_json": self.metadata_path.name,
            "caller_audio": self.caller_audio_path.name,
            "assistant_audio": self.assistant_audio_path.name,
            "timeline_jsonl": str(self.timeline_path.relative_to(self.call_dir)),
            "control_jsonl": str(self.control_path.relative_to(self.call_dir)),
            "media_jsonl": str(self.media_path.relative_to(self.call_dir)),
            "turns_jsonl": str(self.turns_path.relative_to(self.call_dir)),
            "tools_jsonl": str(self.tools_path.relative_to(self.call_dir)),
        }
        if self.ticket_payload_path.exists():
            artifacts["ticket_payload_json"] = self.ticket_payload_path.name

        return LocalCallMetadata(
            session_id=self.session_id,
            status=status,
            started_at=self._started_at,
            updated_at=self._updated_at,
            finalized_at=finalized_at,
            caller_uri=caller_uri,
            caller_number=caller_number,
            runtime=runtime_payload,
            flow=flow_payload,
            latency=latency_payload,
            bridge=_build_bridge_payload(bridge_state),
            egress=_build_egress_payload(egress_state),
            tools=_build_tool_summary_payload(tool_snapshot),
            gateway=_build_gateway_payload(
                call_id=self._gateway_call_id,
                end_reason=self._gateway_end_reason,
                last_event=self._last_gateway_event,
                media_bridge_stats=self._gateway_media_bridge_stats,
            ),
            event_counts={
                "total": self._category_counts.get("timeline", 0),
                "by_category": {
                    key: self._category_counts.get(key, 0)
                    for key in ("control", "media", "turns", "tools")
                },
                "by_source": dict(sorted(self._source_counts.items())),
            },
            counters={
                "transcript_events": self._transcript_events,
                "caller_audio_bytes": self._caller_audio_bytes,
                "assistant_audio_bytes": self._assistant_audio_bytes,
                "caller_audio_sample_rate": self._caller_audio_rate
                or DEFAULT_AUDIO_SAMPLE_RATE,
                "assistant_audio_sample_rate": self._assistant_audio_rate
                or DEFAULT_AUDIO_SAMPLE_RATE,
            },
            artifacts=artifacts,
        )

    def _persist_status(self, *, call_status: str, disposition: str) -> None:
        ticket_payload = self._build_ticket_payload(
            call_status=call_status,
            finalized_at=None,
            runtime_snapshot=None,
            flow_state=None,
            tool_state=None,
        )
        ticket_payload.disposition = disposition
        metadata_payload = self._build_metadata_payload(
            status=call_status,
            finalized_at=None,
            runtime_snapshot=None,
            flow_state=None,
            tool_state=None,
            latency_state=None,
            bridge_state=None,
            egress_state=None,
            tool_snapshot=None,
        )
        _write_json_atomic(
            self.ticket_path,
            ticket_payload.model_dump(mode="json"),
        )
        _write_json_atomic(
            self.metadata_path,
            metadata_payload.model_dump(mode="json"),
        )

    def _write_transcript_text_file(self) -> None:
        lines: list[str] = []
        if self.transcript_jsonl_path.exists():
            with open(self.transcript_jsonl_path, "r", encoding="utf-8") as handle:
                for line in handle:
                    stripped = line.strip()
                    if not stripped:
                        continue
                    payload = json.loads(stripped)
                    role = str(payload.get("role", "")).strip()
                    text = str(payload.get("text", "")).strip()
                    if not text:
                        continue
                    label = "Caller" if role == "caller" else "Assistant"
                    lines.append(f"{label}: {text}")

        with open(self.transcript_txt_path, "w", encoding="utf-8") as handle:
            handle.write("\n".join(lines))
            handle.flush()
            os.fsync(handle.fileno())


def _derive_disposition(
    *,
    end_reason: str | None,
    branch: str | None,
    active_dtmf_kind: str | None,
    formal_ticket_created: bool,
    lookup_status: str | None,
    verification_status: str | None,
    balance_verified: bool,
) -> str:
    if formal_ticket_created:
        return "formal_ticket_created"
    if end_reason == "hangup_requested:worker_error":
        return "worker_error"
    if end_reason == "hangup_requested:assistant_completed":
        return "assistant_completed"
    if end_reason == "hangup_requested:no_speech_timeout":
        return "silent_abandonment"
    if end_reason == "hangup_requested:max_duration_timeout":
        return "max_duration_timeout"
    if active_dtmf_kind == "billing_verification":
        return "ended_during_balance_verification"
    if active_dtmf_kind in {"subscriber_identifier", "billing_identifier"}:
        return "ended_during_identifier_collection"
    if active_dtmf_kind == "callback_number":
        return "ended_during_callback_collection"
    if branch == "human_handoff":
        return "handoff_requested_without_ticket"
    if verification_status == "failed":
        return "balance_verification_failed"
    if lookup_status == "not_found":
        return "subscriber_not_found"
    if branch == "billing" and balance_verified:
        return "verified_billing_flow_ended"
    if branch == "technical_support":
        return "technical_support_flow_ended"
    return "call_ended"


def _build_latency_payload(latency_state: Any) -> dict[str, Any]:
    if latency_state is None:
        return {}

    fields = {
        "turns_started": getattr(latency_state, "turns_started", None),
        "turns_archived": getattr(latency_state, "turns_archived", None),
        "turns_with_llm_start": getattr(latency_state, "turns_with_llm_start", None),
        "turns_with_first_audio": getattr(
            latency_state,
            "turns_with_first_audio",
            None,
        ),
        "interruptions": getattr(latency_state, "interruptions", None),
        "last_turn_id": getattr(latency_state, "last_turn_id", None),
        "last_archive_reason": _normalize_optional_str(
            getattr(latency_state, "last_archive_reason", None)
        ),
        "last_user_speech_ms": getattr(latency_state, "last_user_speech_ms", None),
        "last_llm_started_after_user_stop_ms": getattr(
            latency_state,
            "last_llm_started_after_user_stop_ms",
            None,
        ),
        "last_first_tts_text_after_user_stop_ms": getattr(
            latency_state,
            "last_first_tts_text_after_user_stop_ms",
            None,
        ),
        "last_tts_started_after_user_stop_ms": getattr(
            latency_state,
            "last_tts_started_after_user_stop_ms",
            None,
        ),
        "last_first_audio_after_user_stop_ms": getattr(
            latency_state,
            "last_first_audio_after_user_stop_ms",
            None,
        ),
        "last_first_audio_after_llm_start_ms": getattr(
            latency_state,
            "last_first_audio_after_llm_start_ms",
            None,
        ),
        "last_bot_started_after_user_stop_ms": getattr(
            latency_state,
            "last_bot_started_after_user_stop_ms",
            None,
        ),
        "last_bot_speaking_ms": getattr(latency_state, "last_bot_speaking_ms", None),
        "avg_llm_started_after_user_stop_ms": getattr(
            latency_state,
            "avg_llm_started_after_user_stop_ms",
            None,
        ),
        "max_llm_started_after_user_stop_ms": getattr(
            latency_state,
            "max_llm_started_after_user_stop_ms",
            None,
        ),
        "avg_first_audio_after_user_stop_ms": getattr(
            latency_state,
            "avg_first_audio_after_user_stop_ms",
            None,
        ),
        "max_first_audio_after_user_stop_ms": getattr(
            latency_state,
            "max_first_audio_after_user_stop_ms",
            None,
        ),
    }
    return {
        key: value
        for key, value in fields.items()
        if value not in (None, "")
    }


def _build_bridge_payload(bridge_state: Any) -> dict[str, Any]:
    if bridge_state is None:
        return {}
    fields = {
        "media_ready": getattr(bridge_state, "media_ready", None),
        "audio_queue_depth": getattr(bridge_state, "audio_queue_depth", None),
        "audio_queue_high_watermark": getattr(
            bridge_state, "audio_queue_high_watermark", None
        ),
        "inbound_audio_chunks": getattr(bridge_state, "inbound_audio_chunks", None),
        "inbound_audio_bytes": getattr(bridge_state, "inbound_audio_bytes", None),
        "inbound_audio_drops": getattr(bridge_state, "inbound_audio_drops", None),
        "output_queue_depth": getattr(bridge_state, "output_queue_depth", None),
        "output_queue_high_watermark": getattr(
            bridge_state, "output_queue_high_watermark", None
        ),
        "output_audio_chunks_enqueued": getattr(
            bridge_state, "output_audio_chunks_enqueued", None
        ),
        "output_audio_bytes_enqueued": getattr(
            bridge_state, "output_audio_bytes_enqueued", None
        ),
        "output_audio_chunks_sent": getattr(
            bridge_state, "output_audio_chunks_sent", None
        ),
        "output_audio_bytes_sent": getattr(
            bridge_state, "output_audio_bytes_sent", None
        ),
        "output_flush_requested": getattr(
            bridge_state, "output_flush_requested", None
        ),
        "output_flush_processed": getattr(
            bridge_state, "output_flush_processed", None
        ),
        "output_clear_events": getattr(bridge_state, "output_clear_events", None),
        "output_queue_items_cleared": getattr(
            bridge_state, "output_queue_items_cleared", None
        ),
        "output_queue_drops": getattr(bridge_state, "output_queue_drops", None),
    }
    return {
        key: value
        for key, value in fields.items()
        if value not in (None, "")
    }


def _build_egress_payload(egress_state: Any) -> dict[str, Any]:
    if egress_state is None:
        return {}
    fields = {
        "output_audio_frames": getattr(egress_state, "output_audio_frames", None),
        "output_audio_bytes": getattr(egress_state, "output_audio_bytes", None),
        "transport_hangups": getattr(egress_state, "transport_hangups", None),
        "interruption_count": getattr(egress_state, "interruption_count", None),
        "speech_boundary_flushes": getattr(
            egress_state, "speech_boundary_flushes", None
        ),
        "terminal_flushes": getattr(egress_state, "terminal_flushes", None),
    }
    return {
        key: value
        for key, value in fields.items()
        if value not in (None, "")
    }


def _build_tool_summary_payload(tool_snapshot: Any) -> dict[str, Any]:
    if tool_snapshot is None:
        return {}
    fields = {
        "calls_started": getattr(tool_snapshot, "calls_started", None),
        "calls_succeeded": getattr(tool_snapshot, "calls_succeeded", None),
        "validation_failures": getattr(tool_snapshot, "validation_failures", None),
        "execution_failures": getattr(tool_snapshot, "execution_failures", None),
        "tickets_created": getattr(tool_snapshot, "tickets_created", None),
        "total_duration_ms": getattr(tool_snapshot, "total_duration_ms", None),
        "max_duration_ms": getattr(tool_snapshot, "max_duration_ms", None),
        "last_tool_id": _normalize_optional_str(
            getattr(tool_snapshot, "last_tool_id", None)
        ),
        "last_ticket_id": _normalize_optional_str(
            getattr(tool_snapshot, "last_ticket_id", None)
        ),
        "call_counts": _json_safe(getattr(tool_snapshot, "call_counts", None) or {}),
    }
    return {
        key: value
        for key, value in fields.items()
        if value not in (None, "", {})
    }


def _build_gateway_payload(
    *,
    call_id: int | None,
    end_reason: str | None,
    last_event: str | None,
    media_bridge_stats: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = {
        "call_id": call_id,
        "end_reason": _normalize_optional_str(end_reason),
        "last_event": _normalize_optional_str(last_event),
        "media_bridge_stats": _json_safe(media_bridge_stats or {}),
    }
    return {
        key: value
        for key, value in payload.items()
        if value not in (None, "", {})
    }


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.tmp")

    with open(tmp_path, "w", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, indent=2))
        handle.flush()
        os.fsync(handle.fileno())

    os.replace(tmp_path, path)

    dir_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _normalize_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    raw_value = getattr(value, "value", value)
    normalized = str(raw_value).strip()
    return normalized or None


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {
            str(key): _json_safe(item)
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(item) for item in value]
    return str(value)


def _isoformat_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _resolve_event_time(
    *,
    timestamp: str | None,
    ts_ms: int | None,
) -> tuple[str, int]:
    if ts_ms is not None:
        event_dt = datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=UTC)
        return _isoformat_utc(event_dt), int(ts_ms)
    normalized_timestamp = _normalize_optional_str(timestamp)
    if normalized_timestamp is not None:
        try:
            event_dt = datetime.fromisoformat(
                normalized_timestamp.replace("Z", "+00:00")
            )
            return _isoformat_utc(event_dt), int(event_dt.timestamp() * 1000)
        except ValueError:
            pass
    event_dt = datetime.now(UTC)
    return _isoformat_utc(event_dt), int(event_dt.timestamp() * 1000)


def _utc_now() -> str:
    return _isoformat_utc(datetime.now(UTC))
