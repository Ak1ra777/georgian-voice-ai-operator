from __future__ import annotations

from typing import Any, Callable

from src.runtime.calls.session.runtime import SessionRuntimeSnapshot


def log_pipecat_call_snapshot(
    *,
    session_id: str,
    bridge,
    egress,
    turn_timing,
    runtime_snapshot: SessionRuntimeSnapshot,
    event: str,
    tool_runtime=None,
    media_event_recorder: Callable[[str, dict[str, Any]], None] | None = None,
    tool_event_recorder: Callable[[str, dict[str, Any]], None] | None = None,
) -> None:
    bridge_snapshot = _safe_snapshot(bridge)
    egress_snapshot = _safe_snapshot(egress)
    turn_timing_snapshot = _safe_snapshot(turn_timing)
    tool_snapshot = _safe_snapshot(tool_runtime)
    audio_stream_stats = _safe_bridge_stats(bridge, "audio_stream_stats")
    sink_stats = _safe_bridge_stats(bridge, "sink_stats")

    print(
        "pipecat_call_audio_stats "
        f"event={event} session_id={session_id} "
        f"runtime_state={runtime_snapshot.state.value} "
        f"runtime_cancel_reason={runtime_snapshot.cancel_reason or '-'} "
        f"bridge_media_ready={_bool_value(_snapshot_value(bridge_snapshot, 'media_ready', False))} "
        f"bridge_rx_chunks={_snapshot_value(bridge_snapshot, 'inbound_audio_chunks', 0)} "
        f"bridge_rx_bytes={_snapshot_value(bridge_snapshot, 'inbound_audio_bytes', 0)} "
        f"bridge_rx_drops={_snapshot_value(bridge_snapshot, 'inbound_audio_drops', 0)} "
        f"bridge_rx_qdepth={_snapshot_value(bridge_snapshot, 'audio_queue_depth', 0)} "
        f"bridge_rx_qwm={_snapshot_value(bridge_snapshot, 'audio_queue_high_watermark', 0)} "
        f"rx_packets={_stats_value(audio_stream_stats, 'packets_received', 0)} "
        f"rx_dropped={_stats_value(audio_stream_stats, 'packets_dropped_queue_full', 0)} "
        f"rx_rejected={_stats_value(audio_stream_stats, 'packets_rejected_source', 0)} "
        f"rx_jitter_avg_ms={float(_stats_value(audio_stream_stats, 'inter_arrival_jitter_ms_avg', 0.0)):.2f} "
        f"rx_jitter_max_ms={float(_stats_value(audio_stream_stats, 'inter_arrival_jitter_ms_max', 0.0)):.2f} "
        f"rx_gap_events={_stats_value(audio_stream_stats, 'inter_arrival_gap_events', 0)} "
        f"rx_burst_events={_stats_value(audio_stream_stats, 'inter_arrival_burst_events', 0)} "
        f"rx_chunks={_stats_value(audio_stream_stats, 'chunks_yielded', 0)} "
        f"bridge_tx_enqueued={_snapshot_value(bridge_snapshot, 'output_audio_chunks_enqueued', 0)} "
        f"bridge_tx_enqueued_bytes={_snapshot_value(bridge_snapshot, 'output_audio_bytes_enqueued', 0)} "
        f"bridge_tx_sent={_snapshot_value(bridge_snapshot, 'output_audio_chunks_sent', 0)} "
        f"bridge_tx_sent_bytes={_snapshot_value(bridge_snapshot, 'output_audio_bytes_sent', 0)} "
        f"bridge_tx_flush_requested={_snapshot_value(bridge_snapshot, 'output_flush_requested', 0)} "
        f"bridge_tx_flush_processed={_snapshot_value(bridge_snapshot, 'output_flush_processed', 0)} "
        f"bridge_tx_clears={_snapshot_value(bridge_snapshot, 'output_clear_events', 0)} "
        f"bridge_tx_items_cleared={_snapshot_value(bridge_snapshot, 'output_queue_items_cleared', 0)} "
        f"bridge_tx_qdepth={_snapshot_value(bridge_snapshot, 'output_queue_depth', 0)} "
        f"bridge_tx_qwm={_snapshot_value(bridge_snapshot, 'output_queue_high_watermark', 0)} "
        f"bridge_tx_drops={_snapshot_value(bridge_snapshot, 'output_queue_drops', 0)} "
        f"tx_frames={_stats_value(sink_stats, 'frames_sent_total', 0)} "
        f"tx_stream_frames={_stats_value(sink_stats, 'stream_frames_sent_total', 0)} "
        f"tx_stream_chunks={_stats_value(sink_stats, 'stream_chunks_in', 0)} "
        f"tx_flushes={_stats_value(sink_stats, 'stream_flushes', 0)} "
        f"tx_padded={_stats_value(sink_stats, 'partial_frames_padded', 0)} "
        f"egress_frames={_snapshot_value(egress_snapshot, 'output_audio_frames', 0)} "
        f"egress_bytes={_snapshot_value(egress_snapshot, 'output_audio_bytes', 0)} "
        f"egress_interruptions={_snapshot_value(egress_snapshot, 'interruption_count', 0)} "
        f"egress_speech_flushes={_snapshot_value(egress_snapshot, 'speech_boundary_flushes', 0)} "
        f"egress_terminal_flushes={_snapshot_value(egress_snapshot, 'terminal_flushes', 0)} "
        f"egress_transport_hangups={_snapshot_value(egress_snapshot, 'transport_hangups', 0)} "
        f"latency_turns_started={_snapshot_value(turn_timing_snapshot, 'turns_started', 0)} "
        f"latency_turns_archived={_snapshot_value(turn_timing_snapshot, 'turns_archived', 0)} "
        f"latency_interruptions={_snapshot_value(turn_timing_snapshot, 'interruptions', 0)} "
        f"latency_last_first_audio_ms={_snapshot_value(turn_timing_snapshot, 'last_first_audio_after_user_stop_ms', '-')} "
        f"latency_avg_first_audio_ms={_snapshot_value(turn_timing_snapshot, 'avg_first_audio_after_user_stop_ms', '-')} "
        f"latency_max_first_audio_ms={_snapshot_value(turn_timing_snapshot, 'max_first_audio_after_user_stop_ms', '-')}"
    )
    if media_event_recorder is not None:
        try:
            media_event_recorder(
                event,
                {
                    "runtime_state": runtime_snapshot.state.value,
                    "runtime_cancel_reason": runtime_snapshot.cancel_reason,
                    "bridge_snapshot": _snapshot_to_dict(bridge_snapshot),
                    "audio_stream_stats": audio_stream_stats or {},
                    "sink_stats": sink_stats or {},
                    "egress_snapshot": _snapshot_to_dict(egress_snapshot),
                    "turn_timing_snapshot": _snapshot_to_dict(turn_timing_snapshot),
                },
            )
        except Exception:
            pass

    if tool_snapshot is None:
        return

    call_counts = _snapshot_value(tool_snapshot, "call_counts", {})
    counts_text = "-"
    if isinstance(call_counts, dict) and call_counts:
        counts_text = ",".join(
            f"{tool_id}:{call_counts[tool_id]}"
            for tool_id in sorted(call_counts)
        )

    print(
        "pipecat_tool_stats "
        f"event={event} session_id={session_id} "
        f"calls_started={_snapshot_value(tool_snapshot, 'calls_started', 0)} "
        f"calls_succeeded={_snapshot_value(tool_snapshot, 'calls_succeeded', 0)} "
        f"validation_failures={_snapshot_value(tool_snapshot, 'validation_failures', 0)} "
        f"execution_failures={_snapshot_value(tool_snapshot, 'execution_failures', 0)} "
        f"tickets_created={_snapshot_value(tool_snapshot, 'tickets_created', 0)} "
        f"total_duration_ms={_snapshot_value(tool_snapshot, 'total_duration_ms', 0)} "
        f"max_duration_ms={_snapshot_value(tool_snapshot, 'max_duration_ms', 0)} "
        f"last_tool_id={_snapshot_value(tool_snapshot, 'last_tool_id', '-') or '-'} "
        f"last_ticket_id={_snapshot_value(tool_snapshot, 'last_ticket_id', '-') or '-'} "
        f"call_counts={counts_text}"
    )
    if tool_event_recorder is not None:
        try:
            tool_event_recorder(
                event,
                {
                    "calls_started": _snapshot_value(tool_snapshot, "calls_started", 0),
                    "calls_succeeded": _snapshot_value(tool_snapshot, "calls_succeeded", 0),
                    "validation_failures": _snapshot_value(
                        tool_snapshot, "validation_failures", 0
                    ),
                    "execution_failures": _snapshot_value(
                        tool_snapshot, "execution_failures", 0
                    ),
                    "tickets_created": _snapshot_value(tool_snapshot, "tickets_created", 0),
                    "total_duration_ms": _snapshot_value(
                        tool_snapshot, "total_duration_ms", 0
                    ),
                    "max_duration_ms": _snapshot_value(tool_snapshot, "max_duration_ms", 0),
                    "last_tool_id": _snapshot_value(tool_snapshot, "last_tool_id", None),
                    "last_ticket_id": _snapshot_value(
                        tool_snapshot, "last_ticket_id", None
                    ),
                    "call_counts": call_counts if isinstance(call_counts, dict) else {},
                },
            )
        except Exception:
            pass


def _safe_snapshot(component):
    if component is None:
        return None
    snapshot = getattr(component, "snapshot", None)
    if not callable(snapshot):
        return None
    try:
        return snapshot()
    except Exception as ex:
        print(
            "pipecat_telemetry_snapshot_failed "
            f"component={type(component).__name__} error={ex}"
        )
        return None


def _safe_bridge_stats(bridge, method_name: str):
    if bridge is None:
        return None
    stats_method = getattr(bridge, method_name, None)
    if not callable(stats_method):
        return None
    try:
        return stats_method()
    except Exception as ex:
        print(
            "pipecat_telemetry_stats_failed "
            f"component={type(bridge).__name__} method={method_name} error={ex}"
        )
        return None


def _snapshot_value(snapshot, field_name: str, default):
    if snapshot is None:
        return default
    return getattr(snapshot, field_name, default)


def _stats_value(stats: dict | None, key: str, default):
    if not isinstance(stats, dict):
        return default
    return stats.get(key, default)


def _bool_value(value: bool) -> str:
    return "true" if value else "false"


def _snapshot_to_dict(snapshot) -> dict[str, Any]:
    if snapshot is None:
        return {}
    return {
        key: value
        for key, value in vars(snapshot).items()
        if value not in (None, "")
    }
