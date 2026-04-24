#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


TIMESTAMP_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})")
SESSION_RE = re.compile(r"\bsession_id=([0-9a-f]+)\b")
EVENT_RE = re.compile(r"\bevent=([a-z_]+)\b")
LATENCY_FIELD_RE = re.compile(r"\b([a-z_]+_ms)=([0-9]+(?:\.[0-9]+)?)\b")


@dataclass(frozen=True)
class LatencyMeasurement:
    label: str
    duration_s: float
    source_line: str
    target_line: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize the latest live SIP call from manager and gateway logs."
    )
    parser.add_argument(
        "--manager-log",
        default="outputs/pipecat_manager.log",
        help="Path to the Python manager log.",
    )
    parser.add_argument(
        "--gateway-log",
        default="outputs/gateway.log",
        help="Path to the SIP gateway log.",
    )
    parser.add_argument(
        "--session-id",
        default=None,
        help="Explicit session_id to summarize. Defaults to the latest call found in manager or gateway logs.",
    )
    parser.add_argument(
        "--include-periodic",
        action="store_true",
        help="Include periodic telemetry lines instead of only start/final snapshots.",
    )
    return parser.parse_args()


def read_lines(path: Path) -> list[str]:
    if not path.is_file():
        raise SystemExit(f"Log file not found: {path}")
    return path.read_text(encoding="utf-8", errors="replace").splitlines()


def detect_latest_session_id(lines: list[str]) -> str | None:
    session_id = None
    for line in lines:
        if "call_allocated session_id=" not in line:
            continue
        match = SESSION_RE.search(line)
        if match:
            session_id = match.group(1)
    return session_id


def detect_latest_session_id_from_lines(lines: list[str]) -> str | None:
    session_id = None
    for line in lines:
        match = SESSION_RE.search(line)
        if match:
            session_id = match.group(1)
    return session_id


def find_session_block(lines: list[str], session_id: str) -> tuple[int, int]:
    start = 0
    found_start = False
    for index, line in enumerate(lines):
        if f"call_allocated session_id={session_id}" in line:
            start = index
            found_start = True
    end = len(lines)
    for index in range(start, len(lines)):
        if f"call_end session_id={session_id}" in lines[index]:
            end = index + 1
            break
    if found_start:
        return start, end

    indices = [
        index for index, line in enumerate(lines) if f"session_id={session_id}" in line
    ]
    if not indices:
        return 0, 0
    return indices[0], indices[-1] + 1


def detect_session_id(manager_lines: list[str], gateway_lines: list[str]) -> tuple[str | None, str]:
    session_id = detect_latest_session_id(manager_lines)
    if session_id:
        return session_id, "manager"
    session_id = detect_latest_session_id_from_lines(gateway_lines)
    if session_id:
        return session_id, "gateway"
    return None, "none"


def has_session_lines(lines: list[str], session_id: str) -> bool:
    return any(f"session_id={session_id}" in line for line in lines)


def should_keep_manager_line(line: str, include_periodic: bool) -> bool:
    if not include_periodic and (
        "pipecat_call_audio_stats event=periodic" in line
        or "pipecat_tool_stats event=periodic" in line
    ):
        return False

    patterns = (
        "control_connected",
        "control_disconnected",
        "control_command_",
        "call_allocated",
        "call_media_started",
        "call_end ",
        "pipecat_call_worker_",
        "pipecat_stt_profile_override",
        "pipecat_greeting_sent",
        "pipecat_audio_interruption",
        "pipecat_gateway_hangup_requested",
        "pipecat_tool_call_",
        "pipecat_call_audio_stats",
        "pipecat_tool_stats",
        "pipecat_turn_timing",
        "User started speaking",
        "User stopped speaking",
        "Generating chat",
        "Generating TTS",
    )
    return any(pattern in line for pattern in patterns)


def should_keep_gateway_line(line: str, include_periodic: bool) -> bool:
    if not include_periodic and "call_media_bridge_stats event=periodic" in line:
        return False

    patterns = (
        "Registration:",
        "control_connected",
        "call_media_started",
        "call_media_bridge_stats",
        "call_end ",
        "[DISCONNECTED]",
    )
    return any(pattern in line for pattern in patterns)


def parse_timestamp(line: str) -> datetime | None:
    match = TIMESTAMP_RE.match(line)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S.%f")


def build_latency_summary(lines: list[str]) -> list[LatencyMeasurement]:
    results: list[LatencyMeasurement] = []
    last_user_stop: tuple[datetime, str] | None = None
    last_chat_start: tuple[datetime, str] | None = None

    for line in lines:
        if "pipecat_turn_timing " in line:
            event_match = EVENT_RE.search(line)
            if event_match:
                event = event_match.group(1)
                latencies = {key: float(value) for key, value in LATENCY_FIELD_RE.findall(line)}
                if event == "llm_started" and "since_user_stop_ms" in latencies:
                    results.append(
                        LatencyMeasurement(
                            label="user_stop_to_llm_start",
                            duration_s=latencies["since_user_stop_ms"] / 1000.0,
                            source_line=line,
                            target_line=line,
                        )
                    )
                elif event == "first_tts_text" and "since_user_stop_ms" in latencies:
                    results.append(
                        LatencyMeasurement(
                            label="user_stop_to_first_tts_text",
                            duration_s=latencies["since_user_stop_ms"] / 1000.0,
                            source_line=line,
                            target_line=line,
                        )
                    )
                elif event == "first_audio" and "since_user_stop_ms" in latencies:
                    results.append(
                        LatencyMeasurement(
                            label="user_stop_to_first_audio",
                            duration_s=latencies["since_user_stop_ms"] / 1000.0,
                            source_line=line,
                            target_line=line,
                        )
                    )

        timestamp = parse_timestamp(line)
        if timestamp is None:
            continue

        if "User stopped speaking" in line:
            last_user_stop = (timestamp, line)
        elif "Generating chat" in line and last_user_stop is not None:
            results.append(
                LatencyMeasurement(
                    label="user_stop_to_llm_start",
                    duration_s=(timestamp - last_user_stop[0]).total_seconds(),
                    source_line=last_user_stop[1],
                    target_line=line,
                )
            )
            last_chat_start = (timestamp, line)
        elif "Generating TTS" in line and last_chat_start is not None:
            results.append(
                LatencyMeasurement(
                    label="llm_start_to_first_tts",
                    duration_s=(timestamp - last_chat_start[0]).total_seconds(),
                    source_line=last_chat_start[1],
                    target_line=line,
                )
            )
            last_chat_start = None

    return results


def print_section(title: str, lines: list[str]) -> None:
    print(f"\n== {title} ==")
    if not lines:
        print("(no matching lines)")
        return
    for line in lines:
        print(line)


def main() -> None:
    args = parse_args()
    manager_path = Path(args.manager_log)
    gateway_path = Path(args.gateway_log)

    manager_lines = read_lines(manager_path)
    gateway_lines = read_lines(gateway_path)

    auto_session_id, session_source = detect_session_id(manager_lines, gateway_lines)
    session_id = args.session_id or auto_session_id
    if not session_id:
        raise SystemExit(
            "Could not find any session_id in the manager or gateway logs."
        )

    manager_start, manager_end = find_session_block(manager_lines, session_id)
    manager_block = manager_lines[manager_start:manager_end]
    manager_view = [
        line
        for line in manager_block
        if should_keep_manager_line(line, include_periodic=args.include_periodic)
    ]
    gateway_view = [
        line
        for line in gateway_lines
        if f"session_id={session_id}" in line
        and should_keep_gateway_line(line, include_periodic=args.include_periodic)
    ]

    print(f"session_id={session_id}")
    print(f"manager_log={manager_path}")
    print(f"gateway_log={gateway_path}")
    print(f"session_source={session_source}")
    if not has_session_lines(manager_lines, session_id):
        print("manager_log_warning=no_session_lines_found")
    elif len(manager_lines) <= 5:
        print("manager_log_warning=too_small_to_be_useful")

    latencies = build_latency_summary(manager_block)
    print_section("Manager Timeline", manager_view)
    print_section("Gateway Timeline", gateway_view)

    print("\n== Latency Summary ==")
    if not latencies:
        print("(no timestamp pairs found)")
    else:
        for item in latencies:
            print(f"{item.label}={item.duration_s:.3f}s")


if __name__ == "__main__":
    main()
