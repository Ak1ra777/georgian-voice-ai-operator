from __future__ import annotations

import argparse
import json
import socket
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import SIP_CONTROL_SOCKET


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Drive a minimal Phase 1 Pipecat control-session against a running "
            "sip_main.py process."
        )
    )
    parser.add_argument(
        "--socket",
        default=SIP_CONTROL_SOCKET,
        help="Unix control socket path for the running Pipecat manager.",
    )
    parser.add_argument(
        "--session-id",
        default=f"manual-pipecat-smoke-{int(time.time())}",
        help="Session id to use for the smoke call.",
    )
    parser.add_argument(
        "--rx-port",
        type=int,
        default=43100,
        help="UDP RX port advertised to the worker.",
    )
    parser.add_argument(
        "--tx-port",
        type=int,
        default=43101,
        help="UDP TX port advertised to the worker.",
    )
    parser.add_argument(
        "--post-ready-wait-s",
        type=float,
        default=0.2,
        help="Delay between call_ready ACK and call_media_started.",
    )
    parser.add_argument(
        "--post-media-wait-s",
        type=float,
        default=1.0,
        help="Delay after call_media_started before call_end.",
    )
    parser.add_argument(
        "--skip-media-started",
        action="store_true",
        help="Only verify call_allocated -> call_ready -> call_end.",
    )
    return parser.parse_args()


def _send(sock: socket.socket, payload: dict) -> None:
    line = json.dumps(payload) + "\n"
    sock.sendall(line.encode("utf-8"))
    print(f"SENT {json.dumps(payload)}")


def _recv_line(sock: socket.socket, *, timeout: float) -> dict | None:
    sock.settimeout(timeout)
    buffer = b""
    while b"\n" not in buffer:
        chunk = sock.recv(4096)
        if not chunk:
            return None
        buffer += chunk
    line, _separator, _remainder = buffer.partition(b"\n")
    payload = json.loads(line.decode("utf-8"))
    print(f"RECV {json.dumps(payload)}")
    return payload


def main() -> int:
    args = _parse_args()

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.connect(args.socket)

        _send(
            sock,
            {
                "type": "call_allocated",
                "session_id": args.session_id,
                "rx_port": args.rx_port,
                "tx_port": args.tx_port,
            },
        )
        ready_payload = _recv_line(sock, timeout=2.0)
        if ready_payload is None:
            print("ERROR no response after call_allocated", file=sys.stderr)
            return 1
        if ready_payload.get("type") != "call_ready":
            print(
                f"ERROR expected call_ready, got {ready_payload.get('type')}",
                file=sys.stderr,
            )
            return 1

        _send(
            sock,
            {
                "type": "control_ack",
                "seq": ready_payload["seq"],
                "command": ready_payload["type"],
                "session_id": args.session_id,
            },
        )

        time.sleep(max(0.0, args.post_ready_wait_s))

        if not args.skip_media_started:
            _send(
                sock,
                {
                    "type": "call_media_started",
                    "session_id": args.session_id,
                },
            )
            time.sleep(max(0.0, args.post_media_wait_s))

        _send(
            sock,
            {
                "type": "call_end",
                "session_id": args.session_id,
            },
        )

        try:
            trailing_payload = _recv_line(sock, timeout=0.5)
        except TimeoutError:
            trailing_payload = None
        print(f"FINAL {json.dumps(trailing_payload)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
