# SIP Gateway

## Purpose

`native/sip_gateway/` is the C++ SIP/media gateway for the application.

It terminates SIP calls with PJSUA2, bridges call audio to Python over per-call
UDP PCM ports, and exchanges control-plane messages with the Python manager
over a Unix socket.

Python owns dialog, STT, TTS, and worker policy. This package owns SIP
signaling, RTP/media attachment, and the real-time bridge into the Python
runtime.

## Responsibilities

- start the PJSUA2 endpoint and register the SIP account
- accept inbound calls and enforce `MAX_CALLS` backpressure
- allocate canonical `session_id` values and per-call UDP port pairs
- run the two-phase call handshake:
  `180 Ringing` -> `call_allocated` -> `call_ready` -> `200 OK`
- bridge call media between `8 kHz` SIP audio and `16 kHz` Python UDP audio
- emit `call_media_started`, `call_end`, and control ACK messages
- handle timeout-driven cleanup when Python never becomes ready or disconnect
  callbacks are missed

## Directory Layout

- `core/sip_gateway.cpp`
  Main executable, env-config loading, call lifecycle, and orchestration.
- `core/call_session.hpp`
  Per-call state container.
- `core/task_queue.hpp`
  Simple callback deferral queue so PJSUA2 callbacks stay lightweight.
- `control/control_channel.*`
  Resilient Unix socket client for newline-delimited JSON control messages.
- `media/udp_audio_port.*`
  `AudioMediaPort` bridge with `8 kHz <-> 16 kHz` conversion, jitter rings,
  and UDP worker threads.
- `util/port_allocator.*`
  Bind-checked UDP port-pair reservation.
- `Makefile`
  Local build entrypoint against `pjproject`.

## Call Lifecycle

1. Start the SIP endpoint, create the configured transport/account, and connect
   the control channel.
2. On inbound call, reserve a UDP port pair and create a `CallSession`.
3. Send `180 Ringing` and emit `call_allocated` to Python.
4. Wait for both Python readiness and active call media.
5. Send `200 OK`, start the UDP media bridge, and emit `call_media_started`.
6. On hangup, timeout, or disconnect, stop media, release ports, and emit
   `call_end`.

## Control Protocol Boundary

Outbound events to Python:

- `call_allocated`
- `call_media_started`
- `call_end`
- `control_ack`

Inbound commands from Python:

- `call_ready`
- `call_hangup`

## Source Of Truth

- `.cpp`, `.hpp`, and `Makefile` are source files.
- `sip_gateway`, `*.o`, and `*.d` in this directory are generated build
  artifacts from local compilation and should not be treated as the design
  source of truth.

## Rules

- Keep SIP callbacks lightweight; post work into `TaskQueue`.
- Keep socket framing/parsing in `control/`, media conversion in `media/`, and
  port allocation in `util/`.
- Do not move dialog policy, STT/TTS provider code, or worker retry logic into
  this package.
