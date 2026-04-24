# Core Package

## Purpose

`native/sip_gateway/core/` is the C++ gateway composition root.

It ties together SIP callbacks, per-call session bookkeeping, the control
channel, and the UDP audio bridge.

## Files

- `sip_gateway.cpp`
  Main executable, environment parsing, PJSUA2 bootstrap, session map, timeout
  handling, and gateway orchestration.
- `call_session.hpp`
  Per-call state container shared across the gateway lifecycle.
- `task_queue.hpp`
  Lightweight callback deferral queue so PJSUA2 callbacks do not do heavy work
  inline.

## Rules

- Keep PJSUA2 callbacks lightweight and push real work through `TaskQueue`.
- Keep socket framing in `../control/` and audio conversion/bridging in
  `../media/`.
- Do not move dialog or provider-specific behavior into this package.
