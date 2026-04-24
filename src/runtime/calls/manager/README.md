# Manager Package

## Purpose

`src/runtime/calls/manager/` contains the control-plane implementation behind
`CallManager`.

The manager is responsible for coordination across sessions, not speech
execution inside a single call.

## Responsibilities

- own the Unix control socket lifecycle
- validate and accept authorized peers
- parse gateway events
- queue outbound control commands and retry them until ACKed
- maintain the live worker/runtime registry
- route gateway events to the correct session action

## Modules

- `settings.py`
  Normalized manager configuration from `src.config`.
- `socket_server.py`
  Socket path preparation, peer auth, reconnect wait, send/receive loop.
- `protocol.py`
  Typed control-plane events and outbound payload builders.
- `command_queue.py`
  ACK/retry queue for `call_ready` and `call_hangup`.
- `session_registry.py`
  Live `session_id -> worker/runtime` ownership.
- `event_router.py`
  Event dispatch from parsed control messages to registry and queue actions.

## Rules

- The manager should not contain STT, TTS, dialog, or turn-level policy logic.
- Session registry code should not know about socket framing.
- Socket transport code should not know how a call worker runs internally.

## When Adding New Behavior

- New gateway event type: start in `protocol.py`, then route it in
  `event_router.py`.
- New manager command with ACK/retry behavior: add it in `command_queue.py`.
- New session bookkeeping concern: add it in `session_registry.py`.

If a change needs to touch all manager modules, the design is probably drifting
too much toward hidden coupling.
