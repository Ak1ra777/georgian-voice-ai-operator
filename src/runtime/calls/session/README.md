# Session Package

## Purpose

`src/runtime/calls/session/` owns runtime lifecycle helpers that are shared
across the worker runtime.

This package does not own graph/dialog state. Graph state lives in
`src/dialog/`.

## Modules

- `runtime.py`
  Session lifecycle state machine, cancellation token, retry execution, turn
  counters, and turn-loop helpers.
- `src/dialog/conversation_buffer.py`
  A lightweight conversation message buffer used by generic pipeline helpers and
  kept in sync with graph-owned message history during live calls.

## Rules

- Keep live call lifecycle state here.
- Keep graph flow state out of this package.
- Do not put worker media or tool binding behavior here.
