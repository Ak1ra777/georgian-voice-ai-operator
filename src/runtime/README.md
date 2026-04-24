# Runtime Package

## Purpose

`src/runtime/` contains the cross-cutting Layer-1 contracts and helpers that
sit above concrete call workers but below dialog.

## Files And Subpackages

- `capabilities.py`
  Protocols for audio, speech, DTMF, and call-control capabilities.
- `worker_capabilities.py`
  Concrete worker-backed adapters that implement those protocols.
- `directives.py`
  `RuntimeDirectiveExecutor`, which maps Layer-2 directives onto Layer-1
  capabilities.
- [`calls/`](calls/README.md)
  Live call manager/worker implementation.
- [`media/`](media/README.md)
  Audio, STT, TTS, VAD, and turn-pipeline helpers.

## Runtime State

`src/runtime/calls/session_state.py` defines the mutable per-session runtime
state object re-exported from `src.runtime`.

## Rules

- Keep Layer-2 directive execution here rather than inside dialog nodes.
- Keep worker/session lifecycle ownership in `calls/`.
- Keep provider-specific media behavior in `media/`.
