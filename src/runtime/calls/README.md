# Calls Package

## Purpose

`src/runtime/calls/` is the canonical layer-1 orchestration boundary for live
calls.

It owns:

- the manager control plane
- the per-call worker runtime
- session lifecycle state

It does not own dialog flow or business tools. Those are composed into the
worker from Layer 2 and Layer 3.

## Mental Model

There are two top-level runtime roles:

- `CallManager`
  Owns the Unix control socket and session registry.
- `CallWorker`
  Owns one live call session and runs the graph-native media/dialog loop.

`CallWorker` is a composition root, not the place where support flow logic
should live.

## Lifecycle

1. `sip_main.py` creates `CallManager`.
2. `CallManager` listens for gateway events.
3. `call_allocated` starts one `CallWorker`.
4. The worker builds media resources, runtime capabilities, the graph engine,
   and worker-bound tool adapters.
5. `CallTurnRunner` executes directives until timeout, hangup, or graph-owned
   closeout.

## Package Layout

- `call_manager.py`
  Manager-side entrypoint and composition root.
- `call_worker.py`
  Worker-side entrypoint and composition root.
- `manager/`
  Control-plane implementation details.
- `session/`
  Shared session lifecycle helpers.
- `worker/`
  Per-call runtime collaborators.

## Ownership Rules

- Keep `call_manager.py` thin and focused on manager wiring.
- Keep `call_worker.py` thin and focused on worker wiring.
- Put concrete runtime behavior under `worker/`.
- Put control-socket and registry behavior under `manager/`.
- Keep graph/dialog flow in `src/dialog/`.
- Keep business integrations behind `src/tools/` contracts.

## What Not To Do

- Do not grow `call_manager.py` into a socket/state-machine blob.
- Do not grow `call_worker.py` into a support-flow blob.
- Do not move business wiring or graph transition logic back into Layer 1.
