# Repository Guidelines

## Project Structure & Module Organization

`src/` contains the Python application. The active runtime path on this branch is `src/pipecat_runtime/` plus the lower-level seams in `src/runtime/`. Keep reusable contracts in `src/tools/` and ISP-specific business logic in `src/packs/isp/`. The native SIP/media process lives in `native/sip_gateway/`. Tests are under `tests/`, checked-in audio assets under `assets/audio/`, and generated artifacts under `outputs/`.

## Build, Test, and Development Commands

Create the local environment with `python3.12 -m venv .venv && . .venv/bin/activate`, then install dependencies with `python -m pip install -r requirements.txt`. Build the SIP gateway with `make -C native/sip_gateway`; set `PJPROJECT_DIR=/absolute/path/to/pjproject` first if needed. Run the Python manager with `.venv/bin/python sip_main.py` and the native gateway with `./native/sip_gateway/sip_gateway`. Run the automated suite with `.venv/bin/pytest -q`.

## Coding Style & Naming Conventions

Use 4-space indentation and follow existing Python style. Prefer `snake_case` for modules, functions, and variables; use `PascalCase` for classes. Keep `call_manager` and worker entrypoints thin composition roots. Do not move business logic into runtime orchestration: media and session concerns belong in `src/runtime/` or `src/pipecat_runtime/`, tool contracts in `src/tools/`, and ISP flow rules in `src/packs/isp/`. No formatter or linter config is checked in, so match nearby code closely.

## Testing Guidelines

This repository uses `pytest`. Name tests `tests/test_*.py` and keep each file focused on one boundary, such as call orchestration, protocol handling, or ISP integrations. Prefer deterministic tests that do not require live provider calls. Keep manual smoke scripts out of normal pytest discovery unless they become deterministic.

## Commit & Pull Request Guidelines

Recent history uses concise, imperative subjects with prefixes such as `feat:`, `fix:`, and `chore:`. Follow that pattern. Pull requests should describe the behavior change, list the commands run for verification, link related issues when relevant, and include logs or call-flow notes for telephony or runtime changes.

## Configuration Tips

Copy `.env.example` to `.env` for local setup. Export variables before launching the native gateway; the C++ process does not read `.env` unless the shell exports those values.
