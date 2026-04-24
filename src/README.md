# Src Package

## Purpose

`src/` is the Python application source tree for the voice assistant.

The codebase follows a layered runtime shape:

- Layer 1: [`runtime/`](runtime/README.md)
  Call lifecycle, media execution, capability adapters, and directive
  execution.
- Layer 2: [`dialog/`](dialog/README.md)
  Graph state, nodes, transitions, and model-driven orchestration.
- Layer 3: [`tools/`](tools/README.md)
  Generic tool contracts and executable tool metadata.

## Supporting Modules

- [`app/`](app/README.md)
  Default composition factories for the active graph, model gateway, and tool
  bundle.
- [`evals/`](evals/README.md)
  Generic offline scenario manifests and runners for compiled flows and agent
  profiles.
- [`packs/isp/`](packs/isp/README.md)
  First-party Georgian ISP reference pack built on the platform layers.
- `config.py`
  Environment loading, parsing, and validation.

## Rules

- Preserve the layer boundaries when adding behavior.
- Keep runtime/media concerns out of `dialog/`.
- Keep business integrations out of `runtime/`.
