# Tools Package

## Purpose

`src/tools/` defines the Layer-3 contract surface exposed to graph nodes.

This package owns only reusable tool contracts and registry metadata.
Business-specific tool implementations belong in vertical packs such as
`src/packs/isp/`. Runtime-specific binding still lives in Layer 1 under
`src/runtime/calls/worker/tool_adapters.py`.

## Modules

- `contracts.py`
  Tool protocols and the `ToolSet` bundle passed into graph nodes.

## Rules

- Keep tool interfaces here.
- Keep runtime/session binding out of this package.
- Keep pack-specific providers and payload logic out of this package.
- Do not import `src/runtime/calls/worker` or other Layer-1 runtime modules
  here.
