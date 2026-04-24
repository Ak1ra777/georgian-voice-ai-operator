# ISP Pack

## Purpose

`src/packs/isp/` contains the first-party Georgian ISP reference pack built on
top of the generic runtime, dialog engine, and tool-contract layers.

## Ownership

- ISP flow manifests and reusable components
- ISP tool schemas and payload conventions
- ISP default stub integrations for local development
- ISP ticket persistence helpers

## Rules

- Keep ISP prompts, routing policy, and payload semantics here.
- Do not move generic engine or runtime primitives into this pack.
- Promote reusable behavior back into the platform only when it is clearly
  useful outside the ISP domain.
