# Native

## Purpose

`native/` contains compiled components that sit beside the Python runtime.

The current native subtree is the C++ SIP/media gateway under
`native/sip_gateway/`.

## Layout

- `sip_gateway/`
  PJSUA2-based SIP gateway, per-call UDP audio bridge, Unix control-channel
  client, and local build files.

## Rules

- Keep SIP signaling, RTP/media attachment, and other latency-sensitive bridge
  code here.
- Keep dialog, business tools, and provider-SDK orchestration in `src/`.
