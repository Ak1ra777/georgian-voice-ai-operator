# Control Package

## Purpose

`native/sip_gateway/control/` owns the gateway-side control-channel client that
talks to the Python call manager over a Unix domain socket.

The protocol is newline-delimited JSON with outbound gateway events and inbound
manager commands.

## Files

- `control_channel.hpp`
  Public client interface and callback contract.
- `control_channel.cpp`
  Socket connect/read/write behavior plus the local JSON parsing helpers used
  for inbound messages.

## Responsibilities

- connect to the Python control socket
- send gateway events such as `call_allocated`, `call_media_started`, and
  `call_end`
- receive manager commands such as `call_ready` and `call_hangup`
- keep framing and socket I/O separate from SIP session ownership

## Rules

- Keep control-socket transport and protocol parsing here.
- Keep call routing and lifecycle policy in `../core/`.
