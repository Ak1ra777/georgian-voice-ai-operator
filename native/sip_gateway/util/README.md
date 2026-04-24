# Util Package

## Purpose

`native/sip_gateway/util/` contains small reusable helpers with narrow gateway
ownership.

Today that means bind-checked UDP port-pair reservation.

## Files

- `port_allocator.hpp`
  Reservation types and allocator interface.
- `port_allocator.cpp`
  Round-robin pair reservation with real bind checks.

## Responsibilities

- reserve `(rx, tx)` UDP port pairs for new calls
- keep the receive socket open until the media bridge takes ownership
- release ports back to the allocator on cleanup

## Rules

- Keep this folder small and specific.
- Do not turn it into a generic dumping ground for SIP or media logic.
