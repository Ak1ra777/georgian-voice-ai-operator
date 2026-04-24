# Media Package

## Purpose

`native/sip_gateway/media/` owns the SIP-audio to Python-audio bridge.

This package presents a PJMEDIA port on the SIP side and raw UDP PCM on the
Python side.

## Files

- `udp_audio_port.hpp`
  Public bridge interface, audio config, and bridge stats.
- `udp_audio_port.cpp`
  Ring buffers, worker threads, socket handling, and `8 kHz <-> 16 kHz`
  conversion.

## Responsibilities

- forward `8 kHz` SIP audio to Python as `16 kHz` UDP PCM
- receive `16 kHz` assistant audio from Python and feed it back into SIP media
- smooth jitter with byte-ring buffers
- expose transport counters for observability

## Rules

- Keep audio transport and sample-shape logic here.
- Keep call-session ownership in `../core/`.
- Keep Python-facing media policy in `src/runtime/media/`.
