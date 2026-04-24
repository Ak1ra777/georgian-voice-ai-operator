# Media Package

## Purpose

`src/runtime/media/` groups the Layer-1 media subsystems used during live
calls.

## Subpackages

- [`audio/`](audio/README.md)
  Raw PCM/UDP transport on the Python side of the SIP bridge.
- [`pipeline/`](pipeline/README.md)
  Generic listen/speak turn helpers.
- [`stt/`](stt/README.md)
  Speech-to-text capture and utterance handling.
- [`tts/`](tts/README.md)
  Speech synthesis and playback coordination.
- [`vad/`](vad/README.md)
  Silero VAD wrapper and artifact loading.

## Mental Model

`audio/` handles raw packets, `vad/` detects utterance boundaries, `stt/`
produces text, `pipeline/` coordinates turn-level speech helpers, and `tts/`
plays assistant output back into the call.

## Rules

- Keep speech and media mechanics here.
- Keep call lifecycle policy in `src/runtime/calls/`.
- Keep dialog transitions and support flow in `src/dialog/`.
