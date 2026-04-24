# Audio Assets

## Purpose

`assets/audio/` is reserved for local WAV files referenced by runtime config
and worker media helpers.

## Public Contents

No WAV files are committed in the portfolio-safe snapshot. Add local audio
files for deployment-specific greetings or prompts, and keep them ignored.

## Config Paths

Set `CALL_GREETING_ENABLED=true` and `CALL_GREETING_AUDIO_PATH` to a local WAV
file if the worker should play an initial greeting. Other flow-specific prompt
paths should be handled the same way: local files for real deployments, not
public repository assets.

## Rules

- Keep Python-side playout assets in a format the UDP sink can handle
  consistently; `16 kHz` mono WAV is the current baseline.
- Treat filenames as configuration surface, not decorative labels.
- Do not commit audio recordings or provider-generated speech.
