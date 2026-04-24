# Assets

## Purpose

`assets/` holds non-code resources used by the runtime.

For the public portfolio snapshot, audio recordings and prompt WAV files are
not checked in. Add deployment-specific audio locally when needed.

## Layout

- `audio/`
  Placeholder directory for local greeting, prompt, and filler audio.

## Notes

`CALL_GREETING_AUDIO_PATH` and any flow-specific prompt paths are
deployment-facing configuration seams. Point them at local audio files that are
not committed to the repository.

## Rules

- Keep reusable static resources here only when they are safe to publish.
- Keep generated files out of this folder; runtime output belongs under
  `outputs/`.
- Do not commit caller recordings, provider-generated speech, or private prompt
  audio.
