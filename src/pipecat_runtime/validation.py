from __future__ import annotations

from pathlib import Path

from src import config


def validate_pipecat_runtime() -> None:
    """Validate only the runtime prerequisites the Pipecat path actually uses."""

    config.validate_openai()
    config.validate_tts_provider()
    _require_existing_file(
        "GOOGLE_APPLICATION_CREDENTIALS",
        config.GOOGLE_APPLICATION_CREDENTIALS,
    )

    if config.CALL_GREETING_ENABLED:
        _require_existing_file(
            "CALL_GREETING_AUDIO_PATH",
            config.CALL_GREETING_AUDIO_PATH,
        )


def resolve_project_path(path: str) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate

    cwd_candidate = (Path.cwd() / candidate).resolve()
    if cwd_candidate.exists():
        return cwd_candidate

    project_root = Path(__file__).resolve().parents[2]
    return (project_root / candidate).resolve()


def _require_existing_file(name: str, value: str | None) -> None:
    if value is None or not str(value).strip():
        raise RuntimeError(f"{name} missing in .env")

    resolved = resolve_project_path(str(value))
    if not resolved.is_file():
        raise RuntimeError(f"{name} does not exist: {resolved}")
