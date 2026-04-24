import os

from dotenv import load_dotenv


def _parse_bool(raw: str | None, default: bool = False) -> bool:
    value = (raw or "").strip().lower()
    if not value:
        return default
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"Invalid boolean value: {raw}")


def _parse_socket_mode(raw: str | None, default: int = 0o600) -> int:
    value = (raw or "").strip()
    if not value:
        return default
    try:
        mode = int(value, 8)
    except ValueError as ex:
        raise RuntimeError(f"Invalid octal mode: {raw}") from ex
    if mode < 0 or mode > 0o777:
        raise RuntimeError(f"Invalid socket mode range: {raw}")
    return mode


def _parse_uid_set(raw: str | None) -> set[int]:
    value = (raw or "").strip()
    if not value:
        return set()
    parsed: set[int] = set()
    for part in value.split(","):
        token = part.strip()
        if not token:
            continue
        try:
            parsed.add(int(token))
        except ValueError as ex:
            raise RuntimeError(f"Invalid uid token in list: {token}") from ex
    return parsed


def _parse_csv_list(raw: str | None, default: list[str] | None = None) -> list[str]:
    value = (raw or "").strip()
    if not value:
        return list(default or [])

    parsed = [token.strip() for token in value.split(",") if token.strip()]
    if parsed:
        return parsed
    return list(default or [])


def _parse_choice(
    raw: str | None,
    *,
    default: str,
    allowed: set[str],
) -> str:
    value = (raw or "").strip().lower()
    if not value:
        return default
    if value not in allowed:
        allowed_text = ", ".join(sorted(allowed))
        raise RuntimeError(f"Invalid choice: {raw}. Allowed values: {allowed_text}")
    return value


# Prefer project .env values for local runtime consistency.
load_dotenv(override=True)


OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
CARTESIA_API_KEY = os.getenv("CARTESIA_API_KEY")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
ISP_ACCOUNT_API_URL = (os.getenv("ISP_ACCOUNT_API_URL") or "").strip() or None
ISP_ACCOUNT_API_KEY = (os.getenv("ISP_ACCOUNT_API_KEY") or "").strip() or None
ISP_ACCOUNT_API_TIMEOUT_S = max(
    1.0,
    float(os.getenv("ISP_ACCOUNT_API_TIMEOUT_S", "10.0")),
)

OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
OPENAI_MAX_OUTPUT_TOKENS = max(1, int(os.getenv("OPENAI_MAX_OUTPUT_TOKENS", "120")))
OPENAI_TEMPERATURE = min(
    2.0,
    max(0.0, float(os.getenv("OPENAI_TEMPERATURE", "0.2"))),
)

LOCATION = os.getenv("LOCATION", "eu")
MODEL = os.getenv("MODEL", "chirp_3")
STT_LANGUAGE_CODES = _parse_csv_list(
    os.getenv("STT_LANGUAGE_CODES"),
    default=["ka-GE"],
)

TTS_PROVIDER = os.getenv("TTS_PROVIDER", "cartesia_ws").lower()
TTS_LANGUAGE_CODE = os.getenv("TTS_LANGUAGE_CODE", "ka-GE")
TTS_MODEL_NAME = os.getenv("TTS_MODEL_NAME", "sonic-3")
TTS_AGGREGATION_MODE = _parse_choice(
    os.getenv("TTS_AGGREGATION_MODE"),
    default="token",
    allowed={"sentence", "token"},
)
CARTESIA_TTS_VOICE_ID = os.getenv("CARTESIA_TTS_VOICE_ID")
CARTESIA_GENERATION_SPEED = min(
    1.5,
    max(0.6, float(os.getenv("CARTESIA_GENERATION_SPEED", "1.0"))),
)
CARTESIA_GENERATION_VOLUME = min(
    2.0,
    max(0.5, float(os.getenv("CARTESIA_GENERATION_VOLUME", "1.0"))),
)
CARTESIA_WS_SAMPLE_RATE = int(os.getenv("CARTESIA_WS_SAMPLE_RATE", "16000"))
CARTESIA_WS_ENCODING = os.getenv("CARTESIA_WS_ENCODING", "pcm_s16le")

SIP_CONTROL_SOCKET = os.getenv("SIP_CONTROL_SOCKET", "/tmp/demo_voice_assistant.sock")
SIP_AUDIO_HOST = os.getenv("SIP_AUDIO_HOST", "127.0.0.1")
SIP_AUDIO_RX_PORT = int(os.getenv("SIP_AUDIO_RX_PORT", "4000"))
SIP_AUDIO_TX_PORT = int(os.getenv("SIP_AUDIO_TX_PORT", "4001"))

CALL_NO_SPEECH_TIMEOUT_S = float(os.getenv("CALL_NO_SPEECH_TIMEOUT_S", "10"))
CALL_MAX_DURATION_S = float(os.getenv("CALL_MAX_DURATION_S", "3600"))
CALL_GREETING_ENABLED = _parse_bool(os.getenv("CALL_GREETING_ENABLED", "false"))
CALL_GREETING_AUDIO_PATH = (
    os.getenv("CALL_GREETING_AUDIO_PATH", "").strip()
    or None
)
USER_TURN_STOP_STRATEGY = _parse_choice(
    os.getenv("USER_TURN_STOP_STRATEGY"),
    default="turn_analyzer",
    allowed={"speech_timeout", "turn_analyzer"},
)
USER_TURN_SPEECH_TIMEOUT_S = max(
    0.05,
    float(os.getenv("USER_TURN_SPEECH_TIMEOUT_S", "0.25")),
)
USER_TURN_STOP_TIMEOUT_S = max(
    0.5,
    float(os.getenv("USER_TURN_STOP_TIMEOUT_S", "5.0")),
)
DTMF_SUBMIT_KEY = (os.getenv("DTMF_SUBMIT_KEY", "#").strip() or "#")[:1]
IDENTIFIER_MAX_ATTEMPTS = max(
    1,
    int(os.getenv("IDENTIFIER_MAX_ATTEMPTS", "3")),
)
VERIFICATION_MAX_ATTEMPTS = max(
    1,
    int(os.getenv("VERIFICATION_MAX_ATTEMPTS", "3")),
)
CALLBACK_MAX_ATTEMPTS = max(
    1,
    int(os.getenv("CALLBACK_MAX_ATTEMPTS", "3")),
)

CONTROL_RECONNECT_GRACE_S = float(os.getenv("CONTROL_RECONNECT_GRACE_S", "5"))
_DEFAULT_CONTROL_UID = str(os.getuid()) if hasattr(os, "getuid") else ""
CONTROL_SOCKET_PERMS = _parse_socket_mode(
    os.getenv("CONTROL_SOCKET_PERMS", "0600")
)
CONTROL_ALLOWED_PEER_UIDS = _parse_uid_set(
    os.getenv("CONTROL_ALLOWED_PEER_UIDS", _DEFAULT_CONTROL_UID)
)
CONTROL_CMD_ACK_TIMEOUT_S = max(
    0.1,
    float(os.getenv("CONTROL_CMD_ACK_TIMEOUT_S", "0.5")),
)
CONTROL_CMD_MAX_RETRIES = max(
    0,
    int(os.getenv("CONTROL_CMD_MAX_RETRIES", "3")),
)
PIPECAT_LOG_LEVEL = _parse_choice(
    os.getenv("PIPECAT_LOG_LEVEL"),
    default="info",
    allowed={"trace", "debug", "info", "warning", "error"},
).upper()
PIPECAT_ENABLE_PERIODIC_TELEMETRY = _parse_bool(
    os.getenv("PIPECAT_ENABLE_PERIODIC_TELEMETRY", "false")
)
PIPECAT_TELEMETRY_INTERVAL_S = max(
    0.5,
    float(os.getenv("PIPECAT_TELEMETRY_INTERVAL_S", "5.0")),
)
PIPECAT_FLOW_CONTEXT_STRATEGY = _parse_choice(
    os.getenv("PIPECAT_FLOW_CONTEXT_STRATEGY"),
    default="reset",
    allowed={"append", "reset"},
)
CALL_ARTIFACT_SYNC_WRITES = _parse_bool(
    os.getenv("CALL_ARTIFACT_SYNC_WRITES", "false")
)


def _require(name: str, value: str | None) -> None:
    if value is None or not str(value).strip():
        raise RuntimeError(f"{name} missing in .env")


def validate_openai() -> None:
    _require("OPENAI_API_KEY", OPENAI_API_KEY)


def validate_tts_provider(provider: str | None = None) -> str:
    provider_name = (provider or TTS_PROVIDER).lower()
    if provider_name == "cartesia_ws":
        _require("CARTESIA_API_KEY", CARTESIA_API_KEY)
        _require("CARTESIA_TTS_VOICE_ID", CARTESIA_TTS_VOICE_ID)
        return provider_name

    raise RuntimeError(
        f"Unsupported TTS_PROVIDER: {provider_name}. "
        "Only 'cartesia_ws' is supported."
    )


def validate_isp_account_lookup() -> None:
    _require("ISP_ACCOUNT_API_URL", ISP_ACCOUNT_API_URL)
    _require("ISP_ACCOUNT_API_KEY", ISP_ACCOUNT_API_KEY)
