from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.audio.turn.smart_turn.local_smart_turn_v3 import LocalSmartTurnAnalyzerV3
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.services.cartesia.tts import (
    CartesiaTTSService,
    CartesiaTTSSettings,
    GenerationConfig,
)
from pipecat.services.google.stt import GoogleSTTService, GoogleSTTSettings
from pipecat.services.tts_service import TextAggregationMode
from pipecat.transcriptions.language import Language
from pipecat.turns.user_start import VADUserTurnStartStrategy
from pipecat.turns.user_stop import (
    SpeechTimeoutUserTurnStopStrategy,
    TurnAnalyzerUserTurnStopStrategy,
)
from pipecat.turns.user_turn_strategies import UserTurnStrategies

from src.config import (
    CALL_GREETING_AUDIO_PATH,
    CARTESIA_API_KEY,
    CARTESIA_GENERATION_SPEED,
    CARTESIA_GENERATION_VOLUME,
    CARTESIA_TTS_VOICE_ID,
    CARTESIA_WS_ENCODING,
    CARTESIA_WS_SAMPLE_RATE,
    GOOGLE_APPLICATION_CREDENTIALS,
    LOCATION,
    MODEL,
    OPENAI_MAX_OUTPUT_TOKENS,
    OPENAI_MODEL,
    OPENAI_TEMPERATURE,
    STT_LANGUAGE_CODES,
    TTS_LANGUAGE_CODE,
    TTS_AGGREGATION_MODE,
    TTS_MODEL_NAME,
    USER_TURN_SPEECH_TIMEOUT_S,
    USER_TURN_STOP_STRATEGY,
    USER_TURN_STOP_TIMEOUT_S,
)
from src.runtime.media.audio.udp_audio import wav_to_pcm16_mono
from src.tools.contracts import ToolSet

from .artifacts import CallArtifactWriter
from .gateway_bridge import GatewaySessionBridge
from .flow_runtime import (
    GatewayFlowRuntime,
    build_gateway_flow_runtime,
    snapshot_flow_state,
)
from .dtmf_capture import DtmfCollectionCoordinator
from .hangup import DeferredHangupController
from .openai_responses import (
    OpenAIResponsesLLMService,
    OpenAIResponsesLLMSettings,
)
from .policy import SessionPolicyController
from .prompts import GEORGIAN_ISP_SYSTEM_PROMPT
from .processors.activity import SessionActivityProcessor
from .processors.dtmf_collection import SensitiveDtmfCollectorProcessor
from .processors.dtmf_speech_guard import DtmfSpeechGuardProcessor
from .processors.egress import GatewayEgressProcessor
from .processors.ingress import GatewayIngressProcessor
from .processors.transcript_capture import (
    AssistantTranscriptCaptureProcessor,
    UserTranscriptCaptureProcessor,
)
from .processors.turn_timing import TurnTimingProcessor
from .tools import PipecatToolRuntime
from .user_turn_start import BotAwareTranscriptionUserTurnStartStrategy
from .validation import resolve_project_path


DEFAULT_PIPELINE_SAMPLE_RATE = 16000
DEFAULT_PIPELINE_CHANNELS = 1
GEORGIAN_LANGUAGE_CODES = {"ka-GE", "ka"}
GEORGIAN_SUPPORTED_STT_PROFILES = {
    ("eu", "chirp_3"),
    ("us", "chirp_3"),
    ("asia-southeast1", "chirp"),
    ("asia-southeast1", "chirp_2"),
    ("europe-west4", "chirp"),
    ("europe-west4", "chirp_2"),
    ("us-central1", "chirp"),
    ("us-central1", "chirp_2"),
}


@dataclass(frozen=True)
class GatewayPipelineArtifacts:
    ingress: GatewayIngressProcessor
    activity: SessionActivityProcessor
    egress: GatewayEgressProcessor
    turn_timing: TurnTimingProcessor
    pipeline: Pipeline
    task: PipelineTask
    greeting_pcm16: bytes | None
    tool_runtime: PipecatToolRuntime | None = None
    flow_runtime: GatewayFlowRuntime | None = None
    dtmf_collector: DtmfCollectionCoordinator | None = None
    call_artifact_writer: CallArtifactWriter | None = None


def build_gateway_pipeline(
    *,
    session_id: str,
    bridge: GatewaySessionBridge,
    policy_controller: SessionPolicyController,
    caller_uri: str | None = None,
    call_artifact_writer: CallArtifactWriter | None = None,
    greeting_audio_path: str | None = None,
    request_hangup_fn: Callable[[str], None] | None = None,
    stt_factory: Callable[[], object] | None = None,
    llm_factory: Callable[[], object] | None = None,
    tts_factory: Callable[[], object] | None = None,
    system_prompt: str | None = None,
    toolset: ToolSet | None = None,
) -> GatewayPipelineArtifacts:
    deferred_hangup = DeferredHangupController()
    flow_runtime_ref: dict[str, GatewayFlowRuntime | None] = {"runtime": None}

    def _record_turn_timing_event(event: str, data: dict) -> None:
        if call_artifact_writer is None:
            return
        payload = dict(data)
        turn_id = payload.pop("turn_id", None)
        call_artifact_writer.record_turn_event(
            source="turn_timing",
            event=event,
            turn_id=turn_id,
            data=payload,
        )

    def _record_media_event(source: str):
        if call_artifact_writer is None:
            return None

        def _record(event: str, data: dict) -> None:
            call_artifact_writer.record_media_event(
                source=source,
                event=event,
                data=data,
            )

        return _record

    def _record_tool_event(event: str, data: dict) -> None:
        if call_artifact_writer is None:
            return
        payload = dict(data)
        level = str(payload.pop("level", "info")).strip() or "info"
        call_artifact_writer.record_tool_event(
            source="tools",
            event=event,
            level=level,
            data=payload,
        )

    def _record_flow_event(event: str, data: dict) -> None:
        if call_artifact_writer is None:
            return
        call_artifact_writer.record_control_event(
            source="flow",
            event=event,
            data=data,
        )

    def _record_dtmf_guard_event(event: str, data: dict) -> None:
        if call_artifact_writer is None:
            return
        call_artifact_writer.record_control_event(
            source="dtmf_guard",
            event=event,
            data=data,
        )

    def _flow_state_provider() -> dict[str, str | None]:
        flow_runtime = flow_runtime_ref["runtime"]
        manager = flow_runtime.manager if flow_runtime is not None else None
        return snapshot_flow_state(
            manager=manager,
            dtmf_collector=dtmf_collector,
        )

    ingress = GatewayIngressProcessor(
        bridge,
        audio_sample_rate=DEFAULT_PIPELINE_SAMPLE_RATE,
        num_channels=DEFAULT_PIPELINE_CHANNELS,
        on_audio_chunk=(
            (lambda audio, sample_rate, num_channels: call_artifact_writer.append_caller_audio(
                audio,
                sample_rate=sample_rate,
                num_channels=num_channels,
            ))
            if call_artifact_writer is not None
            else None
        ),
        name=f"gateway-ingress:{session_id}",
    )
    activity = SessionActivityProcessor(
        policy_controller,
        name=f"session-activity:{session_id}",
    )
    turn_timing = TurnTimingProcessor(
        session_id,
        event_recorder=_record_turn_timing_event,
        name=f"turn-timing:{session_id}",
    )
    egress = GatewayEgressProcessor(
        bridge,
        request_hangup_fn=request_hangup_fn,
        deferred_hangup=deferred_hangup,
        event_recorder=_record_media_event("egress"),
        on_audio_chunk=(
            (
                lambda audio, sample_rate, num_channels: call_artifact_writer.append_assistant_audio(
                    audio,
                    sample_rate=sample_rate,
                    num_channels=num_channels,
                )
            )
            if call_artifact_writer is not None
            else None
        ),
        name=f"gateway-egress:{session_id}",
    )

    stt = stt_factory() if stt_factory is not None else _build_default_stt_service()
    llm = (
        llm_factory()
        if llm_factory is not None
        else _build_default_llm_service(system_prompt or _default_system_prompt())
    )
    tts = tts_factory() if tts_factory is not None else _build_default_tts_service()
    tool_runtime = PipecatToolRuntime(
        session_id=session_id,
        toolset=toolset,
        caller_uri=caller_uri,
        schedule_end_call_fn=deferred_hangup.schedule,
        event_recorder=_record_tool_event,
    )
    dtmf_collector = DtmfCollectionCoordinator()
    dtmf_collection_processor = SensitiveDtmfCollectorProcessor(
        dtmf_collector,
        name=f"dtmf-collector:{session_id}",
    )
    dtmf_speech_guard = DtmfSpeechGuardProcessor(
        dtmf_collector,
        event_recorder=_record_dtmf_guard_event,
        name=f"dtmf-speech-guard:{session_id}",
    )

    context = LLMContext(messages=[])
    context.set_tools(ToolsSchema(standard_tools=[]))
    context_aggregator = LLMContextAggregatorPair(
        context,
        user_params=_build_default_user_aggregator_params(),
    )

    processors: list[object] = [
        ingress,
        dtmf_collection_processor,
        stt,
    ]
    if call_artifact_writer is not None:
        processors.append(
            UserTranscriptCaptureProcessor(
                call_artifact_writer,
                flow_state_provider=_flow_state_provider,
                name=f"user-transcript-capture:{session_id}",
            )
        )
    processors.extend(
        [
            dtmf_speech_guard,
            context_aggregator.user(),
            llm,
            tts,
        ]
    )
    if call_artifact_writer is not None:
        processors.append(
            AssistantTranscriptCaptureProcessor(
                call_artifact_writer,
                flow_state_provider=_flow_state_provider,
                name=f"assistant-transcript-capture:{session_id}",
            )
        )
    processors.extend(
        [
            turn_timing,
            activity,
            egress,
            context_aggregator.assistant(),
        ]
    )

    pipeline = Pipeline(processors)
    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            audio_in_sample_rate=DEFAULT_PIPELINE_SAMPLE_RATE,
            audio_out_sample_rate=_effective_gateway_tts_sample_rate(),
        ),
        cancel_on_idle_timeout=False,
        enable_rtvi=False,
        enable_turn_tracking=False,
    )

    greeting_pcm16 = _build_greeting_pcm16(
        greeting_audio_path or CALL_GREETING_AUDIO_PATH
    )

    flow_runtime = None
    if callable(getattr(llm, "register_function", None)):
        flow_runtime = build_gateway_flow_runtime(
            session_id=session_id,
            task=task,
            llm=llm,
            context_aggregator=context_aggregator,
            tool_runtime=tool_runtime,
            dtmf_collector=dtmf_collector,
            event_recorder=_record_flow_event,
        )
        flow_runtime_ref["runtime"] = flow_runtime

    return GatewayPipelineArtifacts(
        ingress=ingress,
        activity=activity,
        egress=egress,
        turn_timing=turn_timing,
        pipeline=pipeline,
        task=task,
        greeting_pcm16=greeting_pcm16,
        tool_runtime=tool_runtime,
        flow_runtime=flow_runtime,
        dtmf_collector=dtmf_collector,
        call_artifact_writer=call_artifact_writer,
    )


def _build_default_stt_service():
    credentials_path = None
    if GOOGLE_APPLICATION_CREDENTIALS:
        credentials_path = str(resolve_project_path(GOOGLE_APPLICATION_CREDENTIALS))

    stt_location, stt_model = _resolve_stt_profile(
        language_codes=list(STT_LANGUAGE_CODES),
        location=LOCATION,
        model=MODEL,
    )
    stt_languages = _resolve_google_stt_languages(list(STT_LANGUAGE_CODES))
    settings = GoogleSTTSettings(
        model=stt_model,
        languages=stt_languages,
        language_codes=list(STT_LANGUAGE_CODES),
        enable_automatic_punctuation=True,
        enable_interim_results=True,
    )
    return GoogleSTTService(
        credentials_path=credentials_path,
        location=stt_location,
        sample_rate=DEFAULT_PIPELINE_SAMPLE_RATE,
        settings=settings,
    )


def _build_default_llm_service(system_prompt: str):
    settings = OpenAIResponsesLLMSettings(
        model=OPENAI_MODEL,
        system_instruction=system_prompt,
        temperature=OPENAI_TEMPERATURE,
        max_completion_tokens=OPENAI_MAX_OUTPUT_TOKENS,
    )
    return OpenAIResponsesLLMService(
        run_in_parallel=False,
        settings=settings,
    )


def _build_default_tts_service():
    sample_rate = _effective_gateway_tts_sample_rate()
    settings = CartesiaTTSSettings(
        model=TTS_MODEL_NAME,
        voice=CARTESIA_TTS_VOICE_ID,
        language=TTS_LANGUAGE_CODE,
        generation_config=GenerationConfig(
            speed=CARTESIA_GENERATION_SPEED,
            volume=CARTESIA_GENERATION_VOLUME,
        ),
    )
    return CartesiaTTSService(
        api_key=CARTESIA_API_KEY,
        sample_rate=sample_rate,
        encoding=CARTESIA_WS_ENCODING,
        text_aggregation_mode=_resolve_tts_aggregation_mode(TTS_AGGREGATION_MODE),
        settings=settings,
    )


def _build_greeting_pcm16(path: str | None) -> bytes | None:
    if path is None:
        return None
    resolved_path = resolve_project_path(path)
    return wav_to_pcm16_mono(
        str(resolved_path),
        target_rate=DEFAULT_PIPELINE_SAMPLE_RATE,
    )


def _default_system_prompt() -> str:
    return GEORGIAN_ISP_SYSTEM_PROMPT


def _resolve_stt_profile(
    *,
    language_codes: list[str],
    location: str,
    model: str,
) -> tuple[str, str]:
    normalized_languages = {
        str(code).strip()
        for code in language_codes
        if str(code).strip()
    }
    if not normalized_languages & GEORGIAN_LANGUAGE_CODES:
        return location, model

    requested_profile = (location, model)
    if requested_profile in GEORGIAN_SUPPORTED_STT_PROFILES:
        return requested_profile

    # Georgian support is region/model specific in Google STT.
    # Prefer the documented Chirp 3 multi-region endpoint when the configured
    # combo is unsupported.
    fallback_profile = ("eu", "chirp_3")
    print(
        "pipecat_stt_profile_override "
        f"languages={sorted(normalized_languages)} "
        f"requested_location={location} requested_model={model} "
        f"effective_location={fallback_profile[0]} "
        f"effective_model={fallback_profile[1]}"
    )
    return fallback_profile


def _resolve_google_stt_languages(language_codes: list[str]) -> list[Language]:
    resolved: list[Language] = []
    for code in language_codes:
        try:
            resolved.append(Language(str(code).strip()))
        except ValueError:
            continue
    return resolved


def _build_default_user_aggregator_params() -> LLMUserAggregatorParams:
    return LLMUserAggregatorParams(
        user_turn_strategies=UserTurnStrategies(
            start=_build_user_turn_start_strategies(),
            stop=_build_user_turn_stop_strategies(),
        ),
        user_turn_stop_timeout=_effective_user_turn_stop_timeout_s(),
        vad_analyzer=_build_default_vad_analyzer(),
    )


def _build_user_turn_start_strategies():
    return [
        VADUserTurnStartStrategy(),
        BotAwareTranscriptionUserTurnStartStrategy(),
    ]


def _build_user_turn_stop_strategies():
    if USER_TURN_STOP_STRATEGY == "speech_timeout":
        return [
            SpeechTimeoutUserTurnStopStrategy(
                user_speech_timeout=USER_TURN_SPEECH_TIMEOUT_S
            )
        ]

    return [
        TurnAnalyzerUserTurnStopStrategy(
            turn_analyzer=LocalSmartTurnAnalyzerV3()
        )
    ]


def _resolve_tts_aggregation_mode(mode: str) -> TextAggregationMode:
    if mode == "sentence":
        return TextAggregationMode.SENTENCE
    return TextAggregationMode.TOKEN


def _effective_user_turn_stop_timeout_s() -> float:
    timeout_s = USER_TURN_STOP_TIMEOUT_S
    if USER_TURN_STOP_STRATEGY != "speech_timeout":
        return timeout_s

    # In speech-timeout mode, the controller timeout is only a safety net.
    # It must stay above the STT-based stop strategy so one utterance cannot
    # be force-closed before transcription arrives and then reopened as a
    # phantom second turn by the late transcript.
    effective_timeout_s = max(timeout_s, 5.0)
    if effective_timeout_s != timeout_s:
        print(
            "pipecat_user_turn_stop_timeout_adjusted "
            f"requested_s={timeout_s:.2f} effective_s={effective_timeout_s:.2f} "
            f"strategy={USER_TURN_STOP_STRATEGY}"
        )
    return effective_timeout_s


def _build_default_vad_analyzer() -> SileroVADAnalyzer:
    return SileroVADAnalyzer(sample_rate=DEFAULT_PIPELINE_SAMPLE_RATE)


def _effective_gateway_tts_sample_rate() -> int:
    if CARTESIA_WS_SAMPLE_RATE != DEFAULT_PIPELINE_SAMPLE_RATE:
        print(
            "pipecat_tts_sample_rate_override "
            f"requested_sample_rate={CARTESIA_WS_SAMPLE_RATE} "
            f"effective_sample_rate={DEFAULT_PIPELINE_SAMPLE_RATE}"
        )
    return DEFAULT_PIPELINE_SAMPLE_RATE
