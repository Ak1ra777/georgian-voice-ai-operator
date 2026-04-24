from __future__ import annotations

import tempfile
import unittest
import wave
from pathlib import Path
from unittest.mock import patch

from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.pipeline.task import PipelineTask
from pipecat.transcriptions.language import Language
from pipecat.turns.user_start import VADUserTurnStartStrategy
from pipecat.turns.user_stop import (
    SpeechTimeoutUserTurnStopStrategy,
    TurnAnalyzerUserTurnStopStrategy,
)

from src.pipecat_runtime.pipeline_factory import (
    _build_default_user_aggregator_params,
    _build_default_llm_service,
    _build_default_stt_service,
    _build_default_tts_service,
    _default_system_prompt,
    _resolve_stt_profile,
    build_gateway_pipeline,
)
from src.pipecat_runtime.policy import SessionPolicyController
from src.runtime.calls.session.runtime import SessionRuntime
from src.pipecat_runtime.user_turn_start import BotAwareTranscriptionUserTurnStartStrategy


class _FakeBridge:
    def __init__(self) -> None:
        self.session_id = "sess-1"

    def poll_audio_chunk(self, timeout: float | None = None):
        _ = timeout
        return None

    def poll_dtmf_digit(self, timeout: float | None = None):
        _ = timeout
        return None

    def send_output_audio(self, pcm16: bytes, *, sample_rate: int = 16000) -> None:
        _ = (pcm16, sample_rate)

    def flush_output_audio(self, pad_final_frame: bool = True) -> None:
        _ = pad_final_frame


class _PassThroughProcessor(FrameProcessor):
    def __init__(self, name: str) -> None:
        super().__init__(name=name, enable_direct_mode=True)

    async def process_frame(self, frame, direction: FrameDirection):
        await super().process_frame(frame, direction)
        await self.push_frame(frame, direction)


class PipecatPipelineFactoryTest(unittest.TestCase):
    def _write_test_wav(self) -> str:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as handle:
            path = Path(handle.name)

        with wave.open(str(path), "wb") as wav_file:
            wav_file.setnchannels(1)
            wav_file.setsampwidth(2)
            wav_file.setframerate(16000)
            wav_file.writeframes(b"\x01\x00" * 160)

        self.addCleanup(lambda: path.unlink(missing_ok=True))
        return str(path)

    def test_build_gateway_pipeline_returns_task_and_greeting_frame(self) -> None:
        bridge = _FakeBridge()
        greeting_path = self._write_test_wav()
        policy = SessionPolicyController(
            session_id="sess-1",
            runtime=SessionRuntime(session_id="sess-1"),
            no_speech_timeout_s=10.0,
            max_duration_s=300.0,
        )

        with patch(
            "src.pipecat_runtime.pipeline_factory.LocalSmartTurnAnalyzerV3"
        ), patch(
            "src.pipecat_runtime.pipeline_factory.SileroVADAnalyzer"
        ):
            artifacts = build_gateway_pipeline(
                session_id="sess-1",
                bridge=bridge,
                policy_controller=policy,
                greeting_audio_path=greeting_path,
                stt_factory=lambda: _PassThroughProcessor("fake-stt"),
                llm_factory=lambda: _PassThroughProcessor("fake-llm"),
                tts_factory=lambda: _PassThroughProcessor("fake-tts"),
            )

        self.assertIsInstance(artifacts.task, PipelineTask)
        self.assertIsInstance(artifacts.greeting_pcm16, bytes)
        self.assertGreater(len(artifacts.greeting_pcm16), 0)
        self.assertIsNotNone(artifacts.tool_runtime)
        self.assertIsNone(artifacts.flow_runtime)

    def test_default_service_builders_map_repo_config_into_pipecat_settings(self) -> None:
        with patch("src.pipecat_runtime.pipeline_factory.GoogleSTTService") as stt_cls:
            _build_default_stt_service()
            stt_kwargs = stt_cls.call_args.kwargs
            self.assertEqual(stt_kwargs["sample_rate"], 16000)
            self.assertTrue(stt_kwargs["settings"].enable_interim_results)
            self.assertTrue(stt_kwargs["settings"].enable_automatic_punctuation)

        with patch(
            "src.pipecat_runtime.pipeline_factory.OpenAIResponsesLLMService"
        ) as llm_cls:
            _build_default_llm_service("system prompt")
            llm_kwargs = llm_cls.call_args.kwargs
            self.assertEqual(llm_kwargs["settings"].system_instruction, "system prompt")

        with patch("src.pipecat_runtime.pipeline_factory.CartesiaTTSService") as tts_cls:
            _build_default_tts_service()
            tts_kwargs = tts_cls.call_args.kwargs
            self.assertEqual(tts_kwargs["text_aggregation_mode"].value, "token")
            self.assertIsNotNone(tts_kwargs["settings"].generation_config)
            self.assertEqual(tts_kwargs["sample_rate"], 16000)

    def test_default_stt_builder_resolves_relative_credentials_path(self) -> None:
        project_root = Path(__file__).resolve().parents[1]
        with tempfile.NamedTemporaryFile(dir=project_root, delete=False) as handle:
            credentials_path = Path(handle.name)
        self.addCleanup(lambda: credentials_path.unlink(missing_ok=True))

        relative_path = credentials_path.relative_to(project_root)
        with patch(
            "src.pipecat_runtime.pipeline_factory.GOOGLE_APPLICATION_CREDENTIALS",
            str(relative_path),
        ), patch("src.pipecat_runtime.pipeline_factory.GoogleSTTService") as stt_cls:
            _build_default_stt_service()
            stt_kwargs = stt_cls.call_args.kwargs
            self.assertEqual(
                stt_kwargs["credentials_path"],
                str(credentials_path.resolve()),
            )

    def test_georgian_stt_profile_falls_back_to_supported_location_and_model(self) -> None:
        self.assertEqual(
            _resolve_stt_profile(
                language_codes=["ka-GE"],
                location="europe-west3",
                model="chirp_3",
            ),
            ("eu", "chirp_3"),
        )

    def test_default_stt_builder_overrides_unsupported_georgian_profile(self) -> None:
        with patch(
            "src.pipecat_runtime.pipeline_factory.STT_LANGUAGE_CODES",
            ["ka-GE"],
        ), patch(
            "src.pipecat_runtime.pipeline_factory.LOCATION",
            "europe-west3",
        ), patch(
            "src.pipecat_runtime.pipeline_factory.MODEL",
            "chirp_3",
        ), patch(
            "src.pipecat_runtime.pipeline_factory.GoogleSTTService"
        ) as stt_cls:
            _build_default_stt_service()
            stt_kwargs = stt_cls.call_args.kwargs
            self.assertEqual(stt_kwargs["location"], "eu")
            self.assertEqual(stt_kwargs["settings"].model, "chirp_3")
            self.assertEqual(stt_kwargs["settings"].languages, [Language.KA_GE])
            self.assertEqual(stt_kwargs["settings"].language_codes, ["ka-GE"])

    def test_default_system_prompt_uses_isp_manifest_prompt(self) -> None:
        self.assertIn("ქართული ინტერნეტ-პროვაიდერის ხმოვანი აგენტი", _default_system_prompt())

    def test_default_user_aggregator_params_use_turn_analyzer_and_explicit_vad(self) -> None:
        with patch(
            "src.pipecat_runtime.pipeline_factory.USER_TURN_STOP_STRATEGY",
            "turn_analyzer",
        ), patch(
            "src.pipecat_runtime.pipeline_factory.LocalSmartTurnAnalyzerV3"
        ) as turn_analyzer_cls, patch(
            "src.pipecat_runtime.pipeline_factory.SileroVADAnalyzer"
        ) as vad_cls:
            params = _build_default_user_aggregator_params()

        self.assertIsNotNone(params.user_turn_strategies)
        start_strategies = params.user_turn_strategies.start or []
        stop_strategies = params.user_turn_strategies.stop or []
        self.assertEqual(len(start_strategies), 2)
        self.assertIsInstance(start_strategies[0], VADUserTurnStartStrategy)
        self.assertIsInstance(
            start_strategies[1],
            BotAwareTranscriptionUserTurnStartStrategy,
        )
        self.assertEqual(len(stop_strategies), 1)
        self.assertIsInstance(stop_strategies[0], TurnAnalyzerUserTurnStopStrategy)
        self.assertIs(params.vad_analyzer, vad_cls.return_value)
        turn_analyzer_cls.assert_called_once()
        vad_cls.assert_called_once()

    def test_speech_timeout_strategy_uses_non_preemptive_controller_timeout(self) -> None:
        with patch(
            "src.pipecat_runtime.pipeline_factory.USER_TURN_STOP_STRATEGY",
            "speech_timeout",
        ), patch(
            "src.pipecat_runtime.pipeline_factory.USER_TURN_STOP_TIMEOUT_S",
            0.8,
        ), patch(
            "src.pipecat_runtime.pipeline_factory.SileroVADAnalyzer"
        ):
            params = _build_default_user_aggregator_params()

        stop_strategies = params.user_turn_strategies.stop or []
        self.assertEqual(len(stop_strategies), 1)
        self.assertIsInstance(stop_strategies[0], SpeechTimeoutUserTurnStopStrategy)
        self.assertGreaterEqual(params.user_turn_stop_timeout, 5.0)
        self.assertIsNotNone(params.vad_analyzer)
