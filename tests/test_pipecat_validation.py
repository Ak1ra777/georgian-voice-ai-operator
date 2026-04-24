from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import config
from src.pipecat_runtime.validation import validate_pipecat_runtime


class PipecatRuntimeValidationTest(unittest.TestCase):
    def _write_temp_file(self, *, suffix: str = ".tmp") -> str:
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as handle:
            path = Path(handle.name)
        self.addCleanup(lambda: path.unlink(missing_ok=True))
        return str(path)

    def test_validate_pipecat_runtime_does_not_require_legacy_project_id(self) -> None:
        credentials_path = self._write_temp_file()

        with patch.multiple(
            config,
            OPENAI_API_KEY="openai-test",
            GOOGLE_APPLICATION_CREDENTIALS=credentials_path,
            CARTESIA_API_KEY="cartesia-test",
            CARTESIA_TTS_VOICE_ID="voice-test",
            TTS_PROVIDER="cartesia_ws",
            CALL_GREETING_ENABLED=False,
            CALL_GREETING_AUDIO_PATH=None,
        ):
            validate_pipecat_runtime()

    def test_validate_pipecat_runtime_requires_google_credentials_file(self) -> None:
        with patch.multiple(
            config,
            OPENAI_API_KEY="openai-test",
            GOOGLE_APPLICATION_CREDENTIALS="/tmp/pipecat-missing-google-creds.json",
            CARTESIA_API_KEY="cartesia-test",
            CARTESIA_TTS_VOICE_ID="voice-test",
            TTS_PROVIDER="cartesia_ws",
            CALL_GREETING_ENABLED=False,
            CALL_GREETING_AUDIO_PATH=None,
        ):
            with self.assertRaises(RuntimeError) as ctx:
                validate_pipecat_runtime()

        self.assertIn("GOOGLE_APPLICATION_CREDENTIALS", str(ctx.exception))

    def test_validate_pipecat_runtime_checks_greeting_asset_when_enabled(self) -> None:
        credentials_path = self._write_temp_file()

        with patch.multiple(
            config,
            OPENAI_API_KEY="openai-test",
            GOOGLE_APPLICATION_CREDENTIALS=credentials_path,
            CARTESIA_API_KEY="cartesia-test",
            CARTESIA_TTS_VOICE_ID="voice-test",
            TTS_PROVIDER="cartesia_ws",
            CALL_GREETING_ENABLED=True,
            CALL_GREETING_AUDIO_PATH="assets/audio/does_not_exist.wav",
        ):
            with self.assertRaises(RuntimeError) as ctx:
                validate_pipecat_runtime()

        self.assertIn("CALL_GREETING_AUDIO_PATH", str(ctx.exception))
