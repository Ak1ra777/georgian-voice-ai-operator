from __future__ import annotations

import asyncio
import json
import tempfile
from types import SimpleNamespace
import unittest
import wave
from pathlib import Path

from pipecat.frames.frames import TTSStoppedFrame, TTSTextFrame, TranscriptionFrame
from pipecat.processors.frame_processor import FrameDirection

from src.pipecat_runtime.artifacts import CallArtifactWriter
from src.pipecat_runtime.processors.transcript_capture import (
    AssistantTranscriptCaptureProcessor,
    UserTranscriptCaptureProcessor,
)
from src.runtime.calls.session.runtime import SessionRuntimeSnapshot, SessionRuntimeState


class PipecatCallArtifactsTest(unittest.TestCase):
    def test_writer_persists_audio_transcript_and_local_ticket(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            writer = CallArtifactWriter(
                session_id="sess-1",
                base_dir=tmp_dir,
                caller_uri="sip:+12025550100@example.test",
            )
            user_capture = UserTranscriptCaptureProcessor(
                writer,
                flow_state_provider=lambda: {
                    "current_node": "isp_router",
                    "branch": "technical_support",
                    "active_dtmf_kind": None,
                    "last_tool_id": None,
                },
                name="user-transcript-capture",
            )
            assistant_capture = AssistantTranscriptCaptureProcessor(
                writer,
                flow_state_provider=lambda: {
                    "current_node": "isp_technical_support",
                    "branch": "technical_support",
                    "active_dtmf_kind": "subscriber_identifier",
                    "last_tool_id": None,
                },
                name="assistant-transcript-capture",
            )

            asyncio.run(
                user_capture.process_frame(
                    TranscriptionFrame(
                        text="გამარჯობა",
                        user_id="caller-1",
                        timestamp="2026-04-18T10:00:00Z",
                        finalized=True,
                    ),
                    FrameDirection.DOWNSTREAM,
                )
            )
            asyncio.run(
                assistant_capture.process_frame(
                    TTSTextFrame(
                        text="რით",
                        aggregated_by="sentence",
                        context_id="ctx-1",
                    ),
                    FrameDirection.DOWNSTREAM,
                )
            )
            asyncio.run(
                assistant_capture.process_frame(
                    TTSTextFrame(
                        text="შემიძლია დაგეხმაროთ?",
                        aggregated_by="sentence",
                        context_id="ctx-1",
                    ),
                    FrameDirection.DOWNSTREAM,
                )
            )
            asyncio.run(
                assistant_capture.process_frame(
                    TTSStoppedFrame(context_id="ctx-1"),
                    FrameDirection.DOWNSTREAM,
                )
            )

            writer.append_caller_audio(
                b"\x01\x00" * 8,
                sample_rate=16000,
            )
            writer.append_assistant_audio(
                b"\x02\x00" * 6,
                sample_rate=8000,
            )
            writer.record_control_event(
                source="gateway",
                event="call_allocated",
                call_id=7,
                data={
                    "rx_port": 41000,
                    "tx_port": 41001,
                },
            )
            writer.record_gateway_trace(
                event="call_media_bridge_stats",
                call_id=7,
                data={
                    "sample": "final",
                    "rx_ring_drop_bytes": 0,
                    "tx_underrun_frames": 0,
                },
            )
            writer.record_media_event(
                source="worker",
                event="pipeline_started",
                data={
                    "runtime_state": "running",
                },
            )
            writer.record_turn_event(
                source="turn_timing",
                event="user_stopped_speaking",
                turn_id=1,
                data={
                    "user_speech_ms": 400,
                },
            )
            writer.record_tool_event(
                source="tools",
                event="tool_call_completed",
                data={
                    "tool_id": "create_ticket",
                },
            )
            writer.persist_ticket_payload(
                {
                    "session_id": "sess-1",
                    "ticket_type": "escalation",
                }
            )

            writer.finalize(
                runtime_snapshot=SessionRuntimeSnapshot(
                    session_id="sess-1",
                    state=SessionRuntimeState.ENDED,
                    cancel_reason="hangup_requested:pipeline_stopped",
                    is_cancelled=True,
                    state_changed_at_monotonic_s=0.0,
                    turn_index=1,
                    spoken_turns=1,
                    silent_turns=0,
                ),
                flow_state={
                    "current_node": "isp_human_handoff",
                    "branch": "human_handoff",
                    "active_dtmf_kind": None,
                    "last_tool_id": "create_ticket",
                },
                tool_state={
                    "caller_uri": "sip:+12025550100@example.test",
                    "caller_number": "+12025550100",
                    "customer_id": "cust-test-001",
                    "service_id": "svc-test-001",
                    "service_type": "fiber",
                    "lookup_status": "found",
                    "verification_status": "verified",
                    "ticket_id": "sess-1:escalation",
                    "last_ticket_summary": "Needs technician callback.",
                },
                latency_state=SimpleNamespace(
                    turns_started=1,
                    turns_archived=1,
                    turns_with_llm_start=1,
                    turns_with_first_audio=1,
                    interruptions=0,
                    last_turn_id=1,
                    last_archive_reason="bot_stopped",
                    last_user_speech_ms=400,
                    last_llm_started_after_user_stop_ms=200,
                    last_first_tts_text_after_user_stop_ms=300,
                    last_tts_started_after_user_stop_ms=350,
                    last_first_audio_after_user_stop_ms=400,
                    last_first_audio_after_llm_start_ms=200,
                    last_bot_started_after_user_stop_ms=450,
                    last_bot_speaking_ms=300,
                    avg_llm_started_after_user_stop_ms=200,
                    max_llm_started_after_user_stop_ms=200,
                    avg_first_audio_after_user_stop_ms=400,
                    max_first_audio_after_user_stop_ms=400,
                ),
            )

            call_dir = Path(tmp_dir) / "sess-1"
            self.assertTrue((call_dir / "caller.wav").exists())
            self.assertTrue((call_dir / "assistant.wav").exists())
            self.assertTrue((call_dir / "transcript.jsonl").exists())
            self.assertTrue((call_dir / "transcript.txt").exists())
            self.assertTrue((call_dir / "ticket.json").exists())
            self.assertTrue((call_dir / "metadata.json").exists())
            self.assertTrue((call_dir / "ticket_payload.json").exists())
            self.assertTrue((call_dir / "telemetry" / "timeline.jsonl").exists())
            self.assertTrue((call_dir / "telemetry" / "control.jsonl").exists())
            self.assertTrue((call_dir / "telemetry" / "media.jsonl").exists())
            self.assertTrue((call_dir / "telemetry" / "turns.jsonl").exists())
            self.assertTrue((call_dir / "telemetry" / "tools.jsonl").exists())

            transcript_text = (call_dir / "transcript.txt").read_text(encoding="utf-8")
            self.assertIn("Caller: გამარჯობა", transcript_text)
            self.assertIn("Assistant: რით შემიძლია დაგეხმაროთ?", transcript_text)

            transcript_events = [
                json.loads(line)
                for line in (call_dir / "transcript.jsonl").read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(transcript_events[0]["flow_current_node"], "isp_router")
            self.assertEqual(transcript_events[0]["flow_branch"], "technical_support")
            self.assertEqual(
                transcript_events[1]["flow_current_node"],
                "isp_technical_support",
            )
            self.assertEqual(
                transcript_events[1]["flow_active_dtmf_kind"],
                "subscriber_identifier",
            )

            ticket_payload = json.loads((call_dir / "ticket.json").read_text(encoding="utf-8"))
            self.assertEqual(ticket_payload["call_status"], "finalized")
            self.assertEqual(ticket_payload["formal_ticket_id"], "sess-1:escalation")
            self.assertEqual(ticket_payload["disposition"], "formal_ticket_created")
            self.assertEqual(ticket_payload["branch"], "human_handoff")
            self.assertEqual(ticket_payload["caller_number"], "+12025550100")

            metadata_payload = json.loads((call_dir / "metadata.json").read_text(encoding="utf-8"))
            self.assertEqual(metadata_payload["status"], "finalized")
            self.assertEqual(metadata_payload["latency"]["turns_started"], 1)
            self.assertEqual(metadata_payload["latency"]["last_first_audio_after_user_stop_ms"], 400)
            self.assertEqual(metadata_payload["gateway"]["call_id"], 7)
            self.assertEqual(metadata_payload["gateway"]["last_event"], "call_media_bridge_stats")
            self.assertEqual(metadata_payload["event_counts"]["total"], 5)
            self.assertEqual(metadata_payload["event_counts"]["by_category"]["control"], 1)
            self.assertEqual(metadata_payload["event_counts"]["by_category"]["media"], 2)
            self.assertEqual(metadata_payload["event_counts"]["by_category"]["turns"], 1)
            self.assertEqual(metadata_payload["event_counts"]["by_category"]["tools"], 1)
            self.assertEqual(metadata_payload["artifacts"]["ticket_payload_json"], "ticket_payload.json")
            self.assertEqual(metadata_payload["counters"]["transcript_events"], 2)
            self.assertEqual(metadata_payload["counters"]["caller_audio_bytes"], 16)
            self.assertEqual(metadata_payload["counters"]["assistant_audio_bytes"], 12)

            with wave.open(str(call_dir / "caller.wav"), "rb") as caller_wav:
                self.assertEqual(caller_wav.getframerate(), 16000)
                self.assertEqual(caller_wav.getnframes(), 8)

            with wave.open(str(call_dir / "assistant.wav"), "rb") as assistant_wav:
                self.assertEqual(assistant_wav.getframerate(), 8000)
                self.assertEqual(assistant_wav.getnframes(), 6)

    def test_writer_resamples_mixed_assistant_sample_rates_into_one_valid_wav(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            writer = CallArtifactWriter(
                session_id="sess-mixed-rates",
                base_dir=tmp_dir,
            )

            writer.append_assistant_audio(
                b"\x01\x00" * 8,
                sample_rate=16000,
            )
            writer.append_assistant_audio(
                b"\x02\x00" * 12,
                sample_rate=24000,
            )

            writer.finalize(
                runtime_snapshot=SessionRuntimeSnapshot(
                    session_id="sess-mixed-rates",
                    state=SessionRuntimeState.ENDED,
                    cancel_reason="hangup_requested:pipeline_stopped",
                    is_cancelled=True,
                    state_changed_at_monotonic_s=0.0,
                    turn_index=0,
                    spoken_turns=0,
                    silent_turns=0,
                ),
                flow_state={
                    "current_node": "isp_router",
                    "branch": None,
                    "active_dtmf_kind": None,
                    "last_tool_id": None,
                },
                tool_state={},
            )

            call_dir = Path(tmp_dir) / "sess-mixed-rates"
            metadata_payload = json.loads((call_dir / "metadata.json").read_text(encoding="utf-8"))
            self.assertEqual(metadata_payload["counters"]["assistant_audio_sample_rate"], 16000)
            self.assertEqual(metadata_payload["counters"]["assistant_audio_bytes"], 32)

            with wave.open(str(call_dir / "assistant.wav"), "rb") as assistant_wav:
                self.assertEqual(assistant_wav.getframerate(), 16000)
                self.assertEqual(assistant_wav.getnframes(), 16)

    def test_ticket_disposition_prioritizes_stop_reason_over_branch(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            writer = CallArtifactWriter(
                session_id="sess-disposition",
                base_dir=tmp_dir,
            )

            writer.finalize(
                runtime_snapshot=SessionRuntimeSnapshot(
                    session_id="sess-disposition",
                    state=SessionRuntimeState.ENDED,
                    cancel_reason="hangup_requested:no_speech_timeout",
                    is_cancelled=True,
                    state_changed_at_monotonic_s=0.0,
                    turn_index=0,
                    spoken_turns=0,
                    silent_turns=1,
                ),
                flow_state={
                    "current_node": "isp_technical_diagnose",
                    "branch": "technical_support",
                    "active_dtmf_kind": None,
                    "last_tool_id": None,
                },
                tool_state={},
            )

            call_dir = Path(tmp_dir) / "sess-disposition"
            ticket_payload = json.loads((call_dir / "ticket.json").read_text(encoding="utf-8"))
            self.assertEqual(ticket_payload["end_reason"], "hangup_requested:no_speech_timeout")
            self.assertEqual(ticket_payload["disposition"], "silent_abandonment")

    def test_ticket_disposition_marks_assistant_completed_hangup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            writer = CallArtifactWriter(
                session_id="sess-ended-cleanly",
                base_dir=tmp_dir,
            )

            writer.finalize(
                runtime_snapshot=SessionRuntimeSnapshot(
                    session_id="sess-ended-cleanly",
                    state=SessionRuntimeState.ENDED,
                    cancel_reason="hangup_requested:assistant_completed",
                    is_cancelled=True,
                    state_changed_at_monotonic_s=0.0,
                    turn_index=1,
                    spoken_turns=1,
                    silent_turns=0,
                ),
                flow_state={
                    "current_node": "isp_technical_basic_troubleshooting",
                    "branch": "technical_support",
                    "active_dtmf_kind": None,
                    "last_tool_id": None,
                },
                tool_state={},
            )

            call_dir = Path(tmp_dir) / "sess-ended-cleanly"
            ticket_payload = json.loads((call_dir / "ticket.json").read_text(encoding="utf-8"))
            self.assertEqual(ticket_payload["end_reason"], "hangup_requested:assistant_completed")
            self.assertEqual(ticket_payload["disposition"], "assistant_completed")
