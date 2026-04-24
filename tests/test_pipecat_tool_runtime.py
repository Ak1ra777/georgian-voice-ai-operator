from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pipecat.pipeline.task import PipelineTask
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.services.llm_service import FunctionCallParams

from src.packs.isp.contracts import (
    CHECK_OUTAGE_TOOL_ID,
    CREATE_TICKET_TOOL_ID,
    LOAD_SERVICE_CONTEXT_TOOL_ID,
    LOOKUP_CUSTOMER_TOOL_ID,
    SEND_SMS_TOOL_ID,
    VERIFY_CUSTOMER_TOOL_ID,
)
from src.packs.isp.runtime import build_default_isp_toolset
from src.packs.isp.stubs import StubSubscriberLookup
from src.packs.isp.tickets import save_isp_ticket_payload
from src.pipecat_runtime.pipeline_factory import build_gateway_pipeline
from src.pipecat_runtime.policy import SessionPolicyController
from src.pipecat_runtime.tools import PipecatToolRuntime
from src.runtime.calls.session.runtime import SessionRuntime


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


class _FakeLlmProcessor(_PassThroughProcessor):
    def __init__(self) -> None:
        super().__init__("fake-llm")
        self.registered_functions: dict[str, object] = {}
        self.timeout_by_function: dict[str, float | None] = {}

    def register_function(
        self,
        function_name: str | None,
        handler,
        *,
        cancel_on_interruption: bool = True,
        timeout_secs: float | None = None,
    ) -> None:
        _ = cancel_on_interruption
        assert function_name is not None
        self.registered_functions[function_name] = handler
        self.timeout_by_function[function_name] = timeout_secs


class _SelectiveVerification:
    def __init__(self, expected_value: str) -> None:
        self.expected_value = expected_value

    def verify(self, context_payload: dict[str, object]) -> bool:
        return context_payload.get("subscriber_identifier") == self.expected_value


class PipecatToolRuntimeTest(unittest.TestCase):
    def test_tools_schema_exposes_executable_isp_tools_and_hides_session_id(self) -> None:
        runtime = PipecatToolRuntime(
            session_id="sess-tools",
            toolset=build_default_isp_toolset(),
        )

        schema = runtime.tools_schema()
        tool_names = {tool.name for tool in schema.standard_tools}

        self.assertEqual(
            tool_names,
            {
                LOOKUP_CUSTOMER_TOOL_ID,
                LOAD_SERVICE_CONTEXT_TOOL_ID,
                VERIFY_CUSTOMER_TOOL_ID,
                CHECK_OUTAGE_TOOL_ID,
                CREATE_TICKET_TOOL_ID,
                SEND_SMS_TOOL_ID,
            },
        )

        ticket_schema = next(
            tool for tool in schema.standard_tools if tool.name == CREATE_TICKET_TOOL_ID
        )
        self.assertNotIn("session_id", ticket_schema.properties)
        self.assertEqual(ticket_schema.required, ["ticket_kind"])

    def test_runtime_can_schedule_end_call(self) -> None:
        scheduled: list[str] = []
        runtime = PipecatToolRuntime(
            session_id="sess-tools",
            toolset=build_default_isp_toolset(),
            schedule_end_call_fn=lambda reason: scheduled.append(reason),
        )

        self.assertTrue(runtime.schedule_end_call())
        self.assertEqual(scheduled, ["assistant_completed"])

    def test_pipeline_flow_initializes_router_and_routes_to_technical_support(self) -> None:
        fake_llm = _FakeLlmProcessor()
        policy = SessionPolicyController(
            session_id="sess-1",
            runtime=SessionRuntime(session_id="sess-1"),
            no_speech_timeout_s=10.0,
            max_duration_s=300.0,
        )

        artifacts = self._build_pipeline(
            session_id="sess-1",
            bridge=_FakeBridge(),
            policy_controller=policy,
            stt_factory=lambda: _PassThroughProcessor("fake-stt"),
            llm_factory=lambda: fake_llm,
            tts_factory=lambda: _PassThroughProcessor("fake-tts"),
        )

        self.assertIsNotNone(artifacts.flow_runtime)

        asyncio.run(
            artifacts.flow_runtime.manager.initialize(artifacts.flow_runtime.initial_node)
        )

        self.assertEqual(artifacts.flow_runtime.manager.current_node, "isp_router")
        self.assertEqual(
            set(fake_llm.registered_functions.keys()),
            {
                "end_call",
                "route_to_technical_support",
                "route_to_billing",
                "route_to_human_handoff",
            },
        )

        asyncio.run(
            self._invoke_registered_function(
                fake_llm,
                "route_to_technical_support",
                {},
                "tool-call-router-1",
            )
        )

        self.assertEqual(
            artifacts.flow_runtime.manager.current_node,
            "isp_technical_support",
        )
        self.assertEqual(
            artifacts.flow_runtime.manager.state["branch"],
            "technical_support",
        )
        self.assertTrue(
            {
                "end_call",
                "route_to_billing",
                "route_to_human_handoff",
            }.issubset(set(fake_llm.registered_functions.keys()))
        )
        self.assertIsNotNone(artifacts.dtmf_collector)
        self.assertEqual(
            artifacts.dtmf_collector.active_kind(),
            "subscriber_identifier",
        )

    def test_technical_support_dtmf_lookup_loads_context_before_diagnosis(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            toolset = build_default_isp_toolset(
                subscriber_lookup=StubSubscriberLookup(
                    found_identifiers={
                        "100100": {
                            "customer_id": "cust-test-001",
                            "account_id": "acct-test-001",
                            "company_name": "Example ISP",
                            "customer_name": "Example Customer",
                            "phone": "5550100",
                            "service_id": "svc-test-001",
                            "service_type": "fiber",
                            "service_packet": "100 Mbps",
                            "account_status": "active",
                            "service_address": "Example City",
                            "technical_status": {
                                "regional_outage_flag": True,
                                "wan_status": "down",
                            },
                            "verification_hint": "ID",
                        }
                    }
                ),
                save_ticket_payload_fn=lambda ticket: save_isp_ticket_payload(
                    ticket,
                    out_dir=tmp_dir,
                ),
            )
            fake_llm = _FakeLlmProcessor()
            policy = SessionPolicyController(
                session_id="sess-1",
                runtime=SessionRuntime(session_id="sess-1"),
                no_speech_timeout_s=10.0,
                max_duration_s=300.0,
            )

            artifacts = self._build_pipeline(
                session_id="sess-1",
                bridge=_FakeBridge(),
                policy_controller=policy,
                caller_uri="sip:+12025550100@example.test",
                stt_factory=lambda: _PassThroughProcessor("fake-stt"),
                llm_factory=lambda: fake_llm,
                tts_factory=lambda: _PassThroughProcessor("fake-tts"),
                toolset=toolset,
            )

            self.assertIsInstance(artifacts.task, PipelineTask)
            self.assertIsNotNone(artifacts.flow_runtime)
            asyncio.run(
                artifacts.flow_runtime.manager.initialize(artifacts.flow_runtime.initial_node)
            )
            asyncio.run(
                self._invoke_registered_function(
                    fake_llm,
                    "route_to_technical_support",
                    {},
                    "tool-call-router-1",
                )
            )

            asyncio.run(
                self._submit_dtmf(
                    artifacts,
                    "100100#",
                )
            )
            lookup_result = artifacts.flow_runtime.manager.state["lookup_result"]
            self.assertEqual(lookup_result["lookup_status"], "found")
            self.assertEqual(
                artifacts.flow_runtime.manager.current_node,
                "isp_technical_diagnose",
            )
            self.assertEqual(lookup_result["customer"]["account_id"], "acct-test-001")
            self.assertEqual(
                lookup_result["customer"]["technical_status"]["regional_outage_flag"],
                True,
            )
            self.assertEqual(
                artifacts.flow_runtime.manager.state["service_context_result"]["service_id"],
                "svc-test-001",
            )
            self.assertTrue(
                {
                    "end_call",
                    LOAD_SERVICE_CONTEXT_TOOL_ID,
                    CHECK_OUTAGE_TOOL_ID,
                    "route_to_basic_troubleshooting",
                    "route_to_billing",
                    "route_to_human_handoff",
                }.issubset(set(fake_llm.registered_functions.keys()))
            )

            asyncio.run(
                self._invoke_registered_function(
                    fake_llm,
                    "route_to_human_handoff",
                    {},
                    "tool-call-route-handoff-1",
                )
            )
            self.assertEqual(
                artifacts.flow_runtime.manager.current_node,
                "isp_human_handoff",
            )

            ticket_result = asyncio.run(
                self._invoke_registered_function(
                    fake_llm,
                    CREATE_TICKET_TOOL_ID,
                    {
                        "ticket_kind": "escalation",
                        "summary": "Needs technician callback.",
                    },
                    "tool-call-2",
                )
            )
            self.assertEqual(ticket_result["ticket_status"], "created")
            self.assertEqual(ticket_result["ticket_id"], "sess-1:escalation")

            tool_snapshot = artifacts.tool_runtime.snapshot()
            self.assertEqual(tool_snapshot.calls_started, 3)
            self.assertEqual(tool_snapshot.calls_succeeded, 3)
            self.assertEqual(tool_snapshot.validation_failures, 0)
            self.assertEqual(tool_snapshot.execution_failures, 0)
            self.assertEqual(tool_snapshot.tickets_created, 1)
            self.assertEqual(tool_snapshot.last_ticket_id, "sess-1:escalation")
            self.assertEqual(
                tool_snapshot.call_counts,
                {
                    LOOKUP_CUSTOMER_TOOL_ID: 1,
                    LOAD_SERVICE_CONTEXT_TOOL_ID: 1,
                    CREATE_TICKET_TOOL_ID: 1,
                },
            )

            saved_ticket_path = Path(tmp_dir) / "sess-1.json"
            self.assertTrue(saved_ticket_path.exists())
            payload = json.loads(saved_ticket_path.read_text(encoding="utf-8"))

            self.assertEqual(payload["session_id"], "sess-1")
            self.assertEqual(payload["ticket_type"], "escalation")
            self.assertEqual(payload["customer_context"]["customer_id"], "cust-test-001")
            self.assertEqual(payload["customer_context"]["account_id"], "acct-test-001")
            self.assertEqual(payload["customer_context"]["company_name"], "Example ISP")
            self.assertEqual(payload["customer_context"]["service_id"], "svc-test-001")
            self.assertEqual(
                payload["customer_context"]["technical_status"]["wan_status"],
                "down",
            )
            self.assertEqual(payload["customer_name"], "Example Customer")
            self.assertEqual(payload["contact_phone"], "5550100")
            self.assertEqual(payload["handoff_summary"], "Needs technician callback.")

    def test_billing_exact_balance_requires_dtmf_lookup_then_dtmf_verification(self) -> None:
        toolset = build_default_isp_toolset(
            subscriber_lookup=StubSubscriberLookup(
                found_identifiers={
                    "100100": {
                        "customer_id": "cust-test-001",
                        "account_id": "acct-test-001",
                        "company_name": "Example ISP",
                        "customer_name": "Example Customer",
                        "phone": "5550100",
                        "service_id": "svc-test-001",
                        "service_type": "fiber",
                        "service_packet": "100 Mbps",
                        "account_status": "active",
                        "balance": 18.75,
                        "technical_status": {
                            "regional_outage_flag": False,
                            "wan_status": "up",
                        },
                        "verification_hint": "last4_id",
                    }
                }
            ),
            verification_method=_SelectiveVerification(expected_value="4321"),
        )
        fake_llm = _FakeLlmProcessor()
        policy = SessionPolicyController(
            session_id="sess-1",
            runtime=SessionRuntime(session_id="sess-1"),
            no_speech_timeout_s=10.0,
            max_duration_s=300.0,
        )

        artifacts = self._build_pipeline(
            session_id="sess-1",
            bridge=_FakeBridge(),
            policy_controller=policy,
            stt_factory=lambda: _PassThroughProcessor("fake-stt"),
            llm_factory=lambda: fake_llm,
            tts_factory=lambda: _PassThroughProcessor("fake-tts"),
            toolset=toolset,
        )

        self.assertIsNotNone(artifacts.flow_runtime)
        asyncio.run(
            artifacts.flow_runtime.manager.initialize(artifacts.flow_runtime.initial_node)
        )
        asyncio.run(
            self._invoke_registered_function(
                fake_llm,
                "route_to_billing",
                {},
                "tool-call-router-billing-1",
            )
        )
        self.assertEqual(artifacts.flow_runtime.manager.current_node, "isp_billing")

        asyncio.run(
            self._invoke_registered_function(
                fake_llm,
                "route_to_balance_verification",
                {},
                "tool-call-balance-route-1",
            )
        )
        self.assertEqual(
            artifacts.flow_runtime.manager.current_node,
            "isp_billing_identifier_collection",
        )
        self.assertEqual(artifacts.dtmf_collector.active_kind(), "billing_identifier")

        asyncio.run(self._submit_dtmf(artifacts, "100100#"))
        self.assertEqual(
            artifacts.flow_runtime.manager.current_node,
            "isp_billing_verification",
        )
        self.assertEqual(artifacts.dtmf_collector.active_kind(), "billing_verification")

        asyncio.run(self._submit_dtmf(artifacts, "4321#"))
        self.assertEqual(
            artifacts.flow_runtime.manager.current_node,
            "isp_billing_exact_balance",
        )

        balance_result = asyncio.run(
            self._invoke_registered_function(
                fake_llm,
                "get_cached_balance",
                {},
                "tool-call-balance-1",
            )
        )
        self.assertEqual(balance_result["verified"], True)
        self.assertEqual(balance_result["account_id"], "acct-test-001")
        self.assertEqual(balance_result["balance"], 18.75)

        tool_snapshot = artifacts.tool_runtime.snapshot()
        self.assertEqual(
            tool_snapshot.call_counts,
            {
                LOOKUP_CUSTOMER_TOOL_ID: 1,
                VERIFY_CUSTOMER_TOOL_ID: 1,
            },
        )

    def test_handoff_collects_alternate_callback_number_via_dtmf(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            toolset = build_default_isp_toolset(
                save_ticket_payload_fn=lambda ticket: save_isp_ticket_payload(
                    ticket,
                    out_dir=tmp_dir,
                )
            )
            fake_llm = _FakeLlmProcessor()
            policy = SessionPolicyController(
                session_id="sess-1",
                runtime=SessionRuntime(session_id="sess-1"),
                no_speech_timeout_s=10.0,
                max_duration_s=300.0,
            )

            artifacts = self._build_pipeline(
                session_id="sess-1",
                bridge=_FakeBridge(),
                policy_controller=policy,
                caller_uri="sip:+12025550100@example.test",
                stt_factory=lambda: _PassThroughProcessor("fake-stt"),
                llm_factory=lambda: fake_llm,
                tts_factory=lambda: _PassThroughProcessor("fake-tts"),
                toolset=toolset,
            )

            self.assertIsNotNone(artifacts.flow_runtime)
            asyncio.run(
                artifacts.flow_runtime.manager.initialize(artifacts.flow_runtime.initial_node)
            )
            asyncio.run(
                self._invoke_registered_function(
                    fake_llm,
                    "route_to_human_handoff",
                    {},
                    "tool-call-router-handoff-1",
                )
            )
            self.assertEqual(
                artifacts.flow_runtime.manager.current_node,
                "isp_human_handoff",
            )

            asyncio.run(
                self._invoke_registered_function(
                    fake_llm,
                    "collect_alternate_callback_number",
                    {},
                    "tool-call-callback-1",
                )
            )
            self.assertEqual(
                artifacts.flow_runtime.manager.current_node,
                "isp_handoff_callback_collection",
            )
            self.assertEqual(artifacts.dtmf_collector.active_kind(), "callback_number")

            asyncio.run(self._submit_dtmf(artifacts, "5550101#"))
            self.assertEqual(
                artifacts.flow_runtime.manager.current_node,
                "isp_human_handoff",
            )

            ticket_result = asyncio.run(
                self._invoke_registered_function(
                    fake_llm,
                    CREATE_TICKET_TOOL_ID,
                    {
                        "ticket_kind": "callback_request",
                        "summary": "Caller requested callback.",
                    },
                    "tool-call-ticket-callback-1",
                )
            )
            self.assertEqual(ticket_result["ticket_status"], "created")

            payload = json.loads(
                (Path(tmp_dir) / "sess-1.json").read_text(encoding="utf-8")
            )
            self.assertEqual(payload["caller_number"], "+12025550100")
            self.assertEqual(payload["callback_number"], "5550101")
            self.assertEqual(payload["handoff_summary"], "Caller requested callback.")

    async def _invoke_registered_function(
        self,
        fake_llm: _FakeLlmProcessor,
        function_name: str,
        arguments: dict[str, object],
        tool_call_id: str,
    ) -> dict[str, object]:
        handler = fake_llm.registered_functions[function_name]
        results: list[dict[str, object]] = []

        async def _result_callback(result, *, properties=None) -> None:
            results.append(result)
            if properties is not None and properties.on_context_updated is not None:
                await properties.on_context_updated()

        params = FunctionCallParams(
            function_name=function_name,
            tool_call_id=tool_call_id,
            arguments=arguments,
            llm=fake_llm,
            context=LLMContext(messages=[]),
            result_callback=_result_callback,
        )
        await handler(params)
        self.assertEqual(len(results), 1)
        return results[0]

    def _build_pipeline(self, **kwargs):
        with patch(
            "src.pipecat_runtime.pipeline_factory.LocalSmartTurnAnalyzerV3"
        ), patch(
            "src.pipecat_runtime.pipeline_factory.SileroVADAnalyzer"
        ):
            return build_gateway_pipeline(**kwargs)

    async def _submit_dtmf(self, artifacts, sequence: str) -> None:
        assert artifacts.dtmf_collector is not None
        for digit in sequence:
            await artifacts.dtmf_collector.handle_digit(digit)


if __name__ == "__main__":
    unittest.main()
