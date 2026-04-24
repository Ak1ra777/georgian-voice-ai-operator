from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from src.packs.isp.flows.isp_flow import (
    build_billing_identifier_collection_node,
    build_billing_verification_node,
    build_handoff_callback_collection_node,
    build_technical_diagnosis_node,
    build_technical_support_node,
)


def _fake_tool_runtime() -> MagicMock:
    runtime = MagicMock()
    runtime.conversation_context.return_value = {}
    runtime.has_customer_context.return_value = False
    return runtime


class IspFlowLatencyTest(unittest.TestCase):
    def test_technical_support_identifier_node_uses_direct_tts_and_no_immediate_llm(self) -> None:
        node = build_technical_support_node(_fake_tool_runtime())

        self.assertFalse(node["respond_immediately"])
        self.assertEqual(
            [action["type"] for action in node["pre_actions"]],
            ["clear_dtmf_collection", "gateway_tts_say", "begin_dtmf_collection"],
        )
        self.assertIn("კლავიატურით", node["pre_actions"][1]["text"])

    def test_billing_identifier_node_uses_direct_tts_and_no_immediate_llm(self) -> None:
        node = build_billing_identifier_collection_node(_fake_tool_runtime())

        self.assertFalse(node["respond_immediately"])
        self.assertEqual(
            [action["type"] for action in node["pre_actions"]],
            ["clear_dtmf_collection", "gateway_tts_say", "begin_dtmf_collection"],
        )

    def test_billing_verification_node_uses_direct_tts_and_no_immediate_llm(self) -> None:
        node = build_billing_verification_node(_fake_tool_runtime())

        self.assertFalse(node["respond_immediately"])
        self.assertEqual(
            [action["type"] for action in node["pre_actions"]],
            ["clear_dtmf_collection", "gateway_tts_say", "begin_dtmf_collection"],
        )
        self.assertIn("ვერიფიკაციის", node["pre_actions"][1]["text"])

    def test_callback_collection_node_uses_direct_tts_and_no_immediate_llm(self) -> None:
        node = build_handoff_callback_collection_node(_fake_tool_runtime())

        self.assertFalse(node["respond_immediately"])
        self.assertEqual(
            [action["type"] for action in node["pre_actions"]],
            ["clear_dtmf_collection", "gateway_tts_say", "begin_dtmf_collection"],
        )
        self.assertIn("უკუგამოძახების ნომერი", node["pre_actions"][1]["text"])

    def test_known_context_message_compacts_nested_runtime_state(self) -> None:
        runtime = _fake_tool_runtime()
        runtime.conversation_context.return_value = {
            "customer_id": "cust-test-001",
            "service_id": "svc-test-001",
            "outage_summary": "X" * 300,
            "technical_status": {
                "regional_outage_flag": True,
                "wan_status": "down",
                "nested_blob": {"ignored": "value"},
            },
            "lookup_context": {
                "plan_name": "Fiber Max",
                "raw_lookup": {"huge": "payload"},
            },
        }

        node = build_technical_diagnosis_node(runtime)
        content = node["task_messages"][1]["content"]

        self.assertIn('"customer_id": "cust-test-001"', content)
        self.assertIn('"lookup_context_available": true', content)
        self.assertIn('"technical_status"', content)
        self.assertNotIn("raw_lookup", content)
        self.assertNotIn("ignored", content)
        self.assertIn("...", content)
