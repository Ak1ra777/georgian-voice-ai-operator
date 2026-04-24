from __future__ import annotations

import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from src.packs.isp import (
    CREATE_TICKET_TOOL_ID,
    IspTicketPayload,
    build_default_isp_toolset,
    save_isp_ticket_payload,
)


class IspTicketingTest(unittest.TestCase):
    def test_save_isp_ticket_payload_writes_pack_owned_json(self) -> None:
        ticket = IspTicketPayload(
            session_id="isp-ticket-1",
            ticket_type="resolved",
            service_type="fiber",
            subscriber_identifier="100100",
            customer_context={
                "customer_id": "cust-test-001",
                "service_id": "svc-test-001",
            },
            handoff_summary="Issue resolved after router reboot.",
        )

        with TemporaryDirectory() as tmp_dir:
            out_path = save_isp_ticket_payload(ticket, out_dir=tmp_dir)

            self.assertEqual(out_path, Path(tmp_dir) / "isp-ticket-1.json")
            payload = json.loads(out_path.read_text(encoding="utf-8"))

        self.assertEqual(payload["ticket_type"], "resolved")
        self.assertEqual(payload["customer_context"]["customer_id"], "cust-test-001")

    def test_save_isp_ticket_payload_defaults_to_per_call_artifact_path(self) -> None:
        ticket = IspTicketPayload(
            session_id="isp-ticket-default",
            ticket_type="escalation",
            handoff_summary="Needs technician callback.",
        )

        with TemporaryDirectory() as tmp_dir:
            previous_cwd = os.getcwd()
            try:
                os.chdir(tmp_dir)
                out_path = save_isp_ticket_payload(ticket)
            finally:
                os.chdir(previous_cwd)

            self.assertEqual(
                out_path,
                Path("outputs/calls/isp-ticket-default/ticket_payload.json"),
            )
            payload = json.loads(
                (Path(tmp_dir) / out_path).read_text(encoding="utf-8")
            )

        self.assertEqual(payload["session_id"], "isp-ticket-default")
        self.assertEqual(payload["ticket_type"], "escalation")

    def test_default_create_ticket_executor_persists_pack_ticket_payload(self) -> None:
        saved: list[IspTicketPayload] = []

        def _capture(ticket: IspTicketPayload) -> Path:
            saved.append(ticket)
            return Path("outputs/test-ticket.json")

        toolset = build_default_isp_toolset(save_ticket_payload_fn=_capture)
        entry = toolset.require_executable_tool(CREATE_TICKET_TOOL_ID)
        executor = entry.executor
        assert executor is not None

        result = executor.execute(
            session_id="isp-ticket-2",
            ticket_kind="escalation",
            customer_id="cust-test-002",
            service_id="svc-test-002",
            subscriber_identifier="100101",
            callback_number="5550101",
            service_type="wireless",
            summary="Needs field technician.",
            customer_name="Example Customer",
            contact_phone="5550101",
            service_address="Example City",
        )

        self.assertEqual(result["ticket_status"], "created")
        self.assertEqual(len(saved), 1)
        self.assertIsInstance(saved[0], IspTicketPayload)
        self.assertEqual(saved[0].ticket_type, "escalation")
        self.assertEqual(saved[0].customer_context["customer_id"], "cust-test-002")
        self.assertEqual(saved[0].callback_number, "5550101")


if __name__ == "__main__":
    unittest.main()
