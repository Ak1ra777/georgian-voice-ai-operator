from __future__ import annotations

from dataclasses import dataclass, field
import unittest

from src.tools.contracts import ToolRegistry, ToolRegistryEntry, ToolSet


class _FakeLookup:
    def lookup(self, identifier: str):
        _ = identifier
        return None


class _FakeVerification:
    def verify(self, context_payload):
        _ = context_payload
        return True


class _FakeSms:
    def send_resolved_sms(self, subscriber_phone: str, summary: str) -> None:
        _ = (subscriber_phone, summary)

    def send_escalation_sms(self, callback_number: str, ticket_ref: str) -> None:
        _ = (callback_number, ticket_ref)


@dataclass
class _RecordingFunctionTool:
    calls: list[dict[str, object]] = field(default_factory=list)

    def execute(self, **kwargs):
        self.calls.append(dict(kwargs))
        return {"ok": True}


class ToolRegistryTest(unittest.TestCase):
    def test_toolset_uses_supplied_registry_entries(self) -> None:
        tool = _RecordingFunctionTool()
        registry = ToolRegistry()
        registry.register(
            ToolRegistryEntry(
                tool_id="lookup_customer",
                display_name="Lookup Customer",
                description="CRM lookup tool",
                mode="deterministic",
                executor=tool,
            )
        )
        toolset = ToolSet(
            subscriber_lookup=_FakeLookup(),
            verification_method=_FakeVerification(),
            sms_sender=_FakeSms(),
            tool_registry=registry,
        )

        entry = toolset.resolve_tool_entry("lookup_customer")

        self.assertIsNotNone(entry)
        assert entry is not None
        self.assertEqual(entry.mode, "deterministic")
        self.assertEqual(entry.display_name, "Lookup Customer")
        self.assertIs(entry.executor, tool)

    def test_explicit_registry_entry_is_preserved(self) -> None:
        tool = _RecordingFunctionTool()
        registry = ToolRegistry()
        registry.register(
            ToolRegistryEntry(
                tool_id="lookup_customer",
                display_name="Customer Lookup",
                description="CRM lookup tool",
                mode="deterministic",
                executor=tool,
                timeout_s=12.0,
                tags=("crm",),
            )
        )
        toolset = ToolSet(
            subscriber_lookup=_FakeLookup(),
            verification_method=_FakeVerification(),
            sms_sender=_FakeSms(),
            tool_registry=registry,
        )

        entry = toolset.require_executable_tool("lookup_customer", mode="deterministic")

        self.assertEqual(entry.display_name, "Customer Lookup")
        self.assertEqual(entry.description, "CRM lookup tool")
        self.assertEqual(entry.timeout_s, 12.0)
        self.assertEqual(entry.tags, ("crm",))
        self.assertIs(entry.executor, tool)

    def test_register_duplicate_tool_requires_replace(self) -> None:
        registry = ToolRegistry()
        registry.register(
            ToolRegistryEntry(
                tool_id="lookup_customer",
                display_name="Lookup Customer",
                description="First entry",
            )
        )

        with self.assertRaisesRegex(ValueError, "already registered"):
            registry.register(
                ToolRegistryEntry(
                    tool_id="lookup_customer",
                    display_name="Lookup Customer",
                    description="Duplicate entry",
                )
            )

    def test_require_executable_rejects_mode_mismatch(self) -> None:
        registry = ToolRegistry()
        registry.register(
            ToolRegistryEntry(
                tool_id="book_appointment",
                display_name="Book Appointment",
                description="Model-selectable tool",
                mode="model_selectable",
                executor=_RecordingFunctionTool(),
            )
        )

        with self.assertRaisesRegex(RuntimeError, "Tool mode mismatch"):
            registry.require_executable("book_appointment", mode="deterministic")


if __name__ == "__main__":
    unittest.main()
