from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class LookupResult:
    found: bool
    customer_context: dict[str, Any] = field(default_factory=dict)
    message: str | None = None
    raw_response: dict[str, Any] | None = None


class StubSubscriberLookup:
    """Pack-local dev stub for Georgian ISP customer lookup."""

    def __init__(
        self,
        found_identifiers: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self._found = found_identifiers or {}

    def lookup(self, identifier: str) -> LookupResult:
        payload = self._found.get(identifier)
        if payload is not None:
            return LookupResult(found=True, customer_context=dict(payload))
        return LookupResult(found=False)


class AlwaysVerified:
    """Pack-local dev stub for customer verification."""

    def verify(self, context_payload: dict[str, Any]) -> bool:
        _ = context_payload
        return True


class StubSmsSender:
    """Pack-local log-only SMS stub."""

    def send_resolved_sms(self, subscriber_phone: str, summary: str) -> None:
        print(
            f"isp_sms_stub_resolved phone={subscriber_phone} "
            f"summary_chars={len(summary)}"
        )

    def send_escalation_sms(self, callback_number: str, ticket_ref: str) -> None:
        print(
            f"isp_sms_stub_escalation callback={callback_number} "
            f"ticket_ref={ticket_ref}"
        )
