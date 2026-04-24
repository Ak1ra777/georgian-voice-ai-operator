from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from src.config import (
    ISP_ACCOUNT_API_KEY,
    ISP_ACCOUNT_API_TIMEOUT_S,
    ISP_ACCOUNT_API_URL,
)
from src.tools.contracts import ToolSet

from .contracts import (
    CHECK_OUTAGE_TOOL_ID,
    CREATE_TICKET_TOOL_ID,
    LOOKUP_CUSTOMER_TOOL_ID,
    LOAD_SERVICE_CONTEXT_TOOL_ID,
    SEND_SMS_TOOL_ID,
    VERIFY_CUSTOMER_TOOL_ID,
    build_isp_tool_registry,
)
from .http_lookup import HttpSubscriberLookup
from .stubs import AlwaysVerified, StubSmsSender, StubSubscriberLookup
from .tickets import IspTicketPayload, save_isp_ticket_payload


def build_default_isp_toolset(
    *,
    save_ticket_payload_fn: Callable[[IspTicketPayload], Any] | None = None,
    subscriber_lookup: Any | None = None,
    verification_method: Any | None = None,
    sms_sender: Any | None = None,
) -> ToolSet:
    subscriber_lookup = subscriber_lookup or _build_default_subscriber_lookup()
    verification_method = verification_method or AlwaysVerified()
    sms_sender = sms_sender or StubSmsSender()
    tool_registry = build_isp_tool_registry(
        executors={
            LOOKUP_CUSTOMER_TOOL_ID: _LookupCustomerExecutor(subscriber_lookup),
            LOAD_SERVICE_CONTEXT_TOOL_ID: _LoadServiceContextExecutor(),
            VERIFY_CUSTOMER_TOOL_ID: _VerifyCustomerExecutor(verification_method),
            CHECK_OUTAGE_TOOL_ID: _CheckOutageExecutor(),
            CREATE_TICKET_TOOL_ID: _CreateTicketExecutor(
                save_ticket_payload_fn=(
                    save_ticket_payload_fn or save_isp_ticket_payload
                ),
            ),
            SEND_SMS_TOOL_ID: _SendSmsExecutor(sms_sender),
        }
    )
    return ToolSet(
        subscriber_lookup=subscriber_lookup,
        verification_method=verification_method,
        sms_sender=sms_sender,
        tool_registry=tool_registry,
    )


def _build_default_subscriber_lookup() -> Any:
    if ISP_ACCOUNT_API_URL and ISP_ACCOUNT_API_KEY:
        return HttpSubscriberLookup(
            url=ISP_ACCOUNT_API_URL,
            api_key=ISP_ACCOUNT_API_KEY,
            timeout_s=ISP_ACCOUNT_API_TIMEOUT_S,
        )
    return StubSubscriberLookup()


@dataclass
class _LookupCustomerExecutor:
    subscriber_lookup: Any

    def execute(self, *, subscriber_identifier: str) -> dict[str, Any]:
        result = self.subscriber_lookup.lookup(subscriber_identifier)
        found = bool(getattr(result, "found", False))
        lookup_message = getattr(result, "message", None)
        raw_response = getattr(result, "raw_response", None)
        if not found:
            return {
                "found": False,
                "lookup_status": "not_found",
                "customer": None,
                "lookup_message": lookup_message or "Account not found.",
                "lookup_context": (
                    {"raw_lookup": raw_response}
                    if isinstance(raw_response, dict)
                    else {}
                ),
            }

        payload = dict(getattr(result, "customer_context", {}) or {})
        return {
            "found": True,
            "lookup_status": "found",
            "customer": {
                "customer_id": str(
                    payload.get("customer_id")
                    or payload.get("account_id")
                    or subscriber_identifier
                ),
                "account_id": _string_or_none(payload.get("account_id")),
                "company_name": _string_or_none(payload.get("company_name")),
                "customer_name": payload.get("customer_name"),
                "phone_number": payload.get("phone")
                or payload.get("phone_number"),
                "service_id": payload.get("service_id"),
                "service_type": _normalize_service_type(payload.get("service_type")),
                "service_packet": _string_or_none(payload.get("service_packet")),
                "account_status": _string_or_none(payload.get("account_status")),
                "service_address": _string_or_none(payload.get("service_address")),
                "balance": payload.get("balance"),
                "technical_status": _dict_or_empty(payload.get("technical_status")),
                "verification_hint": payload.get("verification_hint"),
            },
            "lookup_message": lookup_message,
            "lookup_context": payload,
        }


class _LoadServiceContextExecutor:
    def execute(
        self,
        *,
        customer_id: str,
        service_id: str | None = None,
    ) -> dict[str, Any]:
        _ = customer_id
        return {
            "service_found": True,
            "service_id": service_id,
            "service_type": None,
            "account_status": "unknown",
            "service_address": None,
            "service_packet": None,
            "technical_status": {},
            "context_summary": None,
        }


@dataclass
class _VerifyCustomerExecutor:
    verification_method: Any

    def execute(
        self,
        *,
        customer_id: str,
        subscriber_identifier: str,
        verification_hint: str | None = None,
    ) -> dict[str, Any]:
        verified = bool(
            self.verification_method.verify(
                {
                    "customer_id": customer_id,
                    "subscriber_identifier": subscriber_identifier,
                    "verification_hint": verification_hint,
                }
            )
        )
        return {
            "verified": verified,
            "verification_status": "verified" if verified else "failed",
            "reason": None if verified else "verification_failed",
        }


class _CheckOutageExecutor:
    def execute(
        self,
        *,
        service_id: str,
        service_type: str | None = None,
    ) -> dict[str, Any]:
        _ = (service_id, service_type)
        return {
            "outage_found": False,
            "outage_status": "clear",
            "outage_id": None,
            "summary": None,
            "eta_text": None,
        }


@dataclass
class _CreateTicketExecutor:
    save_ticket_payload_fn: Callable[[IspTicketPayload], Any]

    def execute(
        self,
        *,
        session_id: str,
        ticket_kind: str,
        caller_uri: str | None = None,
        caller_number: str | None = None,
        customer_id: str | None = None,
        account_id: str | None = None,
        company_name: str | None = None,
        service_id: str | None = None,
        subscriber_identifier: str | None = None,
        callback_number: str | None = None,
        service_type: str | None = None,
        service_packet: str | None = None,
        account_status: str | None = None,
        technical_status: dict[str, Any] | None = None,
        lookup_context: dict[str, Any] | None = None,
        summary: str | None = None,
        handoff_reason: str | None = None,
        customer_name: str | None = None,
        contact_phone: str | None = None,
        service_address: str | None = None,
    ) -> dict[str, Any]:
        ticket = IspTicketPayload(
            session_id=session_id,
            ticket_type=ticket_kind,
            service_type=service_type,
            subscriber_identifier=subscriber_identifier,
            callback_number=callback_number,
            no_action_reason=(
                ticket_kind
                if ticket_kind in {"subscriber_not_found", "verification_failed"}
                else None
            ),
            caller_uri=caller_uri,
            caller_number=caller_number,
            contact_phone=contact_phone,
            customer_name=customer_name,
            service_address=service_address,
            handoff_summary=summary or handoff_reason or "",
            customer_context=_build_ticket_context(
                customer_id=customer_id,
                account_id=account_id,
                company_name=company_name,
                service_id=service_id,
                service_type=service_type,
                service_packet=service_packet,
                account_status=account_status,
                technical_status=technical_status,
                service_address=service_address,
                lookup_context=lookup_context,
            ),
        )
        self.save_ticket_payload_fn(ticket)
        return {
            "created": True,
            "ticket_id": f"{session_id}:{ticket_kind}",
            "ticket_status": "created",
            "queue_name": "technical_support",
        }


@dataclass
class _SendSmsExecutor:
    sms_sender: Any

    def execute(
        self,
        *,
        template: str,
        recipient: str,
        ticket_id: str | None = None,
        summary: str | None = None,
        eta_text: str | None = None,
    ) -> dict[str, Any]:
        if recipient:
            if template in {"resolved", "outage"}:
                body = summary or ""
                if eta_text:
                    body = f"{body} {eta_text}".strip()
                self.sms_sender.send_resolved_sms(recipient, body)
            else:
                self.sms_sender.send_escalation_sms(recipient, ticket_id or "")
        return {
            "sent": bool(recipient),
            "delivery_status": "sent" if recipient else "failed",
            "provider_message_id": None,
        }


def _build_ticket_context(
    *,
    customer_id: str | None,
    account_id: str | None,
    company_name: str | None,
    service_id: str | None,
    service_type: str | None,
    service_packet: str | None,
    account_status: str | None,
    technical_status: dict[str, Any] | None,
    service_address: str | None,
    lookup_context: dict[str, Any] | None,
) -> dict[str, Any]:
    context = dict(lookup_context or {})
    fields = {
        "customer_id": customer_id,
        "account_id": account_id,
        "company_name": company_name,
        "service_id": service_id,
        "service_type": service_type,
        "service_packet": service_packet,
        "account_status": account_status,
        "technical_status": technical_status,
        "service_address": service_address,
    }
    for key, value in fields.items():
        if value not in (None, {}):
            context[key] = value
    return context


def _normalize_service_type(value: Any) -> str | None:
    text = _string_or_none(value)
    if text is None:
        return None
    normalized = text.lower()
    if normalized in {"fiber", "wireless"}:
        return normalized
    return text


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _dict_or_empty(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}
