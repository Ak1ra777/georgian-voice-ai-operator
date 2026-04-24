from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import requests

from .stubs import LookupResult


class _HttpSession(Protocol):
    def post(
        self,
        url: str,
        *,
        headers: dict[str, str],
        json: dict[str, Any],
        timeout: float,
    ) -> Any: ...


@dataclass
class HttpSubscriberLookup:
    """HTTP-backed subscriber lookup for the ISP account API."""

    url: str
    api_key: str
    timeout_s: float = 10.0
    session: _HttpSession | None = None

    def lookup(self, identifier: str) -> LookupResult:
        response = self._session().post(
            self.url,
            headers={
                "X-API-KEY": self.api_key,
                "Content-Type": "application/json",
            },
            json={
                "action": "get_one",
                "account_id": str(identifier).strip(),
            },
            timeout=self.timeout_s,
        )
        response.raise_for_status()

        payload = response.json()
        found = bool(payload.get("success")) and isinstance(payload.get("data"), dict)
        return LookupResult(
            found=found,
            customer_context=_normalize_lookup_payload(payload, identifier=str(identifier)),
            message=_string_or_none(payload.get("message")),
            raw_response=payload if isinstance(payload, dict) else None,
        )

    def _session(self) -> _HttpSession:
        return self.session or requests


def _normalize_lookup_payload(
    payload: dict[str, Any],
    *,
    identifier: str,
) -> dict[str, Any]:
    data = payload.get("data")
    if not isinstance(data, dict):
        data = {}

    service = data.get("service")
    if not isinstance(service, dict):
        service = {}

    technical_status = data.get("technical_status")
    if not isinstance(technical_status, dict):
        technical_status = {}

    service_type = _string_or_none(service.get("type") or data.get("service_type"))
    account_id = _string_or_none(data.get("account_id")) or identifier

    normalized: dict[str, Any] = {
        "customer_id": _string_or_none(data.get("id")) or account_id,
        "account_id": account_id,
        "company_name": _string_or_none(data.get("company_name")),
        "customer_name": _string_or_none(data.get("customer_name")),
        "phone": _string_or_none(data.get("mobile") or data.get("phone")),
        "phone_number": _string_or_none(data.get("mobile") or data.get("phone")),
        "service_id": _string_or_none(service.get("id")) or account_id,
        "service_type": service_type,
        "service_packet": _string_or_none(service.get("packet") or data.get("packet")),
        "account_status": _string_or_none(
            service.get("account_status") or data.get("account_status")
        ),
        "service_address": _string_or_none(data.get("address")),
        "balance": service.get("balance"),
        "technical_status": technical_status,
        "raw_lookup": payload,
        "verification_hint": _string_or_none(data.get("verification_hint")),
    }
    return {key: value for key, value in normalized.items() if value not in (None, {})}


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
