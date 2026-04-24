from __future__ import annotations

import unittest

from src.packs.isp.http_lookup import HttpSubscriberLookup


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload
        self.raise_called = False

    def raise_for_status(self) -> None:
        self.raise_called = True

    def json(self) -> dict[str, object]:
        return dict(self._payload)


class _FakeSession:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload
        self.calls: list[dict[str, object]] = []

    def post(self, url: str, *, headers, json, timeout):
        self.calls.append(
            {
                "url": url,
                "headers": dict(headers),
                "json": dict(json),
                "timeout": timeout,
            }
        )
        return _FakeResponse(self.payload)


class HttpSubscriberLookupTest(unittest.TestCase):
    def test_lookup_normalizes_account_service_and_technical_context(self) -> None:
        session = _FakeSession(
            {
                "success": True,
                "message": "ok",
                "data": {
                    "id": "cust-test-007",
                    "account_id": "acct-test-007",
                    "company_name": "Example ISP",
                    "customer_name": "Example Customer",
                    "mobile": "5550100",
                    "address": "Example City",
                    "service": {
                        "type": "fiber",
                        "packet": "100 Mbps",
                        "balance": 0,
                        "account_status": "active",
                    },
                    "technical_status": {
                        "regional_outage_flag": False,
                        "wan_status": "down",
                        "signal_alarm": False,
                    },
                },
            }
        )
        lookup = HttpSubscriberLookup(
            url="http://example.test/lookup",
            api_key="secret",
            timeout_s=12.0,
            session=session,
        )

        result = lookup.lookup("acct-test-007")

        self.assertTrue(result.found)
        self.assertEqual(result.message, "ok")
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(
            session.calls[0]["json"],
            {
                "action": "get_one",
                "account_id": "acct-test-007",
            },
        )
        self.assertEqual(result.customer_context["customer_id"], "cust-test-007")
        self.assertEqual(result.customer_context["account_id"], "acct-test-007")
        self.assertEqual(result.customer_context["service_type"], "fiber")
        self.assertEqual(result.customer_context["service_packet"], "100 Mbps")
        self.assertEqual(result.customer_context["account_status"], "active")
        self.assertEqual(result.customer_context["service_address"], "Example City")
        self.assertEqual(
            result.customer_context["technical_status"]["wan_status"],
            "down",
        )


if __name__ == "__main__":
    unittest.main()
