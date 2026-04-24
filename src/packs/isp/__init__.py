from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .contracts import (
        CHECK_OUTAGE_TOOL_ID,
        CREATE_TICKET_TOOL_ID,
        LOOKUP_CUSTOMER_TOOL_ID,
        LOAD_SERVICE_CONTEXT_TOOL_ID,
        SEND_SMS_TOOL_ID,
        VERIFY_CUSTOMER_TOOL_ID,
        build_isp_tool_registry,
        isp_tool_registry_entries,
    )
    from .runtime import build_default_isp_toolset
    from .stubs import AlwaysVerified, LookupResult, StubSmsSender, StubSubscriberLookup
    from .tickets import IspTicketPayload, save_isp_ticket_payload


__all__ = [
    "CHECK_OUTAGE_TOOL_ID",
    "CREATE_TICKET_TOOL_ID",
    "LOOKUP_CUSTOMER_TOOL_ID",
    "LOAD_SERVICE_CONTEXT_TOOL_ID",
    "SEND_SMS_TOOL_ID",
    "VERIFY_CUSTOMER_TOOL_ID",
    "build_default_isp_toolset",
    "build_isp_tool_registry",
    "isp_tool_registry_entries",
    "AlwaysVerified",
    "IspTicketPayload",
    "LookupResult",
    "StubSmsSender",
    "StubSubscriberLookup",
    "save_isp_ticket_payload",
]


_EXPORTS = {
    "CHECK_OUTAGE_TOOL_ID": (".contracts", "CHECK_OUTAGE_TOOL_ID"),
    "CREATE_TICKET_TOOL_ID": (".contracts", "CREATE_TICKET_TOOL_ID"),
    "LOOKUP_CUSTOMER_TOOL_ID": (".contracts", "LOOKUP_CUSTOMER_TOOL_ID"),
    "LOAD_SERVICE_CONTEXT_TOOL_ID": (".contracts", "LOAD_SERVICE_CONTEXT_TOOL_ID"),
    "SEND_SMS_TOOL_ID": (".contracts", "SEND_SMS_TOOL_ID"),
    "VERIFY_CUSTOMER_TOOL_ID": (".contracts", "VERIFY_CUSTOMER_TOOL_ID"),
    "build_default_isp_toolset": (".runtime", "build_default_isp_toolset"),
    "build_isp_tool_registry": (".contracts", "build_isp_tool_registry"),
    "isp_tool_registry_entries": (".contracts", "isp_tool_registry_entries"),
    "AlwaysVerified": (".stubs", "AlwaysVerified"),
    "IspTicketPayload": (".tickets", "IspTicketPayload"),
    "LookupResult": (".stubs", "LookupResult"),
    "StubSmsSender": (".stubs", "StubSmsSender"),
    "StubSubscriberLookup": (".stubs", "StubSubscriberLookup"),
    "save_isp_ticket_payload": (".tickets", "save_isp_ticket_payload"),
}


def __getattr__(name: str):
    try:
        module_name, attr_name = _EXPORTS[name]
    except KeyError as ex:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        ) from ex

    module = import_module(module_name, __name__)
    value = getattr(module, attr_name)
    globals()[name] = value
    return value
