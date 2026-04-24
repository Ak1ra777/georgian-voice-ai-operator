from __future__ import annotations

from typing import Any, Literal, Mapping

from pydantic import BaseModel, Field, model_validator

from src.tools.contracts import FunctionTool, ToolRegistry, ToolRegistryEntry

LOOKUP_CUSTOMER_TOOL_ID = "lookup_customer"
LOAD_SERVICE_CONTEXT_TOOL_ID = "load_service_context"
VERIFY_CUSTOMER_TOOL_ID = "verify_customer"
CHECK_OUTAGE_TOOL_ID = "check_outage"
CREATE_TICKET_TOOL_ID = "create_ticket"
SEND_SMS_TOOL_ID = "send_sms"



class IspCustomerRecord(BaseModel):
    customer_id: str
    account_id: str | None = None
    company_name: str | None = None
    customer_name: str | None = None
    phone_number: str | None = None
    service_id: str | None = None
    service_type: str | None = None
    service_packet: str | None = None
    account_status: str | None = None
    service_address: str | None = None
    balance: str | float | int | None = None
    technical_status: dict[str, Any] = Field(default_factory=dict)
    verification_hint: str | None = None


class LookupCustomerInput(BaseModel):
    subscriber_identifier: str = Field(min_length=6, max_length=16)


class LookupCustomerOutput(BaseModel):
    found: bool
    lookup_status: Literal["found", "not_found"] = "not_found"
    customer: IspCustomerRecord | None = None
    lookup_message: str | None = None
    lookup_context: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_customer_presence(self):
        if self.found and self.customer is None:
            raise ValueError("LookupCustomerOutput requires customer when found=True")
        if not self.found:
            self.lookup_status = "not_found"
        elif self.lookup_status != "found":
            self.lookup_status = "found"
        return self


class LoadServiceContextInput(BaseModel):
    customer_id: str
    service_id: str | None = None


class LoadServiceContextOutput(BaseModel):
    service_found: bool = True
    service_id: str | None = None
    service_type: str | None = None
    account_status: str = "unknown"
    service_address: str | None = None
    service_packet: str | None = None
    technical_status: dict[str, Any] = Field(default_factory=dict)
    context_summary: str | None = None


class VerifyCustomerInput(BaseModel):
    customer_id: str
    subscriber_identifier: str
    verification_hint: str | None = None


class VerifyCustomerOutput(BaseModel):
    verified: bool
    verification_status: Literal["verified", "failed"] = "failed"
    reason: str | None = None

    @model_validator(mode="after")
    def _normalize_status(self):
        self.verification_status = "verified" if self.verified else "failed"
        return self


class CheckOutageInput(BaseModel):
    service_id: str
    service_type: str | None = None


class CheckOutageOutput(BaseModel):
    outage_found: bool
    outage_status: Literal["outage", "clear", "unknown"] = "unknown"
    outage_id: str | None = None
    summary: str | None = None
    eta_text: str | None = None

    @model_validator(mode="after")
    def _normalize_status(self):
        if self.outage_found:
            self.outage_status = "outage"
        elif self.outage_status == "outage":
            self.outage_status = "clear"
        return self


class CreateTicketInput(BaseModel):
    session_id: str
    ticket_kind: Literal[
        "resolved",
        "escalation",
        "outage",
        "callback_request",
        "subscriber_not_found",
        "verification_failed",
    ]
    caller_uri: str | None = None
    caller_number: str | None = None
    customer_id: str | None = None
    account_id: str | None = None
    company_name: str | None = None
    service_id: str | None = None
    subscriber_identifier: str | None = None
    callback_number: str | None = None
    service_type: str | None = None
    service_packet: str | None = None
    account_status: str | None = None
    technical_status: dict[str, Any] | None = None
    lookup_context: dict[str, Any] | None = None
    summary: str | None = None
    handoff_reason: str | None = None
    customer_name: str | None = None
    contact_phone: str | None = None
    service_address: str | None = None


class CreateTicketOutput(BaseModel):
    created: bool
    ticket_id: str | None = None
    ticket_status: Literal["created", "failed"] = "failed"
    queue_name: str | None = None

    @model_validator(mode="after")
    def _normalize_status(self):
        self.ticket_status = "created" if self.created else "failed"
        return self


class SendSmsInput(BaseModel):
    template: Literal["outage", "resolved", "callback"]
    recipient: str
    ticket_id: str | None = None
    summary: str | None = None
    eta_text: str | None = None


class SendSmsOutput(BaseModel):
    sent: bool
    delivery_status: Literal["sent", "failed"] = "failed"
    provider_message_id: str | None = None

    @model_validator(mode="after")
    def _normalize_status(self):
        self.delivery_status = "sent" if self.sent else "failed"
        return self


def isp_tool_registry_entries(
    *,
    executors: Mapping[str, FunctionTool] | None = None,
) -> tuple[ToolRegistryEntry, ...]:
    executor_map = dict(executors or {})
    return (
        _tool_entry(
            tool_id=LOOKUP_CUSTOMER_TOOL_ID,
            display_name="Lookup Customer",
            description="Lookup a subscriber by identifier and return customer context.",
            input_model=LookupCustomerInput,
            output_model=LookupCustomerOutput,
            executor=executor_map.get(LOOKUP_CUSTOMER_TOOL_ID),
            tags=("pack:isp", "lookup"),
        ),
        _tool_entry(
            tool_id=LOAD_SERVICE_CONTEXT_TOOL_ID,
            display_name="Load Service Context",
            description="Load the service context for a known ISP customer and service.",
            input_model=LoadServiceContextInput,
            output_model=LoadServiceContextOutput,
            executor=executor_map.get(LOAD_SERVICE_CONTEXT_TOOL_ID),
            tags=("pack:isp", "context"),
        ),
        _tool_entry(
            tool_id=VERIFY_CUSTOMER_TOOL_ID,
            display_name="Verify Customer",
            description="Verify the caller before disclosing ISP account details.",
            input_model=VerifyCustomerInput,
            output_model=VerifyCustomerOutput,
            executor=executor_map.get(VERIFY_CUSTOMER_TOOL_ID),
            tags=("pack:isp", "verification"),
        ),
        _tool_entry(
            tool_id=CHECK_OUTAGE_TOOL_ID,
            display_name="Check Outage",
            description="Check whether a known customer service is impacted by an outage.",
            input_model=CheckOutageInput,
            output_model=CheckOutageOutput,
            executor=executor_map.get(CHECK_OUTAGE_TOOL_ID),
            tags=("pack:isp", "outage"),
        ),
        _tool_entry(
            tool_id=CREATE_TICKET_TOOL_ID,
            display_name="Create Ticket",
            description="Create an ISP support or callback ticket for the active call.",
            input_model=CreateTicketInput,
            output_model=CreateTicketOutput,
            executor=executor_map.get(CREATE_TICKET_TOOL_ID),
            tags=("pack:isp", "ticket"),
        ),
        _tool_entry(
            tool_id=SEND_SMS_TOOL_ID,
            display_name="Send SMS",
            description="Send an outbound ISP SMS update to the caller or callback number.",
            input_model=SendSmsInput,
            output_model=SendSmsOutput,
            executor=executor_map.get(SEND_SMS_TOOL_ID),
            tags=("pack:isp", "messaging"),
        ),
    )


def build_isp_tool_registry(
    *,
    executors: Mapping[str, FunctionTool] | None = None,
) -> ToolRegistry:
    registry = ToolRegistry()
    for entry in isp_tool_registry_entries(executors=executors):
        registry.register(entry)
    return registry


def _tool_entry(
    *,
    tool_id: str,
    display_name: str,
    description: str,
    input_model: type[BaseModel],
    output_model: type[BaseModel],
    executor: FunctionTool | None,
    tags: tuple[str, ...],
) -> ToolRegistryEntry:
    return ToolRegistryEntry(
        tool_id=tool_id,
        display_name=display_name,
        description=description,
        mode="deterministic",
        executor=executor,
        input_schema=input_model.model_json_schema(),
        output_schema=output_model.model_json_schema(),
        timeout_s=20.0,
        idempotency="best_effort",
        adapter_ref=f"packs.isp.{tool_id}",
        security_policy_ref="customer_support_standard",
        tags=tags,
    )
