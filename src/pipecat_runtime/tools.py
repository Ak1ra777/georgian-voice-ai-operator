from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
import re
from threading import Lock
from time import monotonic
from typing import Any, Callable

from pydantic import BaseModel, ValidationError

from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.services.llm_service import FunctionCallParams

from src.packs.isp.contracts import (
    CHECK_OUTAGE_TOOL_ID,
    CREATE_TICKET_TOOL_ID,
    CheckOutageInput,
    CheckOutageOutput,
    LOAD_SERVICE_CONTEXT_TOOL_ID,
    LOOKUP_CUSTOMER_TOOL_ID,
    LoadServiceContextInput,
    LoadServiceContextOutput,
    LookupCustomerInput,
    LookupCustomerOutput,
    SEND_SMS_TOOL_ID,
    SendSmsInput,
    SendSmsOutput,
    VERIFY_CUSTOMER_TOOL_ID,
    VerifyCustomerInput,
    VerifyCustomerOutput,
    CreateTicketInput,
    CreateTicketOutput,
)
from src.packs.isp.runtime import build_default_isp_toolset
from src.tools.contracts import ToolRegistryEntry, ToolSet


_EXPOSED_TOOL_IDS = (
    LOOKUP_CUSTOMER_TOOL_ID,
    LOAD_SERVICE_CONTEXT_TOOL_ID,
    VERIFY_CUSTOMER_TOOL_ID,
    CHECK_OUTAGE_TOOL_ID,
    CREATE_TICKET_TOOL_ID,
    SEND_SMS_TOOL_ID,
)

_TOOL_MODELS: dict[str, tuple[type[BaseModel], type[BaseModel]]] = {
    LOOKUP_CUSTOMER_TOOL_ID: (LookupCustomerInput, LookupCustomerOutput),
    LOAD_SERVICE_CONTEXT_TOOL_ID: (
        LoadServiceContextInput,
        LoadServiceContextOutput,
    ),
    VERIFY_CUSTOMER_TOOL_ID: (VerifyCustomerInput, VerifyCustomerOutput),
    CHECK_OUTAGE_TOOL_ID: (CheckOutageInput, CheckOutageOutput),
    CREATE_TICKET_TOOL_ID: (CreateTicketInput, CreateTicketOutput),
    SEND_SMS_TOOL_ID: (SendSmsInput, SendSmsOutput),
}

_SCHEMA_REQUIRED_FIELDS: dict[str, list[str]] = {
    LOOKUP_CUSTOMER_TOOL_ID: ["subscriber_identifier"],
    LOAD_SERVICE_CONTEXT_TOOL_ID: [],
    VERIFY_CUSTOMER_TOOL_ID: [],
    CHECK_OUTAGE_TOOL_ID: [],
    CREATE_TICKET_TOOL_ID: ["ticket_kind"],
    SEND_SMS_TOOL_ID: ["template"],
}

_SCHEMA_DROPPED_FIELDS: dict[str, set[str]] = {
    CREATE_TICKET_TOOL_ID: {"session_id"},
}

_SCHEMA_DESCRIPTION_SUFFIXES: dict[str, str] = {
    LOAD_SERVICE_CONTEXT_TOOL_ID: (
        " If customer_id or service_id is omitted, reuse the active call context when possible."
    ),
    VERIFY_CUSTOMER_TOOL_ID: (
        " If customer_id, subscriber_identifier, or verification_hint is omitted, reuse the active call context when possible."
    ),
    CHECK_OUTAGE_TOOL_ID: (
        " If service_id or service_type is omitted, reuse the active call context when possible."
    ),
    CREATE_TICKET_TOOL_ID: (
        " The active session_id is injected automatically. Missing customer, service, and contact fields are reused from earlier tool results when possible."
    ),
    SEND_SMS_TOOL_ID: (
        " If recipient, ticket_id, summary, or eta_text is omitted, reuse the active call context when possible."
    ),
}


@dataclass
class _ToolSessionState:
    session_id: str
    caller_uri: str | None = None
    caller_number: str | None = None
    subscriber_identifier: str | None = None
    verification_value: str | None = None
    lookup_status: str | None = None
    lookup_message: str | None = None
    verification_status: str | None = None
    verification_reason: str | None = None
    customer_id: str | None = None
    account_id: str | None = None
    company_name: str | None = None
    customer_name: str | None = None
    contact_phone: str | None = None
    callback_number: str | None = None
    service_id: str | None = None
    service_type: str | None = None
    service_packet: str | None = None
    account_status: str | None = None
    account_balance: str | float | int | None = None
    balance_verified: bool = False
    service_address: str | None = None
    lookup_context: dict[str, Any] | None = None
    technical_status: dict[str, Any] | None = None
    verification_hint: str | None = None
    outage_status: str | None = None
    outage_summary: str | None = None
    outage_eta_text: str | None = None
    ticket_id: str | None = None
    last_ticket_summary: str | None = None


@dataclass(frozen=True)
class PipecatToolRuntimeSnapshot:
    calls_started: int
    calls_succeeded: int
    validation_failures: int
    execution_failures: int
    tickets_created: int
    total_duration_ms: int
    max_duration_ms: int
    last_tool_id: str | None
    last_ticket_id: str | None
    call_counts: dict[str, int]


class PipecatToolRuntime:
    """Expose the current ISP toolset to Pipecat function calling."""

    def __init__(
        self,
        *,
        session_id: str,
        toolset: ToolSet | None = None,
        caller_uri: str | None = None,
        caller_number: str | None = None,
        schedule_end_call_fn: Callable[[str], None] | None = None,
        event_recorder: Callable[[str, dict[str, Any]], None] | None = None,
    ) -> None:
        self.session_id = session_id
        self._toolset = toolset or build_default_isp_toolset()
        self._schedule_end_call_fn = schedule_end_call_fn
        self._event_recorder = event_recorder
        normalized_caller_uri = _normalize_optional_str(caller_uri)
        normalized_caller_number = (
            _normalize_optional_str(caller_number)
            or _extract_caller_number(normalized_caller_uri)
        )
        self._state = _ToolSessionState(
            session_id=session_id,
            caller_uri=normalized_caller_uri,
            caller_number=normalized_caller_number,
        )
        self._lock = Lock()
        self._calls_started = 0
        self._calls_succeeded = 0
        self._validation_failures = 0
        self._execution_failures = 0
        self._tickets_created = 0
        self._total_duration_ms = 0
        self._max_duration_ms = 0
        self._last_tool_id: str | None = None
        self._last_ticket_id: str | None = None
        self._call_counts: dict[str, int] = {}
        self._tools_schema = ToolsSchema(
            standard_tools=[
                _tool_entry_to_function_schema(entry)
                for entry in self._iter_executable_entries()
            ]
        )

    @property
    def toolset(self) -> ToolSet:
        return self._toolset

    def tools_schema(self) -> ToolsSchema:
        return self._tools_schema

    def tool_entry(self, tool_id: str) -> ToolRegistryEntry:
        return self._toolset.require_executable_tool(tool_id)

    def function_schema(self, tool_id: str) -> FunctionSchema:
        return _tool_entry_to_function_schema(self.tool_entry(tool_id))

    def snapshot(self) -> PipecatToolRuntimeSnapshot:
        with self._lock:
            return PipecatToolRuntimeSnapshot(
                calls_started=self._calls_started,
                calls_succeeded=self._calls_succeeded,
                validation_failures=self._validation_failures,
                execution_failures=self._execution_failures,
                tickets_created=self._tickets_created,
                total_duration_ms=self._total_duration_ms,
                max_duration_ms=self._max_duration_ms,
                last_tool_id=self._last_tool_id,
                last_ticket_id=self._last_ticket_id,
                call_counts=dict(self._call_counts),
            )

    def has_customer_context(self) -> bool:
        with self._lock:
            return any(
                (
                    not _is_missing(self._state.customer_id),
                    not _is_missing(self._state.account_id),
                    bool(self._state.lookup_context),
                )
            )

    def set_callback_number(self, callback_number: str) -> None:
        with self._lock:
            self._state.callback_number = str(callback_number).strip() or None

    def cached_balance_payload(self) -> dict[str, Any]:
        with self._lock:
            return {
                "verified": bool(self._state.balance_verified),
                "balance": self._state.account_balance,
                "currency": None,
                "account_id": self._state.account_id,
            }

    def clear_balance_verification(self) -> None:
        with self._lock:
            self._state.balance_verified = False
            self._state.verification_value = None

    def schedule_end_call(self, reason: str = "assistant_completed") -> bool:
        if self._schedule_end_call_fn is None:
            return False
        normalized_reason = _normalize_optional_str(reason) or "assistant_completed"
        self._schedule_end_call_fn(normalized_reason)
        return True

    def conversation_context(self) -> dict[str, Any]:
        state = self._snapshot_state()
        context: dict[str, Any] = {}
        fields = {
            "caller_uri": state.caller_uri,
            "caller_number": state.caller_number,
            "subscriber_identifier": state.subscriber_identifier,
            "lookup_status": state.lookup_status,
            "lookup_message": state.lookup_message,
            "verification_status": state.verification_status,
            "verification_reason": state.verification_reason,
            "customer_id": state.customer_id,
            "account_id": state.account_id,
            "company_name": state.company_name,
            "customer_name": state.customer_name,
            "contact_phone": state.contact_phone,
            "callback_number": state.callback_number,
            "service_id": state.service_id,
            "service_type": state.service_type,
            "service_packet": state.service_packet,
            "account_status": state.account_status,
            "service_address": state.service_address,
            "technical_status": state.technical_status,
            "verification_hint": state.verification_hint,
            "outage_status": state.outage_status,
            "outage_summary": state.outage_summary,
            "outage_eta_text": state.outage_eta_text,
            "ticket_id": state.ticket_id,
            "balance_verified": state.balance_verified,
        }
        for key, value in fields.items():
            if _is_missing(value) or value == {}:
                continue
            context[key] = deepcopy(value)

        if state.lookup_context:
            lookup_context = deepcopy(state.lookup_context)
            lookup_context.pop("balance", None)
            lookup_context.pop("raw_lookup", None)
            if lookup_context:
                context["lookup_context"] = lookup_context

        return context

    def artifact_state(self) -> dict[str, Any]:
        state = self._snapshot_state()
        payload: dict[str, Any] = {
            "caller_uri": state.caller_uri,
            "caller_number": state.caller_number,
            "subscriber_identifier": state.subscriber_identifier,
            "lookup_status": state.lookup_status,
            "lookup_message": state.lookup_message,
            "verification_status": state.verification_status,
            "verification_reason": state.verification_reason,
            "customer_id": state.customer_id,
            "account_id": state.account_id,
            "company_name": state.company_name,
            "customer_name": state.customer_name,
            "contact_phone": state.contact_phone,
            "callback_number": state.callback_number,
            "service_id": state.service_id,
            "service_type": state.service_type,
            "service_packet": state.service_packet,
            "account_status": state.account_status,
            "service_address": state.service_address,
            "verification_hint": state.verification_hint,
            "outage_status": state.outage_status,
            "outage_summary": state.outage_summary,
            "outage_eta_text": state.outage_eta_text,
            "ticket_id": state.ticket_id,
            "last_ticket_summary": state.last_ticket_summary,
            "balance_verified": state.balance_verified,
        }
        if state.technical_status:
            payload["technical_status"] = deepcopy(state.technical_status)
        if state.lookup_context:
            payload["lookup_context"] = deepcopy(state.lookup_context)
        return {
            key: value
            for key, value in payload.items()
            if not _is_missing(value) and value != {}
        }

    def register_with_llm(self, llm: object) -> None:
        register_function = getattr(llm, "register_function", None)
        if not callable(register_function):
            return

        for entry in self._iter_executable_entries():
            register_function(
                entry.tool_id,
                self._handle_function_call,
                timeout_secs=entry.timeout_s,
            )

    async def _handle_function_call(self, params: FunctionCallParams) -> None:
        payload = await self.execute_tool(
            tool_id=params.function_name,
            arguments=params.arguments,
        )
        await params.result_callback(payload)

    async def execute_tool(
        self,
        *,
        tool_id: str,
        arguments: dict[str, Any] | Any,
    ) -> dict[str, Any]:
        started_at = monotonic()

        try:
            entry = self._toolset.require_executable_tool(tool_id)
            input_model, output_model = _TOOL_MODELS[tool_id]
            prepared_arguments = self._prepare_arguments(tool_id, arguments)
            self._record_call_started(tool_id)
            print(
                "pipecat_tool_call_started "
                f"session_id={self.session_id} tool_id={tool_id} "
                f"argument_keys={_format_keys(prepared_arguments.keys())}"
            )
            self._record_event(
                "tool_call_started",
                {
                    "tool_id": tool_id,
                    "argument_keys": sorted(str(key) for key in prepared_arguments.keys()),
                    "arguments_preview": _sanitize_tool_preview_payload(
                        prepared_arguments
                    ),
                },
            )
            validated_input = input_model.model_validate(prepared_arguments)
            result = await asyncio.to_thread(
                entry.executor.execute,  # type: ignore[union-attr]
                **validated_input.model_dump(mode="python", exclude_none=True),
            )
            validated_output = output_model.model_validate(result)
            duration_ms = int((monotonic() - started_at) * 1000)
            self._record_success(
                tool_id,
                validated_input,
                validated_output,
                duration_ms=duration_ms,
            )
            payload = _sanitize_tool_payload(
                tool_id=tool_id,
                payload=validated_output.model_dump(mode="json"),
            )
            print(
                "pipecat_tool_call_completed "
                f"session_id={self.session_id} tool_id={tool_id} "
                f"duration_ms={duration_ms} "
                f"result_keys={_format_keys(payload.keys())}"
            )
            self._record_event(
                "tool_call_completed",
                {
                    "tool_id": tool_id,
                    "duration_ms": duration_ms,
                    "result_keys": sorted(str(key) for key in payload.keys()),
                    "result_preview": _sanitize_tool_preview_payload(payload),
                },
            )
            self._log_ticket_artifact(tool_id, payload)
            return payload
        except ValidationError as ex:
            duration_ms = int((monotonic() - started_at) * 1000)
            self._record_failure(tool_id, duration_ms=duration_ms, kind="validation")
            payload = {
                "error": "tool_validation_failed",
                "tool_id": tool_id,
                "message": str(ex),
                "details": ex.errors(include_url=False),
            }
            print(
                "pipecat_tool_call_validation_failed "
                f"session_id={self.session_id} tool_id={tool_id} "
                f"duration_ms={duration_ms}"
            )
            self._record_event(
                "tool_call_validation_failed",
                {
                    "tool_id": tool_id,
                    "duration_ms": duration_ms,
                    "error": str(ex),
                },
                level="warning",
            )
            return payload
        except Exception as ex:
            duration_ms = int((monotonic() - started_at) * 1000)
            self._record_failure(tool_id, duration_ms=duration_ms, kind="execution")
            payload = {
                "error": "tool_execution_failed",
                "tool_id": tool_id,
                "message": str(ex),
            }
            print(
                "pipecat_tool_call_execution_failed "
                f"session_id={self.session_id} tool_id={tool_id} "
                f"duration_ms={duration_ms} error={ex}"
            )
            self._record_event(
                "tool_call_execution_failed",
                {
                    "tool_id": tool_id,
                    "duration_ms": duration_ms,
                    "error": str(ex),
                },
                level="error",
            )
            return payload

    def _iter_executable_entries(self) -> tuple[ToolRegistryEntry, ...]:
        entries: list[ToolRegistryEntry] = []
        for tool_id in _EXPOSED_TOOL_IDS:
            entry = self._toolset.resolve_tool_entry(tool_id)
            if entry is None or entry.executor is None:
                continue
            entries.append(entry)
        return tuple(entries)

    def _prepare_arguments(
        self,
        tool_id: str,
        arguments: dict[str, Any] | Any,
    ) -> dict[str, Any]:
        payload = dict(arguments or {})
        state = self._snapshot_state()

        if tool_id == LOAD_SERVICE_CONTEXT_TOOL_ID:
            _fill_missing(payload, "customer_id", state.customer_id)
            _fill_missing(payload, "service_id", state.service_id)
        elif tool_id == VERIFY_CUSTOMER_TOOL_ID:
            _fill_missing(payload, "customer_id", state.customer_id)
            _fill_missing(payload, "verification_hint", state.verification_hint)
        elif tool_id == CHECK_OUTAGE_TOOL_ID:
            _fill_missing(payload, "service_id", state.service_id)
            _fill_missing(payload, "service_type", state.service_type)
        elif tool_id == CREATE_TICKET_TOOL_ID:
            payload["session_id"] = state.session_id
            _fill_missing(payload, "caller_uri", state.caller_uri)
            _fill_missing(payload, "caller_number", state.caller_number)
            _fill_missing(payload, "customer_id", state.customer_id)
            _fill_missing(payload, "account_id", state.account_id)
            _fill_missing(payload, "company_name", state.company_name)
            _fill_missing(payload, "service_id", state.service_id)
            _fill_missing(payload, "subscriber_identifier", state.subscriber_identifier)
            _fill_missing(payload, "callback_number", state.callback_number)
            _fill_missing(payload, "service_type", state.service_type)
            _fill_missing(payload, "service_packet", state.service_packet)
            _fill_missing(payload, "account_status", state.account_status)
            _fill_missing(payload, "technical_status", state.technical_status)
            _fill_missing(payload, "lookup_context", state.lookup_context)
            _fill_missing(payload, "customer_name", state.customer_name)
            _fill_missing(payload, "contact_phone", state.contact_phone)
            _fill_missing(payload, "service_address", state.service_address)
            _fill_missing(payload, "summary", state.outage_summary or state.last_ticket_summary)
            if _is_missing(payload.get("callback_number")):
                _fill_missing(payload, "callback_number", state.contact_phone)
        elif tool_id == SEND_SMS_TOOL_ID:
            _fill_missing(payload, "recipient", state.callback_number or state.contact_phone)
            _fill_missing(payload, "ticket_id", state.ticket_id)
            _fill_missing(payload, "summary", state.outage_summary or state.last_ticket_summary)
            _fill_missing(payload, "eta_text", state.outage_eta_text)

        return payload

    def _record_success(
        self,
        tool_id: str,
        validated_input: BaseModel,
        validated_output: BaseModel,
        *,
        duration_ms: int,
    ) -> None:
        with self._lock:
            self._calls_succeeded += 1
            self._total_duration_ms += duration_ms
            if duration_ms > self._max_duration_ms:
                self._max_duration_ms = duration_ms
            self._last_tool_id = tool_id
            if tool_id == LOOKUP_CUSTOMER_TOOL_ID:
                self._state.subscriber_identifier = validated_input.subscriber_identifier
                self._state.lookup_status = validated_output.lookup_status
                self._state.lookup_message = validated_output.lookup_message
                if validated_output.customer is None:
                    self._state.verification_status = None
                    self._state.verification_reason = None
                    self._state.customer_id = None
                    self._state.account_id = None
                    self._state.company_name = None
                    self._state.customer_name = None
                    self._state.contact_phone = None
                    self._state.service_id = None
                    self._state.service_type = None
                    self._state.service_packet = None
                    self._state.account_status = None
                    self._state.account_balance = None
                    self._state.balance_verified = False
                    self._state.verification_value = None
                    self._state.verification_hint = None
                    self._state.service_address = None
                    self._state.lookup_context = (
                        dict(validated_output.lookup_context or {}) or None
                    )
                    self._state.technical_status = None
                    self._state.outage_status = None
                    self._state.outage_summary = None
                    self._state.outage_eta_text = None
                    return
                customer = validated_output.customer
                self._state.verification_status = None
                self._state.verification_reason = None
                self._state.customer_id = customer.customer_id
                self._state.account_id = customer.account_id
                self._state.company_name = customer.company_name
                self._state.customer_name = customer.customer_name
                self._state.contact_phone = customer.phone_number
                self._state.service_id = customer.service_id
                self._state.service_type = customer.service_type
                self._state.service_packet = customer.service_packet
                self._state.account_status = customer.account_status
                self._state.account_balance = customer.balance
                self._state.balance_verified = False
                self._state.verification_value = None
                self._state.verification_hint = customer.verification_hint
                self._state.service_address = customer.service_address
                self._state.lookup_context = (
                    dict(validated_output.lookup_context or {}) or None
                )
                self._state.technical_status = dict(customer.technical_status or {}) or None
                self._state.outage_status = None
                self._state.outage_summary = None
                self._state.outage_eta_text = None
                return

            if tool_id == LOAD_SERVICE_CONTEXT_TOOL_ID:
                if validated_output.service_found:
                    if not _is_missing(validated_output.service_id):
                        self._state.service_id = validated_output.service_id
                    if not _is_missing(validated_output.service_type):
                        self._state.service_type = validated_output.service_type
                    if not _is_missing(validated_output.service_packet):
                        self._state.service_packet = validated_output.service_packet
                    if validated_output.account_status not in (None, "", "unknown"):
                        self._state.account_status = validated_output.account_status
                if not _is_missing(validated_output.service_address):
                    self._state.service_address = validated_output.service_address
                if validated_output.technical_status:
                    self._state.technical_status = (
                        dict(validated_output.technical_status) or None
                    )
                return

            if tool_id == VERIFY_CUSTOMER_TOOL_ID:
                self._state.customer_id = (
                    validated_input.customer_id or self._state.customer_id
                )
                self._state.verification_value = validated_input.subscriber_identifier
                self._state.balance_verified = bool(validated_output.verified)
                self._state.verification_status = validated_output.verification_status
                self._state.verification_reason = validated_output.reason
                self._state.verification_hint = (
                    validated_input.verification_hint or self._state.verification_hint
                )
                return

            if tool_id == CHECK_OUTAGE_TOOL_ID:
                self._state.service_id = validated_input.service_id or self._state.service_id
                self._state.service_type = (
                    validated_input.service_type or self._state.service_type
                )
                self._state.outage_status = validated_output.outage_status
                self._state.outage_summary = validated_output.summary
                self._state.outage_eta_text = validated_output.eta_text
                return

            if tool_id == CREATE_TICKET_TOOL_ID:
                self._state.callback_number = (
                    validated_input.callback_number or self._state.callback_number
                )
                self._state.contact_phone = (
                    validated_input.contact_phone or self._state.contact_phone
                )
                self._state.caller_uri = (
                    validated_input.caller_uri or self._state.caller_uri
                )
                self._state.caller_number = (
                    validated_input.caller_number or self._state.caller_number
                )
                self._state.customer_id = (
                    validated_input.customer_id or self._state.customer_id
                )
                self._state.customer_name = (
                    validated_input.customer_name or self._state.customer_name
                )
                self._state.account_id = (
                    validated_input.account_id or self._state.account_id
                )
                self._state.company_name = (
                    validated_input.company_name or self._state.company_name
                )
                self._state.service_id = (
                    validated_input.service_id or self._state.service_id
                )
                self._state.service_type = (
                    validated_input.service_type or self._state.service_type
                )
                self._state.service_packet = (
                    validated_input.service_packet or self._state.service_packet
                )
                self._state.account_status = (
                    validated_input.account_status or self._state.account_status
                )
                if validated_input.lookup_context:
                    self._state.lookup_context = dict(validated_input.lookup_context) or None
                if validated_input.technical_status:
                    self._state.technical_status = dict(validated_input.technical_status) or None
                self._state.service_address = (
                    validated_input.service_address or self._state.service_address
                )
                self._state.last_ticket_summary = validated_input.summary
                self._state.ticket_id = validated_output.ticket_id or self._state.ticket_id
                if getattr(validated_output, "created", False):
                    self._tickets_created += 1
                    self._last_ticket_id = validated_output.ticket_id
                return

            if tool_id == SEND_SMS_TOOL_ID:
                self._state.ticket_id = validated_input.ticket_id or self._state.ticket_id
                if not _is_missing(validated_input.recipient):
                    self._state.callback_number = validated_input.recipient
                if not _is_missing(validated_input.summary):
                    self._state.last_ticket_summary = validated_input.summary

    def _snapshot_state(self) -> _ToolSessionState:
        with self._lock:
            return _ToolSessionState(**self._state.__dict__)

    def _record_call_started(self, tool_id: str) -> None:
        with self._lock:
            self._calls_started += 1
            self._last_tool_id = tool_id
            self._call_counts[tool_id] = self._call_counts.get(tool_id, 0) + 1

    def _record_failure(
        self,
        tool_id: str,
        *,
        duration_ms: int,
        kind: str,
    ) -> None:
        with self._lock:
            self._total_duration_ms += duration_ms
            if duration_ms > self._max_duration_ms:
                self._max_duration_ms = duration_ms
            self._last_tool_id = tool_id
            if kind == "validation":
                self._validation_failures += 1
            else:
                self._execution_failures += 1

    def _log_ticket_artifact(self, tool_id: str, payload: dict[str, Any]) -> None:
        if tool_id != CREATE_TICKET_TOOL_ID or payload.get("ticket_status") != "created":
            return

        artifact_path = Path("outputs/calls") / self.session_id / "ticket_payload.json"
        print(
            "pipecat_ticket_artifact_status "
            f"session_id={self.session_id} ticket_id={payload.get('ticket_id')} "
            f"artifact_path={artifact_path} artifact_found={artifact_path.exists()}"
        )
        self._record_event(
            "ticket_artifact_status",
            {
                "tool_id": tool_id,
                "ticket_id": payload.get("ticket_id"),
                "artifact_path": str(artifact_path),
                "artifact_found": artifact_path.exists(),
            },
        )

    def _record_event(
        self,
        event: str,
        data: dict[str, Any],
        *,
        level: str = "info",
    ) -> None:
        if self._event_recorder is None:
            return
        try:
            self._event_recorder(
                event,
                {
                    "level": level,
                    **data,
                },
            )
        except Exception:
            return


def _tool_entry_to_function_schema(entry: ToolRegistryEntry) -> FunctionSchema:
    schema = deepcopy(entry.input_schema or {})
    properties = dict(schema.get("properties") or {})
    for field_name in _SCHEMA_DROPPED_FIELDS.get(entry.tool_id, ()):
        properties.pop(field_name, None)

    required = [
        field_name
        for field_name in _SCHEMA_REQUIRED_FIELDS.get(entry.tool_id, schema.get("required", []))
        if field_name in properties
    ]
    description = entry.description + _SCHEMA_DESCRIPTION_SUFFIXES.get(entry.tool_id, "")
    return FunctionSchema(
        name=entry.tool_id,
        description=description,
        properties=properties,
        required=required,
    )


def _fill_missing(payload: dict[str, Any], key: str, value: Any) -> None:
    if _is_missing(payload.get(key)) and not _is_missing(value):
        payload[key] = value


def _normalize_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _format_keys(keys) -> str:
    values = [str(key).strip() for key in keys if str(key).strip()]
    if not values:
        return "-"
    return ",".join(sorted(values))


def _sanitize_tool_payload(*, tool_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    if tool_id != LOOKUP_CUSTOMER_TOOL_ID:
        return payload

    sanitized = deepcopy(payload)
    customer = sanitized.get("customer")
    if isinstance(customer, dict):
        customer.pop("balance", None)

    lookup_context = sanitized.get("lookup_context")
    if isinstance(lookup_context, dict):
        lookup_context.pop("balance", None)
        lookup_context.pop("raw_lookup", None)

    return sanitized


_SENSITIVE_PREVIEW_FIELDS = {
    "subscriber_identifier",
    "verification_value",
    "verification_hint",
    "caller_number",
    "callback_number",
    "contact_phone",
    "customer_id",
    "account_id",
    "service_id",
    "service_address",
    "customer_name",
    "company_name",
    "lookup_context",
    "customer_context",
    "technical_status",
}


def _sanitize_tool_preview_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        sanitized: dict[str, Any] = {}
        for key, value in payload.items():
            normalized_key = str(key).strip()
            if normalized_key in _SENSITIVE_PREVIEW_FIELDS:
                sanitized[normalized_key] = _redact_sensitive_value(value)
                continue
            sanitized[normalized_key] = _sanitize_tool_preview_payload(value)
        return sanitized
    if isinstance(payload, list):
        return [_sanitize_tool_preview_payload(item) for item in payload]
    return payload


def _redact_sensitive_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict):
        return {
            "redacted": True,
            "keys": sorted(str(key) for key in value.keys()),
        }
    if isinstance(value, list):
        return {
            "redacted": True,
            "items": len(value),
        }
    text = str(value).strip()
    if not text:
        return None
    return {
        "redacted": True,
        "length": len(text),
    }


def _extract_caller_number(remote_uri: str | None) -> str | None:
    uri = _normalize_optional_str(remote_uri)
    if uri is None:
        return None

    if "<" in uri and ">" in uri:
        inner = uri.split("<", 1)[1].split(">", 1)[0].strip()
        if inner:
            uri = inner

    for prefix in ("sip:", "sips:", "tel:"):
        if uri.lower().startswith(prefix):
            uri = uri[len(prefix):]
            break

    uri = uri.split(";", 1)[0].split("@", 1)[0].split("?", 1)[0].strip()
    if not uri:
        return None

    match = re.findall(r"[+]?\d+", uri)
    if not match:
        return None

    candidate = "".join(match)
    digits_only = candidate.replace("+", "")
    if len(digits_only) < 5:
        return None
    return candidate
