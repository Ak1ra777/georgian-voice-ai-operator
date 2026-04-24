from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol


class SubscriberLookupTool(Protocol):
    """Layer-3 lookup tool used by graph/business nodes."""

    def lookup(self, identifier: str) -> Any: ...


class VerificationTool(Protocol):
    """Layer-3 verification tool used by graph/business nodes."""

    def verify(self, context_payload: dict[str, Any]) -> bool: ...


class SmsTool(Protocol):
    """Layer-3 SMS side-effect tool used by graph/business nodes."""

    def send_resolved_sms(self, subscriber_phone: str, summary: str) -> None: ...

    def send_escalation_sms(self, callback_number: str, ticket_ref: str) -> None: ...


class FunctionTool(Protocol):
    """Generic deterministic tool callable used by manifest-driven function nodes."""

    def execute(self, **kwargs: Any) -> Any: ...


ToolMode = Literal["deterministic", "model_selectable"]
ToolIdempotency = Literal["required", "best_effort", "none"]


@dataclass(frozen=True)
class ToolRegistryEntry:
    """Typed registry entry for a tool exposed to dialog/runtime layers."""

    tool_id: str
    display_name: str
    description: str
    mode: ToolMode = "deterministic"
    executor: FunctionTool | None = None
    input_schema: dict[str, Any] | None = None
    output_schema: dict[str, Any] | None = None
    timeout_s: float = 30.0
    idempotency: ToolIdempotency = "best_effort"
    adapter_ref: str | None = None
    security_policy_ref: str | None = None
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.timeout_s <= 0.0:
            raise ValueError("ToolRegistryEntry timeout_s must be > 0")


@dataclass
class ToolRegistry:
    """Metadata-backed registry for deterministic and model-selectable tools."""

    entries: dict[str, ToolRegistryEntry] = field(default_factory=dict)

    def register(self, entry: ToolRegistryEntry, *, replace: bool = False) -> None:
        if not replace and entry.tool_id in self.entries:
            raise ValueError(f"Tool already registered: {entry.tool_id}")
        self.entries[entry.tool_id] = entry

    def get(self, tool_id: str) -> ToolRegistryEntry | None:
        return self.entries.get(tool_id)

    def require(self, tool_id: str) -> ToolRegistryEntry:
        entry = self.get(tool_id)
        if entry is None:
            raise RuntimeError(f"Tool not found: {tool_id}")
        return entry

    def require_executable(
        self,
        tool_id: str,
        *,
        mode: ToolMode | None = None,
    ) -> ToolRegistryEntry:
        entry = self.require(tool_id)
        if mode is not None and entry.mode != mode:
            raise RuntimeError(
                f"Tool mode mismatch: expected={mode} actual={entry.mode} tool_id={tool_id}"
            )
        if entry.executor is None:
            raise RuntimeError(f"Tool is not executable: {tool_id}")
        return entry

    def list(self, *, mode: ToolMode | None = None) -> tuple[ToolRegistryEntry, ...]:
        values = tuple(self.entries.values())
        if mode is None:
            return values
        return tuple(entry for entry in values if entry.mode == mode)


@dataclass(frozen=True)
class ToolSet:
    """Tool bundle consumed by the Pipecat runtime and ISP pack adapters."""

    subscriber_lookup: SubscriberLookupTool
    verification_method: VerificationTool
    sms_sender: SmsTool
    tool_registry: ToolRegistry = field(default_factory=ToolRegistry)

    def resolve_tool_entry(self, tool_id: str) -> ToolRegistryEntry | None:
        return self.tool_registry.get(tool_id)

    def require_executable_tool(
        self,
        tool_id: str,
        *,
        mode: ToolMode | None = None,
    ) -> ToolRegistryEntry:
        return self.tool_registry.require_executable(tool_id, mode=mode)
