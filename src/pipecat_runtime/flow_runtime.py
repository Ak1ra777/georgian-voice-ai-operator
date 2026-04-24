from __future__ import annotations

from dataclasses import dataclass
import inspect
from typing import Any, Callable

from pipecat_flows import ContextStrategy, ContextStrategyConfig, FlowManager, NodeConfig
from pipecat.frames.frames import TTSSpeakFrame

from src.config import PIPECAT_FLOW_CONTEXT_STRATEGY

from src.packs.isp.flows import build_isp_router_node

from .dtmf_capture import DtmfCollectionCoordinator, DtmfCollectionRequest
from .tools import PipecatToolRuntime


@dataclass(frozen=True)
class GatewayFlowRuntime:
    manager: FlowManager
    initial_node: NodeConfig


def build_gateway_flow_runtime(
    *,
    session_id: str,
    task: Any,
    llm: object,
    context_aggregator: Any,
    tool_runtime: PipecatToolRuntime,
    dtmf_collector: DtmfCollectionCoordinator,
    event_recorder: Callable[[str, dict[str, Any]], None] | None = None,
) -> GatewayFlowRuntime:
    _ = session_id
    manager = FlowManager(
        task=task,
        llm=llm,
        context_aggregator=context_aggregator,
        context_strategy=ContextStrategyConfig(
            strategy=_resolve_flow_context_strategy()
        ),
    )

    async def _begin_collection(action: dict[str, Any], flow_manager: FlowManager) -> None:
        await _begin_dtmf_collection_action(
            action=action,
            flow_manager=flow_manager,
            dtmf_collector=dtmf_collector,
            event_recorder=event_recorder,
        )

    async def _clear_collection(action: dict[str, Any], flow_manager: FlowManager) -> None:
        await _clear_dtmf_collection_action(
            action=action,
            flow_manager=flow_manager,
            dtmf_collector=dtmf_collector,
            event_recorder=event_recorder,
        )

    async def _queue_tts_prompt(action: dict[str, Any], flow_manager: FlowManager) -> None:
        await _queue_tts_prompt_action(
            action=action,
            flow_manager=flow_manager,
        )

    manager.register_action("begin_dtmf_collection", _begin_collection)
    manager.register_action("clear_dtmf_collection", _clear_collection)
    manager.register_action("gateway_tts_say", _queue_tts_prompt)
    _instrument_flow_manager(
        manager=manager,
        dtmf_collector=dtmf_collector,
        event_recorder=event_recorder,
    )
    return GatewayFlowRuntime(
        manager=manager,
        initial_node=build_isp_router_node(tool_runtime),
    )


def _resolve_flow_context_strategy() -> ContextStrategy:
    if PIPECAT_FLOW_CONTEXT_STRATEGY == "append":
        return ContextStrategy.APPEND
    return ContextStrategy.RESET


async def _begin_dtmf_collection_action(
    *,
    action: dict[str, Any],
    flow_manager: FlowManager,
    dtmf_collector: DtmfCollectionCoordinator,
    event_recorder: Callable[[str, dict[str, Any]], None] | None = None,
) -> None:
    on_complete = action["on_complete"]
    on_cancel = action.get("on_cancel")
    kind = str(action.get("kind", "generic"))
    submit_key = str(action.get("submit_key", "#"))

    async def _wrapped_complete(value: str) -> None:
        _record_flow_event(
            event_recorder,
            "dtmf_collection_completed",
            manager=flow_manager,
            dtmf_collector=dtmf_collector,
            extra={
                "kind": kind,
                "digits_count": len(value),
            },
        )
        await on_complete(value, flow_manager)

    async def _wrapped_cancel() -> None:
        _record_flow_event(
            event_recorder,
            "dtmf_collection_cleared",
            manager=flow_manager,
            dtmf_collector=dtmf_collector,
            extra={
                "kind": kind,
            },
        )
        if callable(on_cancel):
            await on_cancel(flow_manager)

    await dtmf_collector.start(
        DtmfCollectionRequest(
            kind=kind,
            submit_key=submit_key,
            on_complete=_wrapped_complete,
            on_cancel=_wrapped_cancel,
        )
    )
    _record_flow_event(
        event_recorder,
        "dtmf_collection_started",
        manager=flow_manager,
        dtmf_collector=dtmf_collector,
        extra={
            "kind": kind,
            "submit_key": submit_key,
        },
    )


async def _clear_dtmf_collection_action(
    *,
    action: dict[str, Any],
    flow_manager: FlowManager,
    dtmf_collector: DtmfCollectionCoordinator,
    event_recorder: Callable[[str, dict[str, Any]], None] | None = None,
) -> None:
    _ = (action, flow_manager)
    await dtmf_collector.clear()


async def _queue_tts_prompt_action(
    *,
    action: dict[str, Any],
    flow_manager: FlowManager,
) -> None:
    text = str(action.get("text", "")).strip()
    if not text:
        return
    await flow_manager.task.queue_frame(TTSSpeakFrame(text=text))


def snapshot_flow_state(
    *,
    manager: FlowManager | None,
    dtmf_collector: DtmfCollectionCoordinator | None,
) -> dict[str, str | None]:
    manager_state = getattr(manager, "state", None) or {}
    return {
        "current_node": (
            _normalize_optional_str(getattr(manager, "current_node", None))
            if manager is not None
            else None
        ),
        "branch": _normalize_optional_str(manager_state.get("branch")),
        "active_dtmf_kind": (
            _normalize_optional_str(dtmf_collector.active_kind())
            if dtmf_collector is not None
            else None
        ),
        "last_tool_id": _normalize_optional_str(manager_state.get("last_tool_id")),
    }


def _instrument_flow_manager(
    *,
    manager: FlowManager,
    dtmf_collector: DtmfCollectionCoordinator,
    event_recorder: Callable[[str, dict[str, Any]], None] | None,
) -> None:
    initialize = getattr(manager, "initialize", None)
    if callable(initialize):
        async def _wrapped_initialize(node: NodeConfig) -> Any:
            previous_node = snapshot_flow_state(
                manager=manager,
                dtmf_collector=dtmf_collector,
            ).get("current_node")
            result = initialize(node)
            if inspect.isawaitable(result):
                result = await result
            _record_flow_event(
                event_recorder,
                "flow_initialized",
                manager=manager,
                dtmf_collector=dtmf_collector,
                extra={
                    "previous_node": previous_node,
                    "requested_node": _node_name(node),
                },
            )
            return result

        manager.initialize = _wrapped_initialize

    set_node_from_config = getattr(manager, "set_node_from_config", None)
    if callable(set_node_from_config):
        async def _wrapped_set_node_from_config(node: NodeConfig) -> Any:
            previous_node = snapshot_flow_state(
                manager=manager,
                dtmf_collector=dtmf_collector,
            ).get("current_node")
            result = set_node_from_config(node)
            if inspect.isawaitable(result):
                result = await result
            _record_flow_event(
                event_recorder,
                "flow_node_changed",
                manager=manager,
                dtmf_collector=dtmf_collector,
                extra={
                    "previous_node": previous_node,
                    "requested_node": _node_name(node),
                },
            )
            return result

        manager.set_node_from_config = _wrapped_set_node_from_config


def _record_flow_event(
    event_recorder,
    event: str,
    *,
    manager: FlowManager,
    dtmf_collector: DtmfCollectionCoordinator,
    extra: dict[str, Any] | None = None,
) -> None:
    if event_recorder is None:
        return
    payload: dict[str, Any] = {
        key: value
        for key, value in snapshot_flow_state(
            manager=manager,
            dtmf_collector=dtmf_collector,
        ).items()
        if value not in (None, "")
    }
    for key, value in (extra or {}).items():
        if value not in (None, ""):
            payload[key] = value
    try:
        event_recorder(event, payload)
    except Exception:
        return


def _node_name(node: NodeConfig | None) -> str | None:
    if not isinstance(node, dict):
        return None
    return _normalize_optional_str(node.get("name"))


def _normalize_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None
