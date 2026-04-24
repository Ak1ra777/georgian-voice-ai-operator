from __future__ import annotations

import asyncio
import unittest
from unittest.mock import MagicMock, patch

from pipecat_flows import ContextStrategy

from src.pipecat_runtime.flow_runtime import build_gateway_flow_runtime, snapshot_flow_state


class PipecatFlowRuntimeTest(unittest.TestCase):
    class _FakeFlowManager:
        def __init__(self) -> None:
            self.state: dict[str, str] = {}
            self.current_node: str | None = None
            self.actions: dict[str, object] = {}

        def register_action(self, name: str, handler) -> None:
            self.actions[name] = handler

        async def initialize(self, node) -> None:
            self.current_node = node.get("name")

        async def set_node_from_config(self, node) -> None:
            self.current_node = node.get("name")

    def test_build_gateway_flow_runtime_uses_reset_context_strategy_by_default(self) -> None:
        manager = MagicMock()
        initial_node = {"name": "isp_router"}

        with patch(
            "src.pipecat_runtime.flow_runtime.FlowManager",
            return_value=manager,
        ) as flow_manager_cls, patch(
            "src.pipecat_runtime.flow_runtime.build_isp_router_node",
            return_value=initial_node,
        ):
            runtime = build_gateway_flow_runtime(
                session_id="sess-1",
                task=object(),
                llm=object(),
                context_aggregator=object(),
                tool_runtime=MagicMock(),
                dtmf_collector=MagicMock(),
            )

        context_strategy = flow_manager_cls.call_args.kwargs["context_strategy"]
        self.assertEqual(context_strategy.strategy, ContextStrategy.RESET)
        self.assertIs(runtime.manager, manager)
        self.assertEqual(runtime.initial_node, initial_node)

    def test_build_gateway_flow_runtime_can_use_append_context_strategy(self) -> None:
        manager = MagicMock()

        with patch(
            "src.pipecat_runtime.flow_runtime.FlowManager",
            return_value=manager,
        ) as flow_manager_cls, patch(
            "src.pipecat_runtime.flow_runtime.build_isp_router_node",
            return_value={"name": "isp_router"},
        ), patch(
            "src.pipecat_runtime.flow_runtime.PIPECAT_FLOW_CONTEXT_STRATEGY",
            "append",
        ):
            build_gateway_flow_runtime(
                session_id="sess-1",
                task=object(),
                llm=object(),
                context_aggregator=object(),
                tool_runtime=MagicMock(),
                dtmf_collector=MagicMock(),
            )

        context_strategy = flow_manager_cls.call_args.kwargs["context_strategy"]
        self.assertEqual(context_strategy.strategy, ContextStrategy.APPEND)

    def test_flow_runtime_records_node_transitions_and_collection_events(self) -> None:
        manager = self._FakeFlowManager()
        dtmf_collector = MagicMock()
        dtmf_collector.active_kind.return_value = "subscriber_identifier"
        events: list[tuple[str, dict[str, object]]] = []

        with patch(
            "src.pipecat_runtime.flow_runtime.FlowManager",
            return_value=manager,
        ), patch(
            "src.pipecat_runtime.flow_runtime.build_isp_router_node",
            return_value={"name": "isp_router"},
        ):
            runtime = build_gateway_flow_runtime(
                session_id="sess-1",
                task=object(),
                llm=object(),
                context_aggregator=object(),
                tool_runtime=MagicMock(),
                dtmf_collector=dtmf_collector,
                event_recorder=lambda event, data: events.append((event, data)),
            )

        asyncio.run(runtime.manager.initialize({"name": "isp_router"}))
        manager.state["branch"] = "technical_support"
        asyncio.run(runtime.manager.set_node_from_config({"name": "isp_technical_support"}))

        self.assertEqual(
            events,
            [
                (
                    "flow_initialized",
                    {
                        "current_node": "isp_router",
                        "active_dtmf_kind": "subscriber_identifier",
                        "requested_node": "isp_router",
                    },
                ),
                (
                    "flow_node_changed",
                    {
                        "current_node": "isp_technical_support",
                        "branch": "technical_support",
                        "active_dtmf_kind": "subscriber_identifier",
                        "previous_node": "isp_router",
                        "requested_node": "isp_technical_support",
                    },
                ),
            ],
        )

    def test_snapshot_flow_state_includes_manager_and_dtmf_context(self) -> None:
        manager = MagicMock()
        manager.current_node = "isp_handoff_callback_collection"
        manager.state = {
            "branch": "human_handoff",
            "last_tool_id": "create_ticket",
        }
        dtmf_collector = MagicMock()
        dtmf_collector.active_kind.return_value = "callback_number"

        self.assertEqual(
            snapshot_flow_state(
                manager=manager,
                dtmf_collector=dtmf_collector,
            ),
            {
                "current_node": "isp_handoff_callback_collection",
                "branch": "human_handoff",
                "active_dtmf_kind": "callback_number",
                "last_tool_id": "create_ticket",
            },
        )
