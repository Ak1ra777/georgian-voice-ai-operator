from __future__ import annotations

import unittest

from pipecat.processors.aggregators.llm_context import LLMContext

from src.packs.isp.contracts import (
    CHECK_OUTAGE_TOOL_ID,
    CREATE_TICKET_TOOL_ID,
    LOAD_SERVICE_CONTEXT_TOOL_ID,
    LOOKUP_CUSTOMER_TOOL_ID,
    SEND_SMS_TOOL_ID,
    VERIFY_CUSTOMER_TOOL_ID,
)
from src.packs.isp.runtime import build_default_isp_toolset
from src.pipecat_runtime.openai_responses import (
    OpenAIResponsesLLMAdapter,
    OpenAIResponsesLLMService,
    OpenAIResponsesLLMSettings,
)
from src.pipecat_runtime.tools import PipecatToolRuntime


class OpenAIResponsesAdapterTest(unittest.TestCase):
    def test_function_tools_force_non_strict_mode(self) -> None:
        runtime = PipecatToolRuntime(
            session_id="sess-tools",
            toolset=build_default_isp_toolset(),
        )

        tools = OpenAIResponsesLLMAdapter().to_provider_tools_format(
            runtime.tools_schema()
        )

        self.assertEqual(
            {tool["name"] for tool in tools},
            {
                LOOKUP_CUSTOMER_TOOL_ID,
                LOAD_SERVICE_CONTEXT_TOOL_ID,
                VERIFY_CUSTOMER_TOOL_ID,
                CHECK_OUTAGE_TOOL_ID,
                CREATE_TICKET_TOOL_ID,
                SEND_SMS_TOOL_ID,
            },
        )
        self.assertTrue(all(tool["type"] == "function" for tool in tools))
        self.assertTrue(all(tool["strict"] is False for tool in tools))

    def test_websocket_service_omits_temperature_from_response_params(self) -> None:
        service = OpenAIResponsesLLMService(
            settings=OpenAIResponsesLLMSettings(
                model="gpt-4.1",
                system_instruction="You are concise.",
                temperature=0.2,
            ),
            api_key="test-api-key",
        )
        context = LLMContext(messages=[{"role": "user", "content": "გამარჯობა"}])
        params = service._build_response_params(
            service.get_llm_adapter().get_llm_invocation_params(
                context,
                system_instruction="You are concise.",
            )
        )

        self.assertNotIn("temperature", params)
