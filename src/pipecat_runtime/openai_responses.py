from __future__ import annotations

from copy import deepcopy
from typing import Any

from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.adapters.services.open_ai_responses_adapter import (
    OpenAIResponsesLLMAdapter as _BaseOpenAIResponsesLLMAdapter,
)
from pipecat.services.openai.responses.llm import (
    OpenAIResponsesLLMService as _BaseOpenAIResponsesLLMService,
    OpenAIResponsesLLMSettings,
)


def _strip_none(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_none(item)
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, list):
        return [_strip_none(item) for item in value]
    return value


class OpenAIResponsesLLMAdapter(_BaseOpenAIResponsesLLMAdapter):
    """Repo-local adapter fixes invalid function tool payloads for Responses WS."""

    def to_provider_tools_format(self, tools_schema: ToolsSchema) -> list[dict[str, Any]]:
        tools = super().to_provider_tools_format(tools_schema)
        normalized_tools: list[dict[str, Any]] = []
        for tool in tools:
            if not isinstance(tool, dict):
                normalized_tools.append(tool)
                continue

            normalized_tool = _strip_none(deepcopy(tool))
            if normalized_tool.get("type") == "function":
                normalized_tool["strict"] = False
            normalized_tools.append(normalized_tool)
        return normalized_tools


class OpenAIResponsesLLMService(_BaseOpenAIResponsesLLMService):
    adapter_class = OpenAIResponsesLLMAdapter

    def _build_response_params(self, invocation_params):
        params = super()._build_response_params(invocation_params)
        # OpenAI Responses over WebSocket accepts the request shape this repo
        # needs, but closes the connection cleanly when `temperature` is sent.
        # Keep the websocket path stable by relying on the model default.
        params.pop("temperature", None)
        return params


__all__ = [
    "OpenAIResponsesLLMAdapter",
    "OpenAIResponsesLLMService",
    "OpenAIResponsesLLMSettings",
]
