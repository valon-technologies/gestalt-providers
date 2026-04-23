import json
from dataclasses import dataclass
from typing import Any

from litellm import completion

from .config import SimpleAgentConfig


@dataclass(slots=True)
class BackendToolCall:
    call_id: str
    tool_id: str
    arguments: dict[str, Any]


@dataclass(slots=True)
class BackendStep:
    assistant_message: dict[str, Any]
    output_text: str
    tool_calls: list[BackendToolCall]


class LiteLLMBackend:
    def __init__(self, config: SimpleAgentConfig) -> None:
        self._config = config

    def complete(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        provider_options: dict[str, Any],
    ) -> BackendStep:
        completion_options = self._completion_options(model=model, provider_options=provider_options)
        response = completion(
            model=model,
            messages=messages,
            tools=tools or None,
            timeout=completion_options.pop("timeout", self._config.timeout_seconds),
            **completion_options,
        )
        choice = response.choices[0]
        message = choice.message
        tool_calls = self._parse_tool_calls(message.tool_calls or [])
        assistant_message: dict[str, Any] = {"role": "assistant"}
        if message.content:
            assistant_message["content"] = self._normalize_content(message.content)
        if message.tool_calls:
            assistant_message["tool_calls"] = [tool_call.model_dump() for tool_call in message.tool_calls]
        return BackendStep(
            assistant_message=assistant_message,
            output_text=self._normalize_content(message.content),
            tool_calls=tool_calls,
        )

    def _completion_options(self, *, model: str, provider_options: dict[str, Any]) -> dict[str, Any]:
        options = {
            key: value
            for key, value in provider_options.items()
            if not isinstance(value, dict)
        }
        nested = provider_options.get("litellm")
        if isinstance(nested, dict):
            options.update(nested)

        provider_prefix = model.split("/", 1)[0] if "/" in model else ""
        provider_nested = provider_options.get(provider_prefix)
        if isinstance(provider_nested, dict):
            options.update(provider_nested)
        return options

    def _parse_tool_calls(self, raw_tool_calls: list[Any]) -> list[BackendToolCall]:
        calls: list[BackendToolCall] = []
        for tool_call in raw_tool_calls:
            raw_arguments = getattr(tool_call.function, "arguments", "") or "{}"
            try:
                arguments = json.loads(raw_arguments)
            except json.JSONDecodeError as exc:
                raise ValueError(f"tool call {tool_call.id!r} returned non-JSON arguments: {exc}") from exc
            if not isinstance(arguments, dict):
                raise ValueError(f"tool call {tool_call.id!r} arguments must decode to an object")
            calls.append(
                BackendToolCall(
                    call_id=str(tool_call.id or "").strip(),
                    tool_id=str(tool_call.function.name or "").strip(),
                    arguments=arguments,
                )
            )
        return calls

    def _normalize_content(self, content: Any) -> str:
        if content is None:
            return ""
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                    continue
                if isinstance(item, dict):
                    text = item.get("text")
                    if isinstance(text, str) and text:
                        parts.append(text)
            return "\n".join(part for part in parts if part)
        return str(content)
