import json
from dataclasses import dataclass
from typing import Any

from anthropic import Anthropic
from openai import OpenAI

from .config import SimpleAgentConfig

SUPPORTED_MODEL_PROVIDERS = {"anthropic", "openai"}
DEFAULT_ANTHROPIC_MAX_TOKENS = 1024


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


class ModelBackend:
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
        provider_name, provider_model = self._split_model(model)
        request_options = self._request_options(provider_name=provider_name, provider_options=provider_options)
        if provider_name == "anthropic":
            return self._complete_anthropic(
                model=provider_model, messages=messages, tools=tools, request_options=request_options
            )
        return self._complete_openai(model=provider_model, messages=messages, tools=tools, request_options=request_options)

    def _complete_openai(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        request_options: dict[str, Any],
    ) -> BackendStep:
        timeout = request_options.pop("timeout", self._config.timeout_seconds)
        client = OpenAI(**self._openai_client_options(request_options, timeout))

        create_options: dict[str, Any] = {
            "model": model,
            "messages": self._messages_for_openai(messages),
            "timeout": timeout,
        }
        if tools:
            create_options["tools"] = tools
        create_options.update(request_options)

        response = client.chat.completions.create(**create_options)
        choice = response.choices[0]
        message = choice.message
        tool_calls = self._parse_openai_tool_calls(message.tool_calls or [])
        assistant_message = self._assistant_message_from_openai_message(message)
        return BackendStep(
            assistant_message=assistant_message,
            output_text=self._normalize_content(message.content),
            tool_calls=tool_calls,
        )

    def _complete_anthropic(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        request_options: dict[str, Any],
    ) -> BackendStep:
        timeout = request_options.pop("timeout", self._config.timeout_seconds)
        max_tokens = request_options.pop("max_tokens", request_options.pop("max_completion_tokens", DEFAULT_ANTHROPIC_MAX_TOKENS))
        client = Anthropic(**self._anthropic_client_options(request_options, timeout))
        system_prompt, anthropic_messages = self._messages_for_anthropic(messages)

        create_options: dict[str, Any] = {
            "model": model,
            "messages": anthropic_messages,
            "max_tokens": self._positive_int(max_tokens, field_name="max_tokens"),
            "timeout": timeout,
        }
        if system_prompt:
            create_options["system"] = system_prompt
        anthropic_tools = self._tools_for_anthropic(tools)
        if anthropic_tools:
            create_options["tools"] = anthropic_tools
        create_options.update(request_options)

        response = client.messages.create(**create_options)
        assistant_message, output_text, tool_calls = self._anthropic_response_to_step_parts(response.content)
        return BackendStep(assistant_message=assistant_message, output_text=output_text, tool_calls=tool_calls)

    def _openai_client_options(self, request_options: dict[str, Any], timeout: Any) -> dict[str, Any]:
        client_options: dict[str, Any] = {"timeout": timeout}
        api_key = self._pop_string_option(request_options, "api_key")
        if api_key:
            client_options["api_key"] = api_key
        base_url = self._pop_string_option(request_options, "base_url")
        if base_url:
            client_options["base_url"] = base_url
        return client_options

    def _anthropic_client_options(self, request_options: dict[str, Any], timeout: Any) -> dict[str, Any]:
        client_options: dict[str, Any] = {"timeout": timeout}
        api_key = self._pop_string_option(request_options, "api_key")
        if api_key:
            client_options["api_key"] = api_key
        base_url = self._pop_string_option(request_options, "base_url")
        if base_url:
            normalized_base_url = base_url.rstrip("/")
            if normalized_base_url.endswith("/v1"):
                normalized_base_url = normalized_base_url.removesuffix("/v1")
            client_options["base_url"] = normalized_base_url
        return client_options

    def _assistant_message_from_openai_message(self, message: Any) -> dict[str, Any]:
        assistant_message: dict[str, Any] = {"role": "assistant"}
        content = self._normalize_content(message.content)
        raw_tool_calls = message.tool_calls or []
        if content:
            assistant_message["content"] = content
        elif raw_tool_calls:
            assistant_message["content"] = None
        if raw_tool_calls:
            assistant_message["tool_calls"] = [self._openai_tool_call_message(raw_tool_call) for raw_tool_call in raw_tool_calls]
        return assistant_message

    def _anthropic_response_to_step_parts(
        self, content_blocks: list[Any]
    ) -> tuple[dict[str, Any], str, list[BackendToolCall]]:
        assistant_message: dict[str, Any] = {"role": "assistant"}
        output_text_parts: list[str] = []
        tool_calls: list[BackendToolCall] = []
        tool_call_messages: list[dict[str, Any]] = []

        for raw_block in content_blocks:
            block_type = self._block_type(raw_block)
            if block_type == "text":
                text = self._block_text(raw_block)
                if text:
                    output_text_parts.append(text)
                continue
            if block_type != "tool_use":
                continue

            arguments = self._coerce_tool_arguments(self._block_value(raw_block, "input"), block_id=self._block_text(raw_block, key="id"))
            tool_call_id = self._block_text(raw_block, key="id")
            tool_name = self._block_text(raw_block, key="name")
            tool_calls.append(BackendToolCall(call_id=tool_call_id, tool_id=tool_name, arguments=arguments))
            tool_call_messages.append(
                {
                    "id": tool_call_id,
                    "type": "function",
                    "function": {"name": tool_name, "arguments": json.dumps(arguments, separators=(",", ":"))},
                }
            )

        output_text = "\n".join(part for part in output_text_parts if part)
        if output_text:
            assistant_message["content"] = output_text
        elif tool_call_messages:
            assistant_message["content"] = None
        if tool_call_messages:
            assistant_message["tool_calls"] = tool_call_messages
        return assistant_message, output_text, tool_calls

    def _messages_for_openai(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        translated: list[dict[str, Any]] = []
        for raw_message in messages:
            role = str(raw_message.get("role", "") or "").strip()
            if not role:
                continue
            translated_message: dict[str, Any] = {"role": role}
            if role == "assistant":
                content = self._normalize_content(raw_message.get("content"))
                tool_calls = self._message_tool_calls(raw_message)
                if content:
                    translated_message["content"] = content
                elif tool_calls:
                    translated_message["content"] = None
                if tool_calls:
                    translated_message["tool_calls"] = tool_calls
            elif role == "tool":
                translated_message["content"] = self._normalize_content(raw_message.get("content"))
                tool_call_id = str(raw_message.get("tool_call_id", "") or "").strip()
                if tool_call_id:
                    translated_message["tool_call_id"] = tool_call_id
                name = str(raw_message.get("name", "") or "").strip()
                if name:
                    translated_message["name"] = name
            else:
                translated_message["content"] = self._normalize_content(raw_message.get("content"))
            translated.append(translated_message)
        return translated

    def _messages_for_anthropic(self, messages: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
        system_parts: list[str] = []
        translated: list[dict[str, Any]] = []
        pending_tool_results: list[dict[str, Any]] = []

        def flush_tool_results() -> None:
            if pending_tool_results:
                translated.append({"role": "user", "content": list(pending_tool_results)})
                pending_tool_results.clear()

        for raw_message in messages:
            role = str(raw_message.get("role", "") or "").strip()
            if not role:
                continue

            if role == "system":
                system_text = self._normalize_content(raw_message.get("content"))
                if system_text:
                    system_parts.append(system_text)
                continue

            if role == "tool":
                tool_use_id = str(raw_message.get("tool_call_id", "") or "").strip()
                if not tool_use_id:
                    raise ValueError("tool_result messages require tool_call_id for anthropic models")
                pending_tool_results.append(
                    {
                        "type": "tool_result",
                        "tool_use_id": tool_use_id,
                        "content": self._normalize_content(raw_message.get("content")),
                    }
                )
                continue

            flush_tool_results()

            if role == "assistant":
                assistant_blocks: list[dict[str, Any]] = []
                assistant_text = self._normalize_content(raw_message.get("content"))
                if assistant_text:
                    assistant_blocks.append({"type": "text", "text": assistant_text})
                for tool_call in self._message_tool_calls(raw_message):
                    function = tool_call.get("function") or {}
                    tool_name = str(function.get("name", "") or "").strip()
                    tool_call_id = str(tool_call.get("id", "") or "").strip()
                    raw_arguments = function.get("arguments", "")
                    arguments = self._coerce_tool_arguments(raw_arguments, block_id=tool_call_id)
                    assistant_blocks.append(
                        {"type": "tool_use", "id": tool_call_id, "name": tool_name, "input": arguments}
                    )
                if not assistant_blocks:
                    continue
                if len(assistant_blocks) == 1 and assistant_blocks[0].get("type") == "text":
                    translated.append({"role": "assistant", "content": assistant_blocks[0]["text"]})
                else:
                    translated.append({"role": "assistant", "content": assistant_blocks})
                continue

            translated.append({"role": "user", "content": self._normalize_content(raw_message.get("content"))})

        flush_tool_results()
        return "\n\n".join(part for part in system_parts if part), translated

    def _tools_for_anthropic(self, tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
        anthropic_tools: list[dict[str, Any]] = []
        for tool in tools:
            function = tool.get("function") or {}
            tool_name = str(function.get("name", "") or "").strip()
            if not tool_name:
                continue
            anthropic_tools.append(
                {
                    "name": tool_name,
                    "description": str(function.get("description", "") or "").strip(),
                    "input_schema": function.get("parameters") or {"type": "object", "properties": {}},
                }
            )
        return anthropic_tools

    def _message_tool_calls(self, message: dict[str, Any]) -> list[dict[str, Any]]:
        raw_tool_calls = message.get("tool_calls")
        if isinstance(raw_tool_calls, list):
            return [tool_call for tool_call in raw_tool_calls if isinstance(tool_call, dict)]
        return []

    def _openai_tool_call_message(self, raw_tool_call: Any) -> dict[str, Any]:
        raw_arguments = getattr(raw_tool_call.function, "arguments", "") or "{}"
        return {
            "id": str(getattr(raw_tool_call, "id", "") or "").strip(),
            "type": str(getattr(raw_tool_call, "type", "function") or "function").strip() or "function",
            "function": {
                "name": str(getattr(raw_tool_call.function, "name", "") or "").strip(),
                "arguments": str(raw_arguments),
            },
        }

    def _parse_openai_tool_calls(self, raw_tool_calls: list[Any]) -> list[BackendToolCall]:
        calls: list[BackendToolCall] = []
        for raw_tool_call in raw_tool_calls:
            arguments = self._coerce_tool_arguments(
                getattr(raw_tool_call.function, "arguments", "") or "{}",
                block_id=str(getattr(raw_tool_call, "id", "") or "").strip(),
            )
            calls.append(
                BackendToolCall(
                    call_id=str(getattr(raw_tool_call, "id", "") or "").strip(),
                    tool_id=str(getattr(raw_tool_call.function, "name", "") or "").strip(),
                    arguments=arguments,
                )
            )
        return calls

    def _request_options(self, *, provider_name: str, provider_options: dict[str, Any]) -> dict[str, Any]:
        options = {key: value for key, value in provider_options.items() if not isinstance(value, dict)}
        nested = provider_options.get(provider_name)
        if isinstance(nested, dict):
            options.update(nested)
        return options

    def _split_model(self, model: str) -> tuple[str, str]:
        provider_name, sep, provider_model = model.partition("/")
        provider_name = provider_name.strip()
        provider_model = provider_model.strip()
        if sep == "":
            return "openai", model
        if provider_name not in SUPPORTED_MODEL_PROVIDERS or not provider_model:
            supported = ", ".join(sorted(SUPPORTED_MODEL_PROVIDERS))
            raise ValueError(f"unsupported model {model!r}; expected one of {supported} or an unprefixed OpenAI model")
        return provider_name, provider_model

    def _coerce_tool_arguments(self, raw_value: Any, *, block_id: str) -> dict[str, Any]:
        if isinstance(raw_value, dict):
            return raw_value
        try:
            parsed = json.loads(str(raw_value or "{}"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"tool call {block_id!r} returned non-JSON arguments: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ValueError(f"tool call {block_id!r} arguments must decode to an object")
        return parsed

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

    def _positive_int(self, raw_value: Any, *, field_name: str) -> int:
        value = int(raw_value)
        if value <= 0:
            raise ValueError(f"{field_name} must be positive")
        return value

    def _pop_string_option(self, options: dict[str, Any], key: str) -> str:
        return str(options.pop(key, "") or "").strip()

    def _block_type(self, block: Any) -> str:
        if isinstance(block, dict):
            return str(block.get("type", "") or "").strip()
        return str(getattr(block, "type", "") or "").strip()

    def _block_text(self, block: Any, *, key: str = "text") -> str:
        if isinstance(block, dict):
            return str(block.get(key, "") or "").strip()
        return str(getattr(block, key, "") or "").strip()

    def _block_value(self, block: Any, key: str) -> Any:
        if isinstance(block, dict):
            return block.get(key)
        return getattr(block, key, None)
