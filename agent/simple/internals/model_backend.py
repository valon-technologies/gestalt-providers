import json
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

import gestalt
from gestalt import telemetry
from anthropic import Anthropic
from openai import OpenAI

from .config import SimpleAgentConfig

DEFAULT_ANTHROPIC_MAX_TOKENS = 1024
OPENAI_RESPONSES_OPTION_KEYS = frozenset(
    {
        "include",
        "max_output_tokens",
        "max_tool_calls",
        "metadata",
        "parallel_tool_calls",
        "prompt_cache_key",
        "reasoning",
        "safety_identifier",
        "service_tier",
        "store",
        "stream_options",
        "temperature",
        "text",
        "tool_choice",
        "top_logprobs",
        "top_p",
        "truncation",
        "user",
    }
)


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


@dataclass(frozen=True, slots=True)
class ModelRoute:
    backend_name: str
    request_model: str
    option_provider_names: tuple[str, ...]


class ModelBackend:
    def __init__(self, config: SimpleAgentConfig) -> None:
        self._config = config

    def complete(
        self,
        *,
        model: str,
        session_id: str,
        turn_id: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        model_options: dict[str, Any],
        run_grant: str,
        disable_tools: bool = False,
    ) -> BackendStep:
        route = self._resolve_model(model)
        request_options = self._request_options(
            option_provider_names=route.option_provider_names, model_options=model_options
        )
        self._apply_model_connection_options(
            request_options=request_options, session_id=session_id, turn_id=turn_id, run_grant=run_grant
        )
        if disable_tools:
            request_options = self._request_options_without_tools(request_options)
        if route.backend_name == "openai" and self._should_use_openai_responses(
            model=route.request_model, tools=tools, request_options=request_options
        ):
            return self._complete_openai_responses(
                model=route.request_model, messages=messages, tools=tools, request_options=request_options
            )
        if route.backend_name == "anthropic":
            return self._complete_anthropic(
                model=route.request_model, messages=messages, tools=tools, request_options=request_options
            )
        return self._complete_openai(
            model=route.request_model, messages=messages, tools=tools, request_options=request_options
        )

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

        with telemetry.model_operation(
            provider_name="openai", request_model=model, request_options=create_options
        ) as operation:
            response = client.chat.completions.create(**create_options)
            operation.set_response_metadata(response, finish_reasons=_openai_finish_reasons(response))
            telemetry.record_openai_usage(operation, response)
        choice = response.choices[0]
        message = choice.message
        tool_calls = self._parse_openai_tool_calls(message.tool_calls or [])
        assistant_message = self._assistant_message_from_openai_message(message)
        return BackendStep(
            assistant_message=assistant_message,
            output_text=self._normalize_content(message.content),
            tool_calls=tool_calls,
        )

    def _complete_openai_responses(
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
            "input": self._messages_for_openai_responses(messages),
            "timeout": timeout,
        }
        response_tools = self._tools_for_openai_responses(tools)
        if response_tools:
            create_options["tools"] = response_tools
        create_options.update(self._request_options_for_openai_responses(request_options))

        with telemetry.model_operation(
            provider_name="openai", request_model=model, request_options=create_options
        ) as operation:
            response = client.responses.create(**create_options)
            operation.set_response_metadata(response)
            telemetry.record_openai_usage(operation, response)
        assistant_message, output_text, tool_calls = self._openai_response_to_step_parts(response)
        return BackendStep(assistant_message=assistant_message, output_text=output_text, tool_calls=tool_calls)

    def _complete_anthropic(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
        request_options: dict[str, Any],
    ) -> BackendStep:
        timeout = request_options.pop("timeout", self._config.timeout_seconds)
        max_tokens = request_options.pop(
            "max_tokens", request_options.pop("max_completion_tokens", DEFAULT_ANTHROPIC_MAX_TOKENS)
        )
        client = Anthropic(**self._anthropic_client_options(request_options, timeout))
        system_prompt, anthropic_messages = self._messages_for_anthropic(messages)

        positive_max_tokens = self._positive_int(max_tokens, field_name="max_tokens")
        create_options: dict[str, Any] = {
            "model": model,
            "messages": anthropic_messages,
            "max_tokens": positive_max_tokens,
            "timeout": timeout,
        }
        if system_prompt:
            create_options["system"] = system_prompt
        anthropic_tools = self._tools_for_anthropic(tools)
        if anthropic_tools:
            create_options["tools"] = anthropic_tools
        create_options.update(request_options)

        with telemetry.model_operation(
            provider_name="anthropic",
            request_model=model,
            request_options=create_options,
            request_attrs={"gen_ai.request.max_tokens": positive_max_tokens},
        ) as operation:
            response = client.messages.create(**create_options)
            operation.set_response_metadata(response, finish_reasons=_anthropic_finish_reasons(response))
            telemetry.record_anthropic_usage(operation, response)
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
        default_headers = self._pop_dict_option(request_options, "default_headers")
        if default_headers:
            client_options["default_headers"] = default_headers
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
        default_headers = self._pop_dict_option(request_options, "default_headers")
        if default_headers:
            client_options["default_headers"] = default_headers
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
            assistant_message["tool_calls"] = [
                self._openai_tool_call_message(raw_tool_call) for raw_tool_call in raw_tool_calls
            ]
        return assistant_message

    def _anthropic_response_to_step_parts(
        self, content_blocks: list[Any]
    ) -> tuple[dict[str, Any], str, list[BackendToolCall]]:
        assistant_message: dict[str, Any] = {"role": "assistant"}
        output_text_parts: list[str] = []
        tool_calls: list[BackendToolCall] = []
        tool_call_messages: list[dict[str, Any]] = []
        anthropic_content_blocks: list[dict[str, Any]] = []

        for raw_block in content_blocks:
            block_type = self._block_type(raw_block)
            anthropic_block = self._anthropic_content_block(raw_block)
            if anthropic_block:
                anthropic_content_blocks.append(anthropic_block)

            if block_type == "text":
                text = self._block_text(raw_block)
                if text:
                    output_text_parts.append(text)
                continue
            if block_type != "tool_use":
                continue

            arguments = self._coerce_tool_arguments(
                self._block_value(raw_block, "input"), block_id=self._block_text(raw_block, key="id")
            )
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

        if anthropic_content_blocks:
            assistant_message["anthropic_content"] = anthropic_content_blocks
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
            else:
                translated_message["content"] = self._normalize_content(raw_message.get("content"))
            translated.append(translated_message)
        return translated

    def _messages_for_openai_responses(self, messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
        translated: list[dict[str, Any]] = []
        for raw_message in messages:
            role = str(raw_message.get("role", "") or "").strip()
            if not role:
                continue

            if role == "tool":
                tool_call_id = str(raw_message.get("tool_call_id", "") or "").strip()
                if tool_call_id:
                    translated.append(
                        {
                            "type": "function_call_output",
                            "call_id": tool_call_id,
                            "output": self._normalize_content(raw_message.get("content")),
                        }
                    )
                continue

            if role not in {"assistant", "developer", "system", "user"}:
                continue

            if role == "assistant":
                response_output = self._message_openai_response_output(raw_message)
                if response_output:
                    translated.extend(response_output)
                    continue

            content = self._normalize_content(raw_message.get("content"))
            if content:
                translated.append({"type": "message", "role": role, "content": content})

            if role != "assistant":
                continue

            for tool_call in self._message_tool_calls(raw_message):
                function = tool_call.get("function") or {}
                tool_name = str(function.get("name", "") or "").strip()
                tool_call_id = str(tool_call.get("id", "") or "").strip()
                if not tool_name or not tool_call_id:
                    continue
                translated.append(
                    {
                        "type": "function_call",
                        "id": tool_call_id,
                        "call_id": tool_call_id,
                        "name": tool_name,
                        "arguments": self._encoded_tool_arguments(function.get("arguments", "{}")),
                    }
                )
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
                tool_result: dict[str, Any] = {
                    "type": "tool_result",
                    "tool_use_id": tool_use_id,
                    "content": self._normalize_content(raw_message.get("content")),
                }
                if raw_message.get("is_error"):
                    tool_result["is_error"] = True
                pending_tool_results.append(tool_result)
                continue

            flush_tool_results()

            if role == "assistant":
                anthropic_content = self._message_anthropic_content(raw_message)
                if anthropic_content:
                    translated.append({"role": "assistant", "content": anthropic_content})
                    continue

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

    def _tools_for_openai_responses(self, tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
        response_tools: list[dict[str, Any]] = []
        for tool in tools:
            function = tool.get("function") or {}
            tool_name = str(function.get("name", "") or "").strip()
            if not tool_name:
                continue
            response_tools.append(
                {
                    "type": "function",
                    "name": tool_name,
                    "description": str(function.get("description", "") or "").strip(),
                    "parameters": function.get("parameters") or {"type": "object", "properties": {}},
                    "strict": bool(function.get("strict", False)),
                }
            )
        return response_tools

    def _openai_response_to_step_parts(self, response: Any) -> tuple[dict[str, Any], str, list[BackendToolCall]]:
        assistant_message: dict[str, Any] = {"role": "assistant"}
        output_text = self._normalize_content(getattr(response, "output_text", ""))
        tool_calls: list[BackendToolCall] = []
        tool_call_messages: list[dict[str, Any]] = []
        response_output: list[dict[str, Any]] = []

        for item in getattr(response, "output", []) or []:
            output_item = self._openai_response_output_item(item)
            if output_item:
                response_output.append(output_item)
            if str(output_item.get("type", "") or "") != "function_call":
                continue
            call_id = str(output_item.get("call_id", "") or output_item.get("id", "") or "").strip()
            tool_name = str(output_item.get("name", "") or "").strip()
            if not call_id or not tool_name:
                continue
            arguments = self._coerce_tool_arguments(output_item.get("arguments", "") or "{}", block_id=call_id)
            encoded_arguments = json.dumps(arguments, separators=(",", ":"))
            tool_calls.append(BackendToolCall(call_id=call_id, tool_id=tool_name, arguments=arguments))
            tool_call_messages.append(
                {"id": call_id, "type": "function", "function": {"name": tool_name, "arguments": encoded_arguments}}
            )

        if response_output:
            assistant_message["openai_response_output"] = response_output
        if output_text:
            assistant_message["content"] = output_text
        elif tool_call_messages:
            assistant_message["content"] = None
        if tool_call_messages:
            assistant_message["tool_calls"] = tool_call_messages
        return assistant_message, output_text, tool_calls

    def _openai_response_output_item(self, item: Any) -> dict[str, Any]:
        if isinstance(item, dict):
            return deepcopy(item)
        if hasattr(item, "model_dump"):
            dumped = item.model_dump(mode="json", exclude_none=True)
            if isinstance(dumped, dict):
                return dumped
        return {}

    def _message_tool_calls(self, message: dict[str, Any]) -> list[dict[str, Any]]:
        raw_tool_calls = message.get("tool_calls")
        if isinstance(raw_tool_calls, list):
            return [tool_call for tool_call in raw_tool_calls if isinstance(tool_call, dict)]
        return []

    def _message_openai_response_output(self, message: dict[str, Any]) -> list[dict[str, Any]]:
        raw_output = message.get("openai_response_output")
        if not isinstance(raw_output, list):
            return []
        return [deepcopy(item) for item in raw_output if isinstance(item, dict)]

    def _message_anthropic_content(self, message: dict[str, Any]) -> list[dict[str, Any]]:
        raw_content = message.get("anthropic_content")
        if not isinstance(raw_content, list):
            return []
        return [deepcopy(block) for block in raw_content if isinstance(block, dict)]

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

    def _request_options(
        self, *, option_provider_names: tuple[str, ...], model_options: dict[str, Any]
    ) -> dict[str, Any]:
        options: dict[str, Any] = {}
        self._merge_request_options(options, self._config.model_options, option_provider_names=option_provider_names)
        self._merge_request_options(options, model_options, option_provider_names=option_provider_names)
        return options

    def _request_options_without_tools(self, options: dict[str, Any]) -> dict[str, Any]:
        filtered = deepcopy(options)
        for key in ("tools", "tool_choice", "parallel_tool_calls"):
            filtered.pop(key, None)
        return filtered

    def _request_options_for_openai_responses(self, request_options: dict[str, Any]) -> dict[str, Any]:
        options = deepcopy(request_options)
        reasoning_effort = options.pop("reasoning_effort", None)
        if reasoning_effort is not None:
            reasoning = options.get("reasoning")
            if isinstance(reasoning, dict):
                merged_reasoning = deepcopy(reasoning)
                merged_reasoning.setdefault("effort", reasoning_effort)
                options["reasoning"] = merged_reasoning
            else:
                options["reasoning"] = {"effort": reasoning_effort}
        max_completion_tokens = options.pop("max_completion_tokens", None)
        if max_completion_tokens is not None and "max_output_tokens" not in options:
            options["max_output_tokens"] = max_completion_tokens
        if "tool_choice" in options:
            options["tool_choice"] = self._tool_choice_for_openai_responses(options["tool_choice"])
        return {key: value for key, value in options.items() if key in OPENAI_RESPONSES_OPTION_KEYS}

    def _tool_choice_for_openai_responses(self, raw_value: Any) -> Any:
        if not isinstance(raw_value, dict):
            return raw_value
        if str(raw_value.get("type", "") or "") != "function":
            return raw_value
        function = raw_value.get("function")
        if not isinstance(function, dict):
            return raw_value
        name = str(function.get("name", "") or "").strip()
        if not name:
            return raw_value
        translated = deepcopy(raw_value)
        translated.pop("function", None)
        translated["name"] = name
        return translated

    def _should_use_openai_responses(
        self, *, model: str, tools: list[dict[str, Any]], request_options: dict[str, Any]
    ) -> bool:
        if not model.lower().startswith("gpt-5"):
            return False
        return bool(tools) or "reasoning_effort" in request_options or "reasoning" in request_options

    def _merge_request_options(
        self, options: dict[str, Any], model_options: dict[str, Any], *, option_provider_names: tuple[str, ...]
    ) -> None:
        for key, value in model_options.items():
            if not isinstance(value, dict):
                options[key] = deepcopy(value)
        for provider_name in option_provider_names:
            nested = model_options.get(provider_name)
            if isinstance(nested, dict):
                options.update(deepcopy(nested))

    def _apply_model_connection_options(
        self, *, request_options: dict[str, Any], session_id: str, turn_id: str, run_grant: str
    ) -> None:
        connection = self._pop_string_option(request_options, "connection")
        if not connection:
            return
        instance = self._pop_string_option(request_options, "connectionInstance")
        if not instance:
            instance = self._pop_string_option(request_options, "connection_instance")
        with gestalt.AgentHost() as host:
            resolved = host.resolve_connection(
                gestalt.ResolveAgentConnectionRequest(
                    session_id=session_id,
                    turn_id=turn_id,
                    connection=connection,
                    instance=instance,
                    run_grant=run_grant,
                )
            )
        self._merge_connection_params(request_options, dict(getattr(resolved, "params", {}) or {}))
        self._merge_connection_headers(request_options, dict(getattr(resolved, "headers", {}) or {}))

    def _merge_connection_params(self, request_options: dict[str, Any], params: dict[str, str]) -> None:
        normalized: dict[str, Any] = {}
        for key, value in params.items():
            clean_key = str(key or "").strip()
            if not clean_key:
                continue
            option_key = {"baseUrl": "base_url", "apiKey": "api_key"}.get(clean_key, clean_key)
            normalized[option_key] = str(value or "")
        for key, value in normalized.items():
            request_options.setdefault(key, value)

    def _merge_connection_headers(self, request_options: dict[str, Any], headers: dict[str, str]) -> None:
        clean_headers = {str(key): str(value) for key, value in headers.items() if str(key or "").strip()}
        if not clean_headers:
            return
        existing = request_options.get("default_headers")
        merged_headers = dict(clean_headers)
        if isinstance(existing, dict):
            merged_headers.update({str(key): str(value) for key, value in existing.items()})
        request_options["default_headers"] = merged_headers
        token = self._bearer_token_from_headers(clean_headers)
        if token:
            request_options.setdefault("api_key", token)

    def _bearer_token_from_headers(self, headers: dict[str, str]) -> str:
        for key, value in headers.items():
            if key.lower() != "authorization":
                continue
            scheme, _, token = str(value or "").strip().partition(" ")
            if scheme.lower() == "bearer" and token.strip():
                return token.strip()
        return ""

    def _resolve_model(self, model: str) -> ModelRoute:
        raw_model = model.strip()
        if not raw_model:
            raise ValueError("model is required")

        provider_name, sep, provider_model = raw_model.partition("/")
        provider_name = provider_name.strip()
        provider_model = provider_model.strip()
        if sep == "":
            return ModelRoute(backend_name="openai", request_model=raw_model, option_provider_names=("openai",))
        if not provider_name:
            raise ValueError(f"model {model!r} must include a provider name before '/'")
        if not provider_model:
            raise ValueError(f"model {model!r} must include a model name after provider prefix")
        if provider_name == "anthropic":
            return ModelRoute(
                backend_name="anthropic", request_model=provider_model, option_provider_names=("anthropic",)
            )
        if provider_name == "openai":
            return ModelRoute(backend_name="openai", request_model=provider_model, option_provider_names=("openai",))
        return ModelRoute(
            backend_name="openai", request_model=raw_model, option_provider_names=("openai", provider_name)
        )

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

    def _encoded_tool_arguments(self, raw_value: Any) -> str:
        if isinstance(raw_value, str):
            return raw_value
        if isinstance(raw_value, dict):
            return json.dumps(raw_value, separators=(",", ":"))
        return str(raw_value or "{}")

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

    def _pop_dict_option(self, options: dict[str, Any], key: str) -> dict[str, str]:
        raw_value = options.pop(key, None)
        if not isinstance(raw_value, dict):
            return {}
        return {str(raw_key): str(raw_item) for raw_key, raw_item in raw_value.items()}

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

    def _anthropic_content_block(self, block: Any) -> dict[str, Any]:
        if isinstance(block, dict):
            return deepcopy(block)

        model_dump = getattr(block, "model_dump", None)
        if callable(model_dump):
            try:
                dumped = model_dump(exclude_none=True)
            except TypeError:
                dumped = model_dump()
            if isinstance(dumped, dict):
                return dumped

        block_type = self._block_type(block)
        if not block_type:
            return {}
        content_block: dict[str, Any] = {"type": block_type}
        for key in ("text", "id", "name", "thinking", "signature", "data"):
            value = self._block_value(block, key)
            if value not in (None, ""):
                content_block[key] = value
        if block_type == "tool_use":
            content_block["input"] = self._block_value(block, "input") or {}
        return content_block


def _openai_finish_reasons(response: Any) -> list[str]:
    reasons: list[str] = []
    for choice in getattr(response, "choices", []) or []:
        reason = str(getattr(choice, "finish_reason", "") or "").strip()
        if reason:
            reasons.append(reason)
    return reasons


def _anthropic_finish_reasons(response: Any) -> list[str]:
    reason = str(getattr(response, "stop_reason", "") or "").strip()
    return [reason] if reason else []
