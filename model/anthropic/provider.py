from __future__ import annotations

import os
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any

import gestalt

DEFAULT_API_KEY_ENV = "ANTHROPIC_API_KEY"
DEFAULT_MAX_TOKENS = 1024
STRUCTURED_OUTPUT_TOOL_NAME = "gestalt_structured_output"

Anthropic: Any | None = None


@dataclass(slots=True)
class AnthropicModelConfig:
    name: str = "anthropic"
    api_key_env: str = DEFAULT_API_KEY_ENV
    api_key: str = ""
    default_model: str = ""
    max_tokens: int = DEFAULT_MAX_TOKENS
    temperature: float | None = None
    timeout_seconds: float = 300.0
    max_retries: int = 0

    @classmethod
    def from_dict(cls, *, name: str, raw_config: dict[str, Any]) -> "AnthropicModelConfig":
        return cls(
            name=name.strip() or "anthropic",
            api_key_env=_config_string(raw_config, "api_key_env", "apiKeyEnv") or DEFAULT_API_KEY_ENV,
            api_key=_config_string(raw_config, "api_key", "apiKey", "anthropicApiKey"),
            default_model=_config_string(raw_config, "default_model", "defaultModel"),
            max_tokens=_config_positive_int(
                raw_config, "max_tokens", "maxTokens", default=DEFAULT_MAX_TOKENS, field_name="max_tokens"
            ),
            temperature=_config_optional_temperature(raw_config, "temperature", field_name="temperature"),
            timeout_seconds=_config_positive_float(
                raw_config, "timeout_seconds", "timeoutSeconds", default=300.0, field_name="timeout_seconds"
            ),
            max_retries=_config_nonnegative_int(
                raw_config, "max_retries", "maxRetries", default=0, field_name="max_retries"
            ),
        )

    def resolve_model(self, requested_model: str) -> str:
        model = requested_model.strip() or self.default_model
        if not model:
            raise gestalt.Error(HTTPStatus.BAD_REQUEST, "model is required when config.defaultModel is not set")
        return model

    def resolve_api_key(self) -> str:
        return self.api_key or os.environ.get(self.api_key_env, "").strip()


class AnthropicModelProvider(gestalt.ModelProvider, gestalt.MetadataProvider, gestalt.WarningsProvider):
    def __init__(self) -> None:
        self._name = "anthropic"
        self._config = AnthropicModelConfig()

    def configure(self, name: str, config: dict[str, Any]) -> None:
        self._name = name.strip() or "anthropic"
        try:
            self._config = AnthropicModelConfig.from_dict(name=self._name, raw_config=config)
        except (TypeError, ValueError) as exc:
            raise gestalt.Error(HTTPStatus.BAD_REQUEST, f"invalid Anthropic model provider config: {exc}") from exc

    def metadata(self) -> gestalt.ProviderMetadata:
        return gestalt.ProviderMetadata(
            kind=gestalt.ProviderKind.MODEL,
            name=self._name,
            display_name="Anthropic",
            description="Stateless Anthropic Messages API model provider.",
            version="0.0.1-alpha.1",
        )

    def warnings(self) -> list[str]:
        if self._config.resolve_api_key():
            return []
        return [f"set config.apiKey or {self._config.api_key_env} before running live model requests"]

    def get_capabilities(
        self, request: gestalt.GetModelProviderCapabilitiesRequest
    ) -> gestalt.ModelProviderCapabilities:
        return gestalt.ModelProviderCapabilities(
            text_output=True, structured_output=True, usage=True, parallel_requests=True
        )

    def generate(self, request: gestalt.GenerateModelRequest) -> gestalt.GenerateModelResponse:
        config = self._config
        model = config.resolve_model(str(request.model or ""))
        api_key = config.resolve_api_key()
        if not api_key:
            raise gestalt.Error(
                HTTPStatus.PRECONDITION_FAILED,
                f"set config.apiKey or {config.api_key_env} before running live model requests",
            )

        model_options = _json_object(request.model_options, field_name="model_options") or {}
        response_schema = _response_schema(request.response_schema)
        messages, system = _anthropic_messages(request.messages)
        kwargs: dict[str, Any] = {
            "model": model,
            "max_tokens": _request_max_tokens(model_options, config.max_tokens),
            "messages": messages,
        }
        if system:
            kwargs["system"] = system
        temperature = _request_temperature(model_options, config.temperature)
        if temperature is not None:
            kwargs["temperature"] = temperature
        if response_schema is not None:
            kwargs["tools"] = [
                {
                    "name": STRUCTURED_OUTPUT_TOOL_NAME,
                    "description": "Return the response as structured JSON matching the requested schema.",
                    "strict": True,
                    "input_schema": response_schema,
                }
            ]
            kwargs["tool_choice"] = {
                "type": "tool",
                "name": STRUCTURED_OUTPUT_TOOL_NAME,
                "disable_parallel_tool_use": True,
            }

        try:
            response = _anthropic_client(api_key, config).messages.create(**kwargs)
        except Exception as exc:
            raise _anthropic_error(exc) from exc

        output_text = _response_text(response)
        structured_output = _structured_output(response) if response_schema is not None else None
        message = gestalt.ModelMessage(role="assistant", text=output_text) if output_text else None
        return gestalt.GenerateModelResponse(
            output_text=output_text,
            structured_output=structured_output,
            message=message,
            usage=_usage(response),
            finish_reason=str(getattr(response, "stop_reason", "") or ""),
            provider_metadata={
                "id": str(getattr(response, "id", "") or ""),
                "model": str(getattr(response, "model", "") or ""),
            },
        )


def _anthropic_client(api_key: str, config: AnthropicModelConfig) -> Any:
    global Anthropic
    if Anthropic is None:
        from anthropic import Anthropic as imported_anthropic

        Anthropic = imported_anthropic
    return Anthropic(api_key=api_key, max_retries=config.max_retries, timeout=config.timeout_seconds)


def _anthropic_messages(messages: Any) -> tuple[list[dict[str, Any]], str]:
    out: list[dict[str, Any]] = []
    system_parts: list[str] = []
    for raw_message in messages or []:
        role = str(getattr(raw_message, "role", "") or "").strip().lower()
        if role not in {"system", "user", "assistant"}:
            raise gestalt.Error(HTTPStatus.BAD_REQUEST, f"unsupported message role {role!r}")
        text_parts = _message_text_parts(raw_message)
        if role == "system":
            system_parts.extend(part for part in text_parts if part)
            continue
        out.append({"role": role, "content": [{"type": "text", "text": part} for part in text_parts]})
    if not out:
        raise gestalt.Error(HTTPStatus.BAD_REQUEST, "at least one user or assistant message is required")
    return out, "\n\n".join(system_parts)


def _message_text_parts(message: Any) -> list[str]:
    parts: list[str] = []
    text = str(getattr(message, "text", "") or "")
    if text:
        parts.append(text)
    for raw_part in getattr(message, "parts", []) or []:
        part_type = _message_part_type(raw_part)
        if part_type not in {"", "text", "1", "model_message_part_type_text"}:
            raise gestalt.Error(HTTPStatus.BAD_REQUEST, f"unsupported message part type {part_type!r}")
        parts.append(str(getattr(raw_part, "text", "") or ""))
    return parts or [""]


def _message_part_type(part: Any) -> str:
    value = getattr(part, "type", "")
    if isinstance(value, int):
        return "" if value == 0 else str(value)
    return str(value or "").strip().lower()


def _response_text(response: Any) -> str:
    text_parts: list[str] = []
    for block in getattr(response, "content", []) or []:
        if str(getattr(block, "type", "") or "") == "text":
            text_parts.append(str(getattr(block, "text", "") or ""))
    return "".join(text_parts)


def _structured_output(response: Any) -> dict[str, Any]:
    matching_blocks: list[Any] = []
    for block in getattr(response, "content", []) or []:
        if str(getattr(block, "type", "") or "") != "tool_use":
            continue
        if str(getattr(block, "name", "") or "") != STRUCTURED_OUTPUT_TOOL_NAME:
            continue
        matching_blocks.append(block)
    if not matching_blocks:
        raise gestalt.Error(HTTPStatus.BAD_GATEWAY, "Anthropic response did not include structured output tool use")
    if len(matching_blocks) > 1:
        raise gestalt.Error(HTTPStatus.BAD_GATEWAY, "Anthropic response included multiple structured output tool uses")
    value = getattr(matching_blocks[0], "input", None)
    if isinstance(value, dict):
        return dict(value)
    raise gestalt.Error(HTTPStatus.BAD_GATEWAY, "Anthropic structured output tool input was not an object")


def _usage(response: Any) -> gestalt.ModelUsage | None:
    usage = getattr(response, "usage", None)
    if usage is None:
        return None
    input_tokens = int(getattr(usage, "input_tokens", 0) or 0)
    output_tokens = int(getattr(usage, "output_tokens", 0) or 0)
    return gestalt.ModelUsage(
        input_tokens=input_tokens, output_tokens=output_tokens, total_tokens=input_tokens + output_tokens
    )


def _request_max_tokens(model_options: dict[str, Any], default: int) -> int:
    raw_value = _first_present(
        model_options,
        "max_tokens",
        "maxTokens",
        "max_output_tokens",
        "maxOutputTokens",
        "max_completion_tokens",
        "maxCompletionTokens",
    )
    if raw_value is None or str(raw_value).strip() == "":
        return default
    return _positive_int(raw_value, field_name="model_options.max_tokens")


def _request_temperature(model_options: dict[str, Any], default: float | None) -> float | None:
    raw_value = _first_present(model_options, "temperature")
    if raw_value is None or str(raw_value).strip() == "":
        return default
    return _temperature(raw_value, field_name="model_options.temperature")


def _anthropic_error(exc: Exception) -> gestalt.Error:
    status = getattr(exc, "status_code", None)
    if isinstance(status, int) and 400 <= status < 500:
        return gestalt.Error(status, str(exc))
    return gestalt.Error(HTTPStatus.BAD_GATEWAY, str(exc))


def _json_object(value: Any, *, field_name: str) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "items"):
        return dict(value)
    raise gestalt.Error(HTTPStatus.BAD_REQUEST, f"{field_name} must be an object")


def _response_schema(value: Any) -> dict[str, Any] | None:
    schema = _json_object(value, field_name="response_schema")
    if schema is None:
        return None
    if str(schema.get("type", "")).strip() != "object":
        raise gestalt.Error(HTTPStatus.BAD_REQUEST, "response_schema.type must be object")
    return schema


def _config_string(config: dict[str, Any], *keys: str) -> str:
    value = _first_present(config, *keys)
    return str(value or "").strip()


def _config_positive_int(config: dict[str, Any], *keys: str, default: int, field_name: str) -> int:
    value = _first_present(config, *keys)
    if value is None or str(value).strip() == "":
        return default
    return _positive_int(value, field_name=field_name)


def _config_optional_temperature(config: dict[str, Any], *keys: str, field_name: str) -> float | None:
    value = _first_present(config, *keys)
    if value is None or str(value).strip() == "":
        return None
    return _temperature(value, field_name=field_name)


def _positive_int(value: Any, *, field_name: str) -> int:
    out = int(value)
    if out <= 0:
        raise ValueError(f"{field_name} must be positive")
    return out


def _config_nonnegative_int(config: dict[str, Any], *keys: str, default: int, field_name: str) -> int:
    value = _first_present(config, *keys)
    if value is None or str(value).strip() == "":
        return default
    out = int(value)
    if out < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return out


def _config_positive_float(config: dict[str, Any], *keys: str, default: float, field_name: str) -> float:
    value = _first_present(config, *keys)
    if value is None or str(value).strip() == "":
        return default
    out = float(value)
    if out <= 0:
        raise ValueError(f"{field_name} must be positive")
    return out


def _temperature(value: Any, *, field_name: str) -> float:
    out = float(value)
    if out < 0 or out > 1:
        raise ValueError(f"{field_name} must be between 0 and 1")
    return out


def _first_present(config: dict[str, Any], *keys: str) -> Any | None:
    for key in keys:
        if key in config:
            return config[key]
    return None


provider = AnthropicModelProvider()
