from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from typing import Any

import gestalt

import provider as provider_module


class FakeMessages:
    def __init__(self, response: Any) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        return self.response


class FakeAnthropic:
    instances: list["FakeAnthropic"] = []
    response: Any = None

    def __init__(self, *, api_key: str, max_retries: int, timeout: float) -> None:
        self.api_key = api_key
        self.max_retries = max_retries
        self.timeout = timeout
        self.messages = FakeMessages(self.response)
        self.instances.append(self)


class AnthropicModelProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        FakeAnthropic.instances = []
        provider_module.Anthropic = FakeAnthropic
        self._previous_key = os.environ.get("TEST_ANTHROPIC_API_KEY")
        os.environ["TEST_ANTHROPIC_API_KEY"] = "test-key"

    def tearDown(self) -> None:
        if self._previous_key is None:
            os.environ.pop("TEST_ANTHROPIC_API_KEY", None)
        else:
            os.environ["TEST_ANTHROPIC_API_KEY"] = self._previous_key
        provider_module.Anthropic = None

    def test_generate_text_uses_messages_api_contract(self) -> None:
        FakeAnthropic.response = _message_response(
            content=[SimpleNamespace(type="text", text="Hello"), SimpleNamespace(type="text", text=" world")],
            stop_reason="end_turn",
            input_tokens=4,
            output_tokens=2,
            model="claude-test",
        )
        provider = _provider()

        result = provider.generate(
            gestalt.GenerateModelRequest(
                model="claude-request",
                messages=[
                    gestalt.ModelMessage(role="system", text="Be terse."),
                    gestalt.ModelMessage(role="user", parts=[gestalt.ModelMessagePart(type="text", text="Say hello.")]),
                ],
                model_options={"temperature": 0.7, "max_tokens": 64},
            )
        )

        self.assertEqual(FakeAnthropic.instances[-1].api_key, "test-key")
        self.assertEqual(FakeAnthropic.instances[-1].max_retries, 0)
        self.assertEqual(FakeAnthropic.instances[-1].timeout, 300.0)
        self.assertEqual(
            FakeAnthropic.instances[-1].messages.calls,
            [
                {
                    "model": "claude-request",
                    "max_tokens": 64,
                    "messages": [{"role": "user", "content": [{"type": "text", "text": "Say hello."}]}],
                    "system": "Be terse.",
                    "temperature": 0.7,
                }
            ],
        )
        self.assertEqual(result.output_text, "Hello world")
        self.assertEqual(result.message.text, "Hello world")
        self.assertEqual(result.message.parts, [])
        self.assertEqual(result.finish_reason, "end_turn")
        self.assertEqual(result.usage.input_tokens, 4)
        self.assertEqual(result.usage.output_tokens, 2)
        self.assertEqual(result.usage.total_tokens, 6)

    def test_generate_structured_output_requires_single_tool(self) -> None:
        schema = {
            "type": "object",
            "additionalProperties": False,
            "properties": {"answer": {"type": "integer"}},
            "required": ["answer"],
        }
        FakeAnthropic.response = _message_response(
            content=[
                SimpleNamespace(type="tool_use", name=provider_module.STRUCTURED_OUTPUT_TOOL_NAME, input={"answer": 42})
            ],
            stop_reason="tool_use",
            input_tokens=10,
            output_tokens=5,
        )
        provider = _provider(config={"maxTokens": 512, "temperature": 0.1})

        result = provider.generate(
            gestalt.GenerateModelRequest(
                messages=[gestalt.ModelMessage(role="user", text="Return the answer.")], response_schema=schema
            )
        )

        call = FakeAnthropic.instances[-1].messages.calls[0]
        self.assertEqual(call["model"], "claude-default")
        self.assertEqual(call["max_tokens"], 512)
        self.assertEqual(call["temperature"], 0.1)
        self.assertEqual(
            call["tools"],
            [
                {
                    "name": provider_module.STRUCTURED_OUTPUT_TOOL_NAME,
                    "description": "Return the response as structured JSON matching the requested schema.",
                    "strict": True,
                    "input_schema": schema,
                }
            ],
        )
        self.assertEqual(
            call["tool_choice"],
            {
                "type": "tool",
                "name": provider_module.STRUCTURED_OUTPUT_TOOL_NAME,
                "disable_parallel_tool_use": True,
            },
        )
        self.assertEqual(result.structured_output, {"answer": 42})
        self.assertIsNone(result.message)
        self.assertEqual(result.finish_reason, "tool_use")

    def test_generate_accepts_common_max_token_aliases(self) -> None:
        FakeAnthropic.response = _message_response(
            content=[SimpleNamespace(type="text", text="Hello")],
            stop_reason="end_turn",
            input_tokens=4,
            output_tokens=2,
        )
        provider = _provider()

        provider.generate(
            gestalt.GenerateModelRequest(
                messages=[gestalt.ModelMessage(role="user", text="Say hello.")],
                model_options={"max_output_tokens": 96},
            )
        )

        self.assertEqual(FakeAnthropic.instances[-1].messages.calls[0]["max_tokens"], 96)

    def test_structured_output_rejects_multiple_matching_tool_uses(self) -> None:
        FakeAnthropic.response = _message_response(
            content=[
                SimpleNamespace(type="tool_use", name=provider_module.STRUCTURED_OUTPUT_TOOL_NAME, input={"answer": 42}),
                SimpleNamespace(type="tool_use", name=provider_module.STRUCTURED_OUTPUT_TOOL_NAME, input={"answer": 43}),
            ],
            stop_reason="tool_use",
            input_tokens=10,
            output_tokens=5,
        )
        provider = _provider()

        with self.assertRaises(gestalt.Error) as raised:
            provider.generate(
                gestalt.GenerateModelRequest(
                    messages=[gestalt.ModelMessage(role="user", text="Return the answer.")],
                    response_schema={"type": "object", "properties": {"answer": {"type": "integer"}}},
                )
            )

        self.assertEqual(raised.exception.status, 502)

    def test_missing_api_key_fails_before_calling_anthropic(self) -> None:
        os.environ.pop("TEST_ANTHROPIC_API_KEY", None)
        provider = _provider()

        with self.assertRaises(gestalt.Error) as raised:
            provider.generate(
                gestalt.GenerateModelRequest(
                    model="claude-request", messages=[gestalt.ModelMessage(role="user", text="Hello")]
                )
            )

        self.assertEqual(raised.exception.status, 412)
        self.assertEqual(FakeAnthropic.instances, [])

    def test_rejects_non_object_response_schema_before_calling_anthropic(self) -> None:
        provider = _provider()

        with self.assertRaises(gestalt.Error) as raised:
            provider.generate(
                gestalt.GenerateModelRequest(
                    messages=[gestalt.ModelMessage(role="user", text="Return one value.")],
                    response_schema={"type": "array"},
                )
            )

        self.assertEqual(raised.exception.status, 400)
        self.assertEqual(FakeAnthropic.instances, [])

    def test_invalid_config_raises_gestalt_error(self) -> None:
        provider = provider_module.AnthropicModelProvider()

        with self.assertRaises(gestalt.Error) as raised:
            provider.configure("anthropic", {"apiKeyEnv": "TEST_ANTHROPIC_API_KEY", "maxRetries": -1})

        self.assertEqual(raised.exception.status, 400)


def _provider(config: dict[str, Any] | None = None) -> provider_module.AnthropicModelProvider:
    provider = provider_module.AnthropicModelProvider()
    provider.configure(
        "anthropic", {"apiKeyEnv": "TEST_ANTHROPIC_API_KEY", "defaultModel": "claude-default", **(config or {})}
    )
    return provider


def _message_response(
    *, content: list[Any], stop_reason: str, input_tokens: int, output_tokens: int, model: str = "claude-default"
) -> Any:
    return SimpleNamespace(
        id="msg_test",
        model=model,
        content=content,
        stop_reason=stop_reason,
        usage=SimpleNamespace(input_tokens=input_tokens, output_tokens=output_tokens),
    )
