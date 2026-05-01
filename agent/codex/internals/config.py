from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

DEFAULT_CODEX_COMMAND = "codex"
DEFAULT_CODEX_ARGS = ("mcp-server",)
DEFAULT_TIMEOUT_SECONDS = 300.0
DEFAULT_APPROVAL_POLICY = "never"
DEFAULT_SANDBOX = "read-only"
SUPPORTED_APPROVAL_POLICIES = frozenset({"never"})
SUPPORTED_SANDBOXES = frozenset({"read-only", "workspace-write", "danger-full-access"})


@dataclass(slots=True)
class CodexAgentConfig:
    name: str
    default_model: str = ""
    codex_command: str = DEFAULT_CODEX_COMMAND
    codex_args: list[str] = field(default_factory=lambda: list(DEFAULT_CODEX_ARGS))
    working_directory: str = ""
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    system_prompt: str = ""
    approval_policy: str = DEFAULT_APPROVAL_POLICY
    sandbox: str = DEFAULT_SANDBOX
    openai_api_key: str = ""

    @classmethod
    def from_dict(cls, *, name: str, raw_config: dict[str, Any]) -> "CodexAgentConfig":
        approval_policy = _trimmed_text(raw_config.get("approvalPolicy")) or DEFAULT_APPROVAL_POLICY
        if approval_policy not in SUPPORTED_APPROVAL_POLICIES:
            raise ValueError("approvalPolicy must be never")

        sandbox = _trimmed_text(raw_config.get("sandbox")) or DEFAULT_SANDBOX
        if sandbox not in SUPPORTED_SANDBOXES:
            raise ValueError(f"sandbox must be one of {', '.join(sorted(SUPPORTED_SANDBOXES))}")

        working_directory = _trimmed_text(raw_config.get("workingDirectory"))
        if working_directory and not os.path.isdir(working_directory):
            raise ValueError("workingDirectory must be an existing directory")

        codex_command = _trimmed_text(raw_config.get("codexCommand")) or DEFAULT_CODEX_COMMAND
        codex_args = _coerce_string_list(raw_config.get("codexArgs"), default=list(DEFAULT_CODEX_ARGS))

        return cls(
            name=name.strip() or "codex",
            default_model=_trimmed_text(raw_config.get("defaultModel")),
            codex_command=codex_command,
            codex_args=codex_args,
            working_directory=working_directory,
            timeout_seconds=_coerce_positive_float(
                raw_config.get("timeoutSeconds"), default=DEFAULT_TIMEOUT_SECONDS, field_name="timeoutSeconds"
            ),
            system_prompt=_trimmed_text(raw_config.get("systemPrompt")),
            approval_policy=approval_policy,
            sandbox=sandbox,
            openai_api_key=_trimmed_text(raw_config.get("openaiApiKey")),
        )

    def resolve_model(self, requested_model: str) -> str:
        return requested_model.strip() or self.default_model


def _coerce_positive_float(raw_value: Any, *, default: float, field_name: str) -> float:
    if raw_value is None or str(raw_value).strip() == "":
        return default
    value = float(raw_value)
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")
    return value


def _coerce_string_list(raw_value: Any, *, default: list[str]) -> list[str]:
    if raw_value is None:
        return list(default)
    if not isinstance(raw_value, list) or not all(isinstance(value, str) for value in raw_value):
        raise ValueError("codexArgs must be a list of strings")
    return [value for value in raw_value]


def _trimmed_text(raw_value: Any) -> str:
    return str(raw_value or "").strip()
