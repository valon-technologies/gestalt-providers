from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

DEFAULT_CLAUDE_BINARY = "claude"
DEFAULT_TIMEOUT_SECONDS = 300.0
DEFAULT_PERMISSION_MODE = "bypassPermissions"
SUPPORTED_PERMISSION_MODES = frozenset({"default", "acceptEdits", "auto", "bypassPermissions", "dontAsk", "plan"})


@dataclass(slots=True)
class ClaudeAgentConfig:
    name: str
    default_model: str = ""
    claude_binary: str = DEFAULT_CLAUDE_BINARY
    working_directory: str = ""
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    system_prompt: str = ""
    permission_mode: str = DEFAULT_PERMISSION_MODE

    @classmethod
    def from_dict(cls, *, name: str, raw_config: dict[str, Any]) -> "ClaudeAgentConfig":
        permission_mode = _trimmed_text(raw_config.get("permissionMode")) or DEFAULT_PERMISSION_MODE
        if permission_mode not in SUPPORTED_PERMISSION_MODES:
            raise ValueError(f"permissionMode must be one of {', '.join(sorted(SUPPORTED_PERMISSION_MODES))}")

        working_directory = _trimmed_text(raw_config.get("workingDirectory"))
        if working_directory and not os.path.isdir(working_directory):
            raise ValueError("workingDirectory must be an existing directory")

        return cls(
            name=name.strip() or "claude",
            default_model=_trimmed_text(raw_config.get("defaultModel")),
            claude_binary=_trimmed_text(raw_config.get("claudeBinary")) or DEFAULT_CLAUDE_BINARY,
            working_directory=working_directory,
            timeout_seconds=_coerce_positive_float(
                raw_config.get("timeoutSeconds"), default=DEFAULT_TIMEOUT_SECONDS, field_name="timeoutSeconds"
            ),
            system_prompt=_trimmed_text(raw_config.get("systemPrompt")),
            permission_mode=permission_mode,
        )

    def resolve_model(self, requested_model: str) -> str:
        model = requested_model.strip() or self.default_model
        if not model:
            raise ValueError("model is required when config.defaultModel is not set")
        return model


def _coerce_positive_float(raw_value: Any, *, default: float, field_name: str) -> float:
    if raw_value is None or str(raw_value).strip() == "":
        return default
    value = float(raw_value)
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")
    return value


def _trimmed_text(raw_value: Any) -> str:
    return str(raw_value or "").strip()
