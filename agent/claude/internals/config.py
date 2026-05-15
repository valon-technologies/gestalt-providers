from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass, field
from typing import Any

from .claude_code_config import ClaudeCodeConfig

DEFAULT_MODEL = "claude-sonnet-4-5-20250929"
DEFAULT_TIMEOUT_SECONDS = 300.0
DEFAULT_PERMISSION_MODE = "dontAsk"
SUPPORTED_PERMISSION_MODES = frozenset({"default", "acceptEdits", "bypassPermissions", "dontAsk", "plan"})


@dataclass(frozen=True, slots=True)
class ClaudeAgentConfig:
    name: str
    run_store: str
    idempotency_store: str
    default_model: str = DEFAULT_MODEL
    cli_path: str = ""
    working_directory: str = ""
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    system_prompt: str = ""
    permission_mode: str = DEFAULT_PERMISSION_MODE
    anthropic_api_key: str = ""
    claude_code: ClaudeCodeConfig = field(default_factory=ClaudeCodeConfig)

    @classmethod
    def from_dict(cls, *, name: str, raw_config: dict[str, Any]) -> "ClaudeAgentConfig":
        permission_mode = _trimmed_text(raw_config.get("permissionMode")) or DEFAULT_PERMISSION_MODE
        if permission_mode not in SUPPORTED_PERMISSION_MODES:
            raise ValueError(f"permissionMode must be one of {', '.join(sorted(SUPPORTED_PERMISSION_MODES))}")
        claude_code = ClaudeCodeConfig.from_raw(raw_config)
        if permission_mode == "bypassPermissions" and claude_code.has_tool_permissions:
            raise ValueError("permissionMode bypassPermissions cannot be used with allowedTools")

        working_directory = _trimmed_text(raw_config.get("workingDirectory"))
        if working_directory and not os.path.isdir(working_directory):
            raise ValueError("workingDirectory must be an existing directory")

        provider_name = name.strip() or "claude"
        run_store, idempotency_store = _derive_store_names(provider_name)

        return cls(
            name=provider_name,
            run_store=run_store,
            idempotency_store=idempotency_store,
            default_model=_trimmed_text(raw_config.get("defaultModel")) or DEFAULT_MODEL,
            cli_path=_trimmed_text(raw_config.get("cliPath")),
            working_directory=working_directory,
            timeout_seconds=_coerce_positive_float(
                raw_config.get("timeoutSeconds"), default=DEFAULT_TIMEOUT_SECONDS, field_name="timeoutSeconds"
            ),
            system_prompt=_trimmed_text(raw_config.get("systemPrompt")),
            permission_mode=permission_mode,
            anthropic_api_key=_trimmed_text(raw_config.get("anthropicApiKey")),
            claude_code=claude_code,
        )

    def resolve_model(self, requested_model: str) -> str:
        model = requested_model.strip() or self.default_model
        if not model:
            raise ValueError("model is required when config.defaultModel is not set")
        return model


def _derive_store_names(provider_name: str) -> tuple[str, str]:
    raw_name = provider_name.strip() or "claude"
    slug = re.sub(r"[^a-z0-9]+", "_", raw_name.lower()).strip("_") or "default"
    digest = hashlib.sha256(raw_name.encode("utf-8")).hexdigest()[:10]
    prefix = f"agent_claude_{slug}_{digest}"
    return f"{prefix}_turns", f"{prefix}_idempotency"


def _coerce_positive_float(raw_value: Any, *, default: float, field_name: str) -> float:
    if raw_value is None or str(raw_value).strip() == "":
        return default
    value = float(raw_value)
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")
    return value


def _trimmed_text(raw_value: Any) -> str:
    return str(raw_value or "").strip()
