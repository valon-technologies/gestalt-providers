import hashlib
import re
from dataclasses import dataclass, field
from typing import Any

DEFAULT_MAX_STEPS = 8
DEFAULT_TIMEOUT_SECONDS = 120.0
UNSUPPORTED_CONFIG_FIELDS = frozenset({"runStore", "idempotencyStore"})


@dataclass(slots=True)
class SimpleAgentConfig:
    name: str
    run_store: str
    idempotency_store: str
    default_model: str
    aliases: dict[str, str] = field(default_factory=dict)
    max_steps: int = DEFAULT_MAX_STEPS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    system_prompt: str = ""
    anthropic_api_key: str = ""
    openai_api_key: str = ""

    @classmethod
    def from_dict(cls, *, name: str, raw_config: dict[str, Any]) -> "SimpleAgentConfig":
        unsupported_fields = sorted(key for key in raw_config if key in UNSUPPORTED_CONFIG_FIELDS)
        if unsupported_fields:
            raise ValueError(
                f"{', '.join(unsupported_fields)} are not supported; agent/simple derives store names from the provider name"
            )

        default_model = _trimmed_text(raw_config.get("defaultModel"))
        aliases = _coerce_aliases(raw_config.get("aliases"))
        max_steps = _coerce_positive_int(raw_config.get("maxSteps"), default=DEFAULT_MAX_STEPS, field_name="maxSteps")
        timeout_seconds = _coerce_positive_float(
            raw_config.get("timeoutSeconds"), default=DEFAULT_TIMEOUT_SECONDS, field_name="timeoutSeconds"
        )
        run_store, idempotency_store = _derive_store_names(name)
        return cls(
            name=name,
            run_store=run_store,
            idempotency_store=idempotency_store,
            default_model=default_model,
            aliases=aliases,
            max_steps=max_steps,
            timeout_seconds=timeout_seconds,
            system_prompt=_trimmed_text(raw_config.get("systemPrompt")),
            anthropic_api_key=_trimmed_text(raw_config.get("anthropicApiKey")),
            openai_api_key=_trimmed_text(raw_config.get("openaiApiKey")),
        )

    def resolve_model(self, requested_model: str) -> str:
        candidate = requested_model.strip() or self.default_model
        if not candidate:
            raise ValueError("model is required when config.defaultModel is not set")
        return self.aliases.get(candidate, candidate)


def _derive_store_names(provider_name: str) -> tuple[str, str]:
    raw_name = provider_name.strip() or "simple"
    slug = re.sub(r"[^a-z0-9]+", "_", raw_name.lower()).strip("_") or "default"
    digest = hashlib.sha256(raw_name.encode("utf-8")).hexdigest()[:10]
    prefix = f"agent_simple_{slug}_{digest}"
    return f"{prefix}_turns", f"{prefix}_idempotency"


def _coerce_aliases(raw_value: Any) -> dict[str, str]:
    if raw_value is None:
        return {}
    if not isinstance(raw_value, dict):
        raise ValueError("aliases must be an object of string-to-string mappings")
    aliases: dict[str, str] = {}
    for raw_key, raw_model in raw_value.items():
        alias = str(raw_key).strip()
        model = str(raw_model).strip()
        if not alias or not model:
            raise ValueError("aliases must contain only non-empty keys and values")
        aliases[alias] = model
    return aliases


def _coerce_positive_int(raw_value: Any, *, default: int, field_name: str) -> int:
    if raw_value is None or str(raw_value).strip() == "":
        return default
    value = int(raw_value)
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")
    return value


def _coerce_positive_float(raw_value: Any, *, default: float, field_name: str) -> float:
    if raw_value is None or str(raw_value).strip() == "":
        return default
    value = float(raw_value)
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")
    return value


def _trimmed_text(raw_value: Any) -> str:
    return str(raw_value or "").strip()
