import hashlib
import re
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any

DEFAULT_MAX_STEPS = 8
DEFAULT_TIMEOUT_SECONDS = 120.0
DEFAULT_RESUME_STARTUP_SCAN_LIMIT = 100
DEFAULT_RESUME_WORKER_LEASE_SECONDS = 60.0
UNSUPPORTED_CONFIG_FIELDS = frozenset({"runStore", "idempotencyStore"})
RESUME_CONFIG_FIELDS = frozenset({"enabled", "startupScanLimit", "workerLeaseSeconds"})


@dataclass(slots=True)
class ResumeConfig:
    enabled: bool = True
    startup_scan_limit: int = DEFAULT_RESUME_STARTUP_SCAN_LIMIT
    worker_lease_seconds: float = DEFAULT_RESUME_WORKER_LEASE_SECONDS


@dataclass(slots=True)
class SimpleAgentConfig:
    name: str
    run_store: str
    idempotency_store: str
    default_model: str
    aliases: dict[str, str] = field(default_factory=dict)
    provider_options: dict[str, Any] = field(default_factory=dict)
    max_steps: int = DEFAULT_MAX_STEPS
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    system_prompt: str = ""
    anthropic_api_key: str = ""
    openai_api_key: str = ""
    resume: ResumeConfig = field(default_factory=ResumeConfig)

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
            provider_options=_coerce_provider_options(raw_config.get("providerOptions")),
            max_steps=max_steps,
            timeout_seconds=timeout_seconds,
            system_prompt=_trimmed_text(raw_config.get("systemPrompt")),
            anthropic_api_key=_trimmed_text(raw_config.get("anthropicApiKey")),
            openai_api_key=_trimmed_text(raw_config.get("openaiApiKey")),
            resume=_coerce_resume_config(raw_config.get("resume")),
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


def _coerce_provider_options(raw_value: Any) -> dict[str, Any]:
    if raw_value is None:
        return {}
    if not isinstance(raw_value, dict):
        raise ValueError("providerOptions must be an object")

    options: dict[str, Any] = {}
    for raw_key, raw_option in raw_value.items():
        key = str(raw_key).strip()
        if not key:
            raise ValueError("providerOptions must contain only non-empty keys")
        options[key] = deepcopy(raw_option)
    return options


def _coerce_resume_config(raw_value: Any) -> ResumeConfig:
    if raw_value is None:
        return ResumeConfig()
    if not isinstance(raw_value, dict):
        raise ValueError("resume must be an object")
    unsupported_fields = sorted(str(key) for key in raw_value if str(key) not in RESUME_CONFIG_FIELDS)
    if unsupported_fields:
        raise ValueError(f"resume contains unsupported fields: {', '.join(unsupported_fields)}")
    return ResumeConfig(
        enabled=_coerce_bool(raw_value.get("enabled"), default=True, field_name="resume.enabled"),
        startup_scan_limit=_coerce_positive_int(
            raw_value.get("startupScanLimit"),
            default=DEFAULT_RESUME_STARTUP_SCAN_LIMIT,
            field_name="resume.startupScanLimit",
        ),
        worker_lease_seconds=_coerce_positive_float(
            raw_value.get("workerLeaseSeconds"),
            default=DEFAULT_RESUME_WORKER_LEASE_SECONDS,
            field_name="resume.workerLeaseSeconds",
        ),
    )


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


def _coerce_bool(raw_value: Any, *, default: bool, field_name: str) -> bool:
    if raw_value is None or str(raw_value).strip() == "":
        return default
    if isinstance(raw_value, bool):
        return raw_value
    if isinstance(raw_value, str):
        normalized = raw_value.strip().lower()
        if normalized in {"true", "1", "yes", "on"}:
            return True
        if normalized in {"false", "0", "no", "off"}:
            return False
    raise ValueError(f"{field_name} must be a boolean")


def _trimmed_text(raw_value: Any) -> str:
    return str(raw_value or "").strip()
