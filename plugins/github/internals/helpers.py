from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def str_field(data: Mapping[str, Any], *path: str) -> str:
    if len(path) > 1:
        return nested_str(data, *path)
    value = data.get(path[0]) if path else ""
    if isinstance(value, str):
        return value.strip()
    return ""


def nested_str(data: Mapping[str, Any], *path: str) -> str:
    value: Any = data
    for key in path:
        if not isinstance(value, Mapping):
            return ""
        value = value.get(key)
    if isinstance(value, str):
        return value.strip()
    return ""


def int_field(data: Mapping[str, Any], field_name: str) -> int:
    value = data.get(field_name)
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return 0


def map_field(data: Mapping[str, Any], field_name: str) -> dict[str, Any]:
    value = data.get(field_name)
    if isinstance(value, dict):
        return value
    return {}


def require_text(value: str, name: str) -> str:
    trimmed = value.strip()
    if not trimmed:
        raise ValueError(f"{name} is required")
    return trimmed


def require_slug(value: str, name: str) -> str:
    trimmed = require_text(value, name)
    if "/" in trimmed:
        raise ValueError(f"{name} must not contain '/'")
    return trimmed


def optional_slug(value: str, name: str) -> str:
    trimmed = value.strip()
    if not trimmed:
        return ""
    return require_slug(trimmed, name)


def require_branch_name(value: str, name: str) -> str:
    trimmed = require_text(value, name)
    invalid_chars = set("~^:?*[\\")
    if (
        trimmed.startswith("/")
        or trimmed.endswith("/")
        or trimmed.startswith("refs/")
        or "//" in trimmed
        or ".." in trimmed
        or trimmed.endswith(".lock")
        or any(char in invalid_chars or ord(char) < 32 for char in trimmed)
    ):
        raise ValueError(f"{name} is not a valid branch name")
    return trimmed
