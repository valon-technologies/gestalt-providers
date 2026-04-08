from typing import Any, cast


def map_slice(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    items: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            items.append(cast(dict[str, Any], item))
    return items


def map_field(data: object, key: str) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {}
    value = cast(dict[str, Any], data).get(key)
    if isinstance(value, dict):
        return cast(dict[str, Any], value)
    return {}


def string_field(data: object, key: str) -> str:
    if not isinstance(data, dict):
        return ""
    value = cast(dict[str, Any], data).get(key)
    if isinstance(value, str):
        return value
    if isinstance(value, int | float):
        return str(value)
    return ""


def bool_field(data: object, key: str) -> bool | None:
    if not isinstance(data, dict):
        return None
    value = cast(dict[str, Any], data).get(key)
    if isinstance(value, bool):
        return value
    return None
