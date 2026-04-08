from __future__ import annotations

import base64
import json
import os
import urllib.error
import urllib.request
from http import HTTPStatus
from typing import Any, TypeAlias, cast

import gestalt

from .models import ListInput

ListResult: TypeAlias = dict[str, Any] | gestalt.Response[dict[str, str]]


def list_operation(operation_id: str, input: ListInput, req: gestalt.Request) -> ListResult:
    token = req.token.strip()
    if not token:
        return gestalt.Response(status=HTTPStatus.UNAUTHORIZED, body={"error": "token is required"})

    payload: dict[str, object] = {}
    if input.limit is not None:
        payload["limit"] = input.limit
    if input.cursor:
        payload["cursor"] = input.cursor

    request = urllib.request.Request(
        url=f"{os.environ.get('ASHBY_BASE_URL', 'https://api.ashbyhq.com').rstrip('/')}/{operation_id}",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": "Basic "
            + base64.b64encode(f"{token}:".encode("utf-8")).decode("ascii"),
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response_body = response.read()
    except urllib.error.HTTPError as exc:
        return gestalt.Response(
            status=exc.code,
            body={"error": f"Ashby {operation_id} returned status {exc.code}: {_error_message(exc.read())}"},
        )
    except urllib.error.URLError as exc:
        return gestalt.Response(
            status=HTTPStatus.BAD_GATEWAY,
            body={"error": f"call Ashby {operation_id}: {exc.reason}"},
        )

    try:
        upstream = json.loads(response_body)
    except json.JSONDecodeError as exc:
        return gestalt.Response(
            status=HTTPStatus.BAD_GATEWAY,
            body={"error": f"parse Ashby {operation_id} response: {exc}"},
        )

    if not upstream.get("success"):
        return gestalt.Response(
            status=HTTPStatus.BAD_GATEWAY,
            body={"error": f"Ashby {operation_id} failed: {_error_message(upstream)}"},
        )

    results = upstream.get("results")
    if not isinstance(results, list):
        results = []

    result: dict[str, Any] = {"data": results, "pagination": None}
    if upstream.get("moreDataAvailable") is not None or upstream.get("nextCursor"):
        result["pagination"] = {
            "has_more": bool(upstream.get("moreDataAvailable")),
            "cursor": str(upstream.get("nextCursor") or ""),
        }
    return result


def _error_message(value: object) -> str:
    if isinstance(value, bytes):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            message = value.decode("utf-8", errors="replace").strip()
            return message or "unknown error"

    if isinstance(value, dict):
        value_dict = cast(dict[str, object], value)

        error = value_dict.get("error")
        if isinstance(error, str) and error:
            return error

        message = value_dict.get("message")
        if isinstance(message, str) and message:
            return message

        errors = value_dict.get("errors")
        if isinstance(errors, list):
            for item in errors:
                if isinstance(item, dict):
                    item_dict = cast(dict[str, object], item)
                    nested = item_dict.get("message")
                    if isinstance(nested, str) and nested:
                        return nested

    text = str(value).strip()
    return text or "unknown error"
