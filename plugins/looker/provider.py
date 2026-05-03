from __future__ import annotations

import dataclasses
import hashlib
import json
import keyword
import os
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt

OPENAPI_SPEC_URL = (
    "https://raw.githubusercontent.com/looker-open-source/sdk-codegen/"
    "d3b299010e95efd86c01f11a7935cccf56063188/spec/Looker.4.0.oas.json"
)
SPEC_URL_ENV = "LOOKER_OPENAPI_SPEC_URL"
SPEC_PATH_ENV = "LOOKER_OPENAPI_SPEC_PATH"
LOOKER_AUTH_SCHEME = "token"
REQUEST_TIMEOUT_SECONDS = 30
TOKEN_EXPIRY_SKEW_SECONDS = 60
DEFAULT_TOKEN_TTL_SECONDS = 3600
USER_AGENT = "gestalt-looker-provider/0.0.1a2"
SKIPPED_OPERATION_IDS = {"login", "logout"}

OperationResult: TypeAlias = (
    dict[str, Any] | list[Any] | str | None | gestalt.Response[Any]
)

plugin = gestalt.Plugin("looker")


@dataclass(frozen=True, slots=True)
class Parameter:
    name: str
    wire_name: str
    location: str
    type_name: str
    description: str
    required: bool


@dataclass(frozen=True, slots=True)
class Operation:
    operation_id: str
    method: str
    path: str
    title: str
    description: str
    tags: list[str]
    parameters: list[Parameter]
    request_content_type: str
    request_body_required: bool


@dataclass(frozen=True, slots=True)
class CachedToken:
    value: str
    expires_at: float


class LookerError(Exception):
    def __init__(self, status: int | HTTPStatus, message: str) -> None:
        self.status = int(status)
        self.message = message
        super().__init__(message)


_TOKEN_CACHE: dict[str, CachedToken] = {}
_TOKEN_LOCK = threading.Lock()


def _load_openapi_spec() -> dict[str, Any]:
    spec_path = os.environ.get(SPEC_PATH_ENV, "").strip()
    if spec_path:
        with open(spec_path, encoding="utf-8") as file:
            data = json.load(file)
        if isinstance(data, dict):
            return data
        raise RuntimeError(f"{SPEC_PATH_ENV} must point to an OpenAPI object")

    spec_url = os.environ.get(SPEC_URL_ENV, OPENAPI_SPEC_URL).strip()
    request = urllib.request.Request(
        spec_url,
        headers={"Accept": "application/json", "User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(
            request,
            timeout=REQUEST_TIMEOUT_SECONDS,
        ) as response:
            data = json.loads(response.read().decode("utf-8"))
    except OSError as err:
        raise RuntimeError(f"load Looker OpenAPI spec: {err}") from err
    if isinstance(data, dict):
        return data
    raise RuntimeError("Looker OpenAPI spec must be a JSON object")


def _operations_from_spec(spec: Mapping[str, Any]) -> Iterable[Operation]:
    paths = spec.get("paths")
    if not isinstance(paths, Mapping):
        return []

    operations: list[Operation] = []
    for raw_path, path_item in paths.items():
        if not isinstance(raw_path, str) or not isinstance(path_item, Mapping):
            continue
        path_parameters = _as_list(path_item.get("parameters"))
        for method, raw_operation in path_item.items():
            if method.lower() not in {
                "delete",
                "get",
                "patch",
                "post",
                "put",
            }:
                continue
            if not isinstance(raw_operation, Mapping):
                continue
            operation_id = str(raw_operation.get("operationId") or "").strip()
            if not operation_id or operation_id in SKIPPED_OPERATION_IDS:
                continue

            parameters = _operation_parameters(
                spec,
                [*path_parameters, *_as_list(raw_operation.get("parameters"))],
                raw_operation.get("requestBody"),
            )
            operations.append(
                Operation(
                    operation_id=operation_id,
                    method=method.upper(),
                    path=raw_path,
                    title=str(raw_operation.get("summary") or operation_id).strip(),
                    description=str(
                        raw_operation.get("description")
                        or raw_operation.get("summary")
                        or ""
                    ).strip(),
                    tags=[
                        str(tag).strip()
                        for tag in _as_list(raw_operation.get("tags"))
                        if str(tag).strip()
                    ],
                    parameters=parameters,
                    request_content_type=_request_content_type(
                        spec,
                        raw_operation.get("requestBody"),
                    ),
                    request_body_required=_request_body_required(
                        spec,
                        raw_operation.get("requestBody"),
                    ),
                )
            )
    return operations


def _operation_parameters(
    spec: Mapping[str, Any],
    raw_parameters: list[Any],
    raw_request_body: Any,
) -> list[Parameter]:
    allocator = NameAllocator()
    parameters: list[Parameter] = []
    seen_wire_names: set[str] = set()

    for raw_parameter in raw_parameters:
        parameter = _resolve_ref(spec, raw_parameter)
        if not isinstance(parameter, Mapping):
            continue
        wire_name = str(parameter.get("name") or "").strip()
        location = str(parameter.get("in") or "").strip()
        if not wire_name or not location:
            continue

        seen_wire_names.add(wire_name)
        schema = _resolve_ref(spec, parameter.get("schema"))
        parameters.append(
            Parameter(
                name=allocator.allocate(wire_name),
                wire_name=wire_name,
                location=location,
                type_name=_schema_type(schema),
                description=str(parameter.get("description") or "").strip(),
                required=bool(parameter.get("required")) or location == "path",
            )
        )

    for body_parameter in _request_body_parameters(
        spec,
        raw_request_body,
        allocator,
        seen_wire_names,
    ):
        parameters.append(body_parameter)
    return parameters


def _request_body_parameters(
    spec: Mapping[str, Any],
    raw_request_body: Any,
    allocator: NameAllocator,
    seen_wire_names: set[str],
) -> list[Parameter]:
    request_body = _resolve_ref(spec, raw_request_body)
    if not isinstance(request_body, Mapping):
        return []
    media_type = _request_media_type(spec, request_body)
    if media_type is None:
        return []

    content_type, media = media_type
    schema = _resolve_ref(spec, media.get("schema"))
    if not isinstance(schema, Mapping):
        return []

    if content_type == "text/plain" or schema.get("type") == "string":
        name = allocator.allocate("body")
        return [
            Parameter(
                name=name,
                wire_name="body",
                location="body",
                type_name="string",
                description=str(
                    request_body.get("description")
                    or schema.get("description")
                    or "Request body"
                ).strip(),
                required=bool(request_body.get("required")),
            )
        ]

    properties = schema.get("properties")
    if not isinstance(properties, Mapping):
        return []
    required = {
        str(name)
        for name in _as_list(schema.get("required"))
        if isinstance(name, str)
    }

    parameters: list[Parameter] = []
    for wire_name, raw_schema in properties.items():
        if not isinstance(wire_name, str):
            continue
        property_schema = _resolve_ref(spec, raw_schema)
        if not isinstance(property_schema, Mapping):
            continue
        collides = wire_name in seen_wire_names
        parameter_name = allocator.allocate(
            f"{wire_name}_body" if collides else wire_name
        )
        description = str(property_schema.get("description") or "").strip()
        if collides:
            description = f"Request body field `{wire_name}`. {description}".strip()
        seen_wire_names.add(wire_name)
        parameters.append(
            Parameter(
                name=parameter_name,
                wire_name=wire_name,
                location="body",
                type_name=_schema_type(property_schema),
                description=description,
                required=wire_name in required,
            )
        )
    return parameters


def _request_content_type(
    spec: Mapping[str, Any],
    raw_request_body: Any,
) -> str:
    request_body = _resolve_ref(spec, raw_request_body)
    if not isinstance(request_body, Mapping):
        return ""
    media_type = _request_media_type(spec, request_body)
    if media_type is None:
        return ""
    return media_type[0]


def _request_body_required(
    spec: Mapping[str, Any],
    raw_request_body: Any,
) -> bool:
    request_body = _resolve_ref(spec, raw_request_body)
    return isinstance(request_body, Mapping) and bool(request_body.get("required"))


def _request_media_type(
    spec: Mapping[str, Any],
    request_body: Mapping[str, Any],
) -> tuple[str, Mapping[str, Any]] | None:
    content = request_body.get("content")
    if not isinstance(content, Mapping):
        return None
    for content_type in (
        "application/json",
        "text/plain",
        "application/x-www-form-urlencoded",
    ):
        media = content.get(content_type)
        if isinstance(media, Mapping):
            return content_type, media
    for content_type, media in content.items():
        if isinstance(content_type, str) and isinstance(media, Mapping):
            return content_type, media
    return None


def _resolve_ref(spec: Mapping[str, Any], value: Any) -> Any:
    if not isinstance(value, Mapping):
        return value
    ref = value.get("$ref")
    if not isinstance(ref, str) or not ref.startswith("#/"):
        return value
    current: Any = spec
    for part in ref[2:].split("/"):
        if not isinstance(current, Mapping):
            return value
        key = part.replace("~1", "/").replace("~0", "~")
        current = current.get(key)
    return current


def _schema_type(schema: Any) -> str:
    if not isinstance(schema, Mapping):
        return "object"
    raw_type = schema.get("type")
    if isinstance(raw_type, list):
        raw_type = next((item for item in raw_type if item != "null"), None)
    if raw_type in {"array", "boolean", "integer", "number", "object", "string"}:
        return str(raw_type)
    if "properties" in schema:
        return "object"
    if "items" in schema:
        return "array"
    return "object"


def _input_model(operation: Operation) -> type[gestalt.Model] | None:
    if not operation.parameters:
        return None

    annotations: dict[str, Any] = {}
    namespace: dict[str, Any] = {
        "__annotations__": annotations,
        "__module__": __name__,
    }
    for parameter in operation.parameters:
        annotations[parameter.name] = _python_type(parameter.type_name) | None
        namespace[parameter.name] = gestalt.field(
            description=parameter.description,
            default=None,
            required=parameter.required,
        )
    return type(
        f"{_class_name(operation.operation_id)}Input",
        (gestalt.Model,),
        namespace,
    )


def _python_type(type_name: str) -> type[Any]:
    match type_name:
        case "array":
            return list
        case "boolean":
            return bool
        case "integer":
            return int
        case "number":
            return float
        case "string":
            return str
        case _:
            return dict


def _register_operation(operation: Operation) -> None:
    input_type = _input_model(operation)

    if input_type is None:

        def handler(req: gestalt.Request) -> OperationResult:
            return _execute_operation(operation.operation_id, {}, req)

        annotations = {"req": gestalt.Request, "return": OperationResult}
    else:

        def handler(input_data: Any, req: gestalt.Request) -> OperationResult:
            return _execute_operation(
                operation.operation_id,
                _model_to_dict(input_data),
                req,
            )

        annotations = {
            "input_data": input_type,
            "req": gestalt.Request,
            "return": OperationResult,
        }

    handler.__name__ = _safe_identifier(operation.operation_id)
    setattr(handler, "__annotations__", annotations)
    plugin.operation(
        id=operation.operation_id,
        method=operation.method,
        title=operation.title,
        description=operation.description,
        tags=operation.tags,
        read_only=operation.method == "GET",
    )(handler)


def _execute_operation(
    operation_id: str,
    params: dict[str, Any],
    req: gestalt.Request,
) -> gestalt.Response[Any]:
    operation = _OPERATIONS.get(operation_id)
    if operation is None:
        return _error_response(HTTPStatus.NOT_FOUND, "unknown operation")
    try:
        base_url = _api_base_url(req.connection_param("host") or "")
        access_token = _access_token(base_url, req.token)
        request = _build_api_request(operation, base_url, access_token, params)
        return _send_api_request(request)
    except LookerError as err:
        return _error_response(err.status, err.message)


def _access_token(base_url: str, credential_token: str) -> str:
    client_id, client_secret = _credentials_from_token(credential_token)
    cache_key = _cache_key(base_url, client_id, client_secret)
    now = time.time()
    with _TOKEN_LOCK:
        cached = _TOKEN_CACHE.get(cache_key)
        if cached is not None and cached.expires_at > now:
            return cached.value

    token, expires_at = _login(base_url, client_id, client_secret)
    with _TOKEN_LOCK:
        _TOKEN_CACHE[cache_key] = CachedToken(token, expires_at)
    return token


def _credentials_from_token(credential_token: str) -> tuple[str, str]:
    token = credential_token.strip()
    if not token:
        raise LookerError(
            HTTPStatus.UNAUTHORIZED,
            "missing Looker client credentials; connect with client_id and client_secret",
        )
    try:
        credentials = json.loads(token)
    except json.JSONDecodeError as err:
        raise LookerError(
            HTTPStatus.UNAUTHORIZED,
            "Looker connection must use named client_id and client_secret credentials",
        ) from err
    if not isinstance(credentials, Mapping):
        raise LookerError(
            HTTPStatus.UNAUTHORIZED,
            "Looker connection credentials must be a JSON object",
        )

    client_id = str(credentials.get("client_id") or "").strip()
    client_secret = str(credentials.get("client_secret") or "").strip()
    if not client_id or not client_secret:
        raise LookerError(
            HTTPStatus.UNAUTHORIZED,
            "Looker connection requires client_id and client_secret",
        )
    return client_id, client_secret


def _login(base_url: str, client_id: str, client_secret: str) -> tuple[str, float]:
    body = urllib.parse.urlencode(
        {"client_id": client_id, "client_secret": client_secret},
    ).encode("utf-8")
    request = urllib.request.Request(
        f"{base_url}/login",
        data=body,
        method="POST",
        headers={
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": USER_AGENT,
        },
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=REQUEST_TIMEOUT_SECONDS,
        ) as response:
            raw = response.read()
            payload = _decode_body(raw, response.headers.get("Content-Type", ""))
    except urllib.error.HTTPError as err:
        message = _looker_error_message(err)
        raise LookerError(
            HTTPStatus.UNAUTHORIZED,
            f"Looker login failed ({err.code}): {message}",
        ) from err
    except OSError as err:
        raise LookerError(
            HTTPStatus.BAD_GATEWAY,
            f"Looker login request failed: {err}",
        ) from err

    if not isinstance(payload, Mapping):
        raise LookerError(
            HTTPStatus.BAD_GATEWAY,
            "Looker login returned an unexpected response",
        )
    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        raise LookerError(
            HTTPStatus.BAD_GATEWAY,
            "Looker login response did not include access_token",
        )

    expires_in = _int_value(payload.get("expires_in"), DEFAULT_TOKEN_TTL_SECONDS)
    expires_at = time.time() + max(0, expires_in - TOKEN_EXPIRY_SKEW_SECONDS)
    return access_token, expires_at


def _build_api_request(
    operation: Operation,
    base_url: str,
    access_token: str,
    params: dict[str, Any],
) -> urllib.request.Request:
    path = operation.path
    query: list[tuple[str, str]] = []
    headers = {
        "Accept": "application/json",
        "Authorization": f"{LOOKER_AUTH_SCHEME} {access_token}",
        "User-Agent": USER_AGENT,
    }
    body_fields: dict[str, Any] = {}

    for parameter in operation.parameters:
        value = params.get(parameter.name)
        if value is None:
            if parameter.required:
                raise LookerError(
                    HTTPStatus.BAD_REQUEST,
                    f"missing required parameter: {parameter.name}",
                )
            continue
        match parameter.location:
            case "path":
                path = path.replace(
                    "{" + parameter.wire_name + "}",
                    urllib.parse.quote(str(value), safe=""),
                )
            case "query":
                query.extend(_query_values(parameter.wire_name, value))
            case "header":
                headers[parameter.wire_name] = str(value)
            case "body":
                body_fields[parameter.wire_name] = value

    url = f"{base_url}{path}"
    if query:
        url = f"{url}?{urllib.parse.urlencode(query)}"

    body = _request_body(operation, body_fields)
    if body is not None:
        headers["Content-Type"] = operation.request_content_type or "application/json"

    return urllib.request.Request(
        url,
        data=body,
        method=operation.method,
        headers=headers,
    )


def _request_body(operation: Operation, body_fields: dict[str, Any]) -> bytes | None:
    if not operation.request_content_type:
        return None
    if operation.request_content_type == "text/plain":
        if "body" not in body_fields:
            return b"" if operation.request_body_required else None
        return str(body_fields["body"]).encode("utf-8")
    if operation.request_content_type == "application/x-www-form-urlencoded":
        if not body_fields and not operation.request_body_required:
            return None
        return urllib.parse.urlencode(body_fields, doseq=True).encode("utf-8")
    if not body_fields and not operation.request_body_required:
        return None
    return json.dumps(body_fields).encode("utf-8")


def _send_api_request(request: urllib.request.Request) -> gestalt.Response[Any]:
    try:
        with urllib.request.urlopen(
            request,
            timeout=REQUEST_TIMEOUT_SECONDS,
        ) as response:
            body = _decode_body(
                response.read(),
                response.headers.get("Content-Type", ""),
            )
            return gestalt.Response(status=response.status, body=body)
    except urllib.error.HTTPError as err:
        return gestalt.Response(
            status=err.code,
            body=_decode_body(err.read(), err.headers.get("Content-Type", "")),
        )
    except OSError as err:
        return _error_response(
            HTTPStatus.BAD_GATEWAY,
            f"Looker API request failed: {err}",
        )


def _decode_body(raw: bytes, content_type: str) -> Any:
    if not raw:
        return None
    text = raw.decode("utf-8", errors="replace")
    if "json" in content_type.lower():
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    return text


def _looker_error_message(err: urllib.error.HTTPError) -> str:
    body = _decode_body(err.read(), err.headers.get("Content-Type", ""))
    if isinstance(body, Mapping):
        for key in ("message", "error", "error_description"):
            value = body.get(key)
            if value:
                return str(value)
    if isinstance(body, str) and body.strip():
        return body.strip()
    try:
        return HTTPStatus(err.code).phrase
    except ValueError:
        return "error"


def _api_base_url(host: str) -> str:
    raw_host = host.strip().rstrip("/")
    if not raw_host:
        raise LookerError(
            HTTPStatus.BAD_REQUEST,
            "missing Looker host connection parameter",
        )
    parsed = urllib.parse.urlsplit(
        raw_host if "://" in raw_host else f"https://{raw_host}",
    )
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise LookerError(
            HTTPStatus.BAD_REQUEST,
            "Looker host must be a host name, optionally prefixed with http:// or https://",
        )

    path = parsed.path.rstrip("/")
    if path.endswith("/api/4.0"):
        api_path = path
    elif path:
        api_path = f"{path}/api/4.0"
    else:
        api_path = "/api/4.0"
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, api_path, "", ""))


def _query_values(name: str, value: Any) -> list[tuple[str, str]]:
    if isinstance(value, (list, tuple, set)):
        return [(name, _query_value(item)) for item in value]
    return [(name, _query_value(value))]


def _query_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _cache_key(base_url: str, client_id: str, client_secret: str) -> str:
    material = f"{base_url}\0{client_id}\0{client_secret}".encode()
    return hashlib.sha256(material).hexdigest()


def _model_to_dict(value: Any) -> dict[str, Any]:
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _error_response(
    status: int | HTTPStatus,
    message: str,
) -> gestalt.Response[dict[str, str]]:
    return gestalt.Response(status=int(status), body={"error": message})


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _int_value(value: Any, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return default
    return default


def _safe_identifier(value: str) -> str:
    name = value.strip()
    name = name.replace("$", "dollar_").replace("@", "at_")
    name = re.sub(r"[^0-9A-Za-z_]+", "_", name).strip("_")
    name = re.sub(r"_+", "_", name)
    if not name:
        name = "value"
    if name[0].isdigit():
        name = f"_{name}"
    if keyword.iskeyword(name):
        name = f"{name}_"
    return name


def _class_name(operation_id: str) -> str:
    parts = re.split(r"[^0-9A-Za-z]+", operation_id)
    value = "".join(part[:1].upper() + part[1:] for part in parts if part)
    return value or "Operation"


class NameAllocator:
    def __init__(self) -> None:
        self._used: set[str] = set()

    def allocate(self, raw_name: str) -> str:
        base = _safe_identifier(raw_name)
        name = base
        suffix = 2
        while name in self._used:
            name = f"{base}_{suffix}"
            suffix += 1
        self._used.add(name)
        return name


_OPERATIONS = {
    operation.operation_id: operation
    for operation in _operations_from_spec(_load_openapi_spec())
}

for _operation in _OPERATIONS.values():
    _register_operation(_operation)
