from __future__ import annotations

from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt

from internals.cli import run_retool_cli

plugin = gestalt.Plugin("retool")
CLI_SESSION_CONNECTION = "cliSession"

ErrorResponse: TypeAlias = gestalt.Response[dict[str, str]]
OperationResult: TypeAlias = "CliOutput | ErrorResponse"


class CliFile(gestalt.Model):
    path: str
    encoding: str
    size_bytes: int
    content: str = gestalt.field(default="", required=False)
    content_base64: str = gestalt.field(default="", required=False)


class CliOutput(gestalt.Model):
    command: list[str]
    exit_code: int
    stdout: str
    stderr: str
    files: list[CliFile] = gestalt.field(default_factory=list, required=False)
    parsed_json: Any | None = gestalt.field(default=None, required=False)


class CliBaseInput(gestalt.Model):
    stdin: str = gestalt.field(
        description="Optional stdin to pass to interactive Retool CLI prompts",
        default="",
        required=False,
    )
    timeout_seconds: int = gestalt.field(
        description="Maximum seconds to wait for the Retool CLI command",
        default=120,
        required=False,
    )


class CliAppExportInput(CliBaseInput):
    app_name: str = gestalt.field(
        description="Exact Retool app name to export",
        default="",
        required=True,
    )


class CliTerraformInput(CliBaseInput):
    generate_imports: bool = gestalt.field(
        description="Generate Terraform import blocks",
        default=True,
        required=False,
    )
    generate_config: bool = gestalt.field(
        description="Generate Terraform resource configuration",
        default=True,
        required=False,
    )
    imports_filename: str = gestalt.field(
        description="Output filename for Terraform import blocks",
        default="retool-imports.tf",
        required=False,
    )
    config_filename: str = gestalt.field(
        description="Output filename for Terraform resource configuration",
        default="retool-config.tf",
        required=False,
    )
    host: str = gestalt.field(
        description="Retool host domain, for example example.retool.com",
        default="",
        required=True,
    )
    scheme: str = gestalt.field(
        description="Retool URL scheme: https or http",
        default="https",
        required=False,
    )


@plugin.configure
def configure(_name: str, _config: dict[str, Any]) -> None:
    return None


@plugin.operation(
    id="apps.exportApp",
    method="POST",
    description="Export a Retool app JSON with the official Retool CLI",
)
def apps_export_app(input: CliAppExportInput, req: gestalt.Request) -> OperationResult:
    app_name = input.app_name.strip()
    if not app_name:
        return _bad_request("app_name is required")
    if "/" in app_name or "\\" in app_name:
        return _bad_request("app_name cannot contain path separators")

    try:
        credentials = _cli_session_credentials(req)
    except ValueError as err:
        return _bad_request(str(err))

    return _run(
        ["apps", "--export", app_name],
        input,
        keyring_credentials=credentials,
        redact_values=[credentials["accessToken"], credentials["xsrf"]],
    )


@plugin.operation(
    id="customComponentLibraries.cloneRepository",
    method="POST",
    description="Clone Retool's custom component repository",
)
def custom_component_libraries_clone_repository(input: CliBaseInput) -> OperationResult:
    return _run(["custom-component", "--clone"], input)


@plugin.operation(
    id="terraform.generateConfiguration",
    method="POST",
    description="Generate Terraform configuration for a Retool organization",
)
def terraform_generate_configuration(
    input: CliTerraformInput, req: gestalt.Request
) -> OperationResult:
    if not input.generate_imports and not input.generate_config:
        return _bad_request("generate_imports or generate_config must be true")

    args = ["terraform"]
    if input.generate_imports:
        args.extend(["--imports", _safe_output_filename(input.imports_filename)])
    if input.generate_config:
        args.extend(["--config", _safe_output_filename(input.config_filename)])

    access_token = req.token.strip()
    host = input.host.strip()
    scheme = input.scheme.strip().lower() or "https"
    if not host:
        return _bad_request("host is required")
    if not access_token:
        return _unauthorized("token is required")
    if scheme not in {"https", "http"}:
        return _bad_request("scheme must be either https or http")

    extra_env = {"RETOOL_SCHEME": scheme}
    if access_token:
        extra_env["RETOOL_ACCESS_TOKEN"] = access_token
    if host:
        extra_env["RETOOL_HOST"] = host

    return _run(
        args,
        input,
        extra_env=extra_env,
        redact_values=[access_token],
    )


def _run(
    arguments: list[str],
    input: CliBaseInput,
    *,
    extra_files: dict[str, bytes] | None = None,
    extra_env: dict[str, str] | None = None,
    keyring_credentials: dict[str, Any] | None = None,
    redact_values: list[str] | None = None,
) -> OperationResult:
    try:
        kwargs: dict[str, Any] = {
            "stdin": input.stdin,
            "timeout_seconds": input.timeout_seconds,
            "extra_files": extra_files,
            "extra_env": extra_env,
            "redact_values": redact_values or [],
        }
        if keyring_credentials is not None:
            kwargs["keyring_credentials"] = keyring_credentials
        result = run_retool_cli(arguments, **kwargs)
    except ValueError as err:
        return _bad_request(str(err))
    except RuntimeError as err:
        return gestalt.Response(
            status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": str(err)}
        )

    return CliOutput(
        command=result["command"],
        exit_code=result["exit_code"],
        stdout=result["stdout"],
        stderr=result["stderr"],
        files=[CliFile(**item) for item in result["files"]],
        parsed_json=result["parsed_json"],
    )


def _safe_filename(value: str) -> str:
    filename = value.strip().replace("\\", "/").rsplit("/", 1)[-1]
    if filename in {"", ".", ".."}:
        return "input.csv"
    return filename


def _safe_output_filename(value: str) -> str:
    filename = _safe_filename(value)
    if "/" in filename or "\\" in filename:
        return "output.tf"
    return filename


def _cli_session_credentials(req: gestalt.Request) -> dict[str, Any]:
    connection = req.credential.connection
    if connection != CLI_SESSION_CONNECTION:
        raise ValueError("apps.exportApp must be invoked with _connection=cliSession")

    origin = _connection_param(req, "origin")
    access_token = _connection_param(
        req,
        "access_token",
        "accessToken",
    )
    xsrf_token = _connection_param(
        req,
        "xsrf_token",
        "xsrfToken",
        "xsrf",
    )

    missing = [
        name
        for name, value in (
            ("origin", origin),
            ("access_token", access_token),
            ("xsrf_token", xsrf_token),
        )
        if not value
    ]
    if missing:
        raise ValueError(
            "apps.exportApp requires connected cliSession credentials: "
            + ", ".join(missing)
        )

    normalized_origin = origin.rstrip("/")
    if not normalized_origin.startswith(("https://", "http://")):
        raise ValueError("origin must start with https:// or http://")

    # The official apps export path calls the DB-aware credential helper even
    # though export only needs cookies; these placeholders skip DB discovery.
    return {
        "origin": normalized_origin,
        "xsrf": xsrf_token,
        "accessToken": access_token,
        "gridId": "__gestalt_cli_session__",
        "retoolDBUuid": "__gestalt_cli_session__",
        "hasConnectionString": False,
        "telemetryEnabled": False,
    }


def _connection_param(req: gestalt.Request, *names: str) -> str:
    params = getattr(req, "connection_params", {}) or {}
    for name in names:
        value = params.get(name, "")
        if value:
            return str(value).strip()
    return ""


def _bad_request(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})


def _unauthorized(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.UNAUTHORIZED, body={"error": message})
