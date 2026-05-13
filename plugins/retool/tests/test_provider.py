from __future__ import annotations

import json
import pathlib
import subprocess
import sys
import unittest
from http import HTTPStatus
from typing import Any, cast
from unittest import mock

import yaml

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import gestalt

import internals.cli as cli_module
import provider as provider_module


PLUGIN_DIR = pathlib.Path(__file__).resolve().parents[1]


class RetoolOpenAPITests(unittest.TestCase):
    def test_openapi_operations_are_stable_and_explicitly_allowed(self) -> None:
        spec = yaml.safe_load((PLUGIN_DIR / "openapi.yaml").read_text())
        manifest = yaml.safe_load((PLUGIN_DIR / "manifest.yaml").read_text())

        operation_ids: list[str] = []
        for path_item in spec["paths"].values():
            for method, operation in path_item.items():
                if method.lower() not in {"get", "post", "put", "patch", "delete"}:
                    continue
                self.assertFalse(operation.get("deprecated", False))
                operation_id = operation.get("operationId")
                self.assertIsInstance(operation_id, str)
                operation_ids.append(operation_id)

        allowed_operations = set(manifest["spec"]["allowedOperations"])
        self.assertEqual(len(operation_ids), 145)
        self.assertEqual(len(operation_ids), len(set(operation_ids)))
        self.assertEqual(set(operation_ids), allowed_operations)
        self.assertIn("users.listUsers", allowed_operations)
        self.assertIn("sourceControl.deployLatestChanges", allowed_operations)
        self.assertIn("workflows.listWorkflows", allowed_operations)

    def test_manifest_uses_retool_cloud_auth_and_cursor_pagination(self) -> None:
        manifest = yaml.safe_load((PLUGIN_DIR / "manifest.yaml").read_text())
        spec = yaml.safe_load((PLUGIN_DIR / "openapi.yaml").read_text())

        self.assertEqual(spec["servers"], [{"url": "https://api.retool.com/api/v2"}])
        auth = manifest["spec"]["connections"]["default"]["auth"]
        self.assertEqual(auth["type"], "bearer")
        self.assertEqual(auth["credentials"][0]["name"], "token")
        self.assertEqual(manifest["spec"]["defaultConnection"], "default")

        cli_session_auth = manifest["spec"]["connections"]["cliSession"]["auth"]
        self.assertEqual(cli_session_auth["type"], "manual")
        self.assertEqual(
            [credential["name"] for credential in cli_session_auth["credentials"]],
            ["origin", "access_token", "xsrf_token"],
        )

        pagination = manifest["spec"]["pagination"]
        self.assertEqual(pagination["style"], "cursor")
        self.assertEqual(pagination["cursorParam"], "next_token")
        self.assertEqual(pagination["cursor"]["path"], "next_token")
        self.assertEqual(pagination["resultsPath"], "data")

        response_mapping = manifest["spec"]["responseMapping"]["pagination"]
        self.assertEqual(response_mapping["hasMore"]["path"], "has_more")
        self.assertEqual(response_mapping["cursor"]["path"], "next_token")


class RetoolCliProviderTests(unittest.TestCase):
    def test_apps_export_uses_cli_session_cookie_connection_params(self) -> None:
        with mock.patch.object(
            provider_module,
            "run_retool_cli",
            return_value=_cli_result(["retool", "apps", "--export", "Ops dashboard"]),
        ) as run:
            result = provider_module.apps_export_app(
                provider_module.CliAppExportInput(app_name="Ops dashboard"),
                gestalt.Request(
                    credential=gestalt.Credential(connection="cliSession"),
                    connection_params={
                        "origin": "https://example.retool.com/",
                        "access_token": "cookie-access",
                        "xsrf_token": "cookie-xsrf",
                    }
                ),
            )

        self.assertIsInstance(result, provider_module.CliOutput)
        run.assert_called_once_with(
            ["apps", "--export", "Ops dashboard"],
            stdin="",
            timeout_seconds=120,
            extra_files=None,
            extra_env=None,
            redact_values=["cookie-access", "cookie-xsrf"],
            keyring_credentials={
                "origin": "https://example.retool.com",
                "xsrf": "cookie-xsrf",
                "accessToken": "cookie-access",
                "gridId": "__gestalt_cli_session__",
                "retoolDBUuid": "__gestalt_cli_session__",
                "hasConnectionString": False,
                "telemetryEnabled": False,
            },
        )

    def test_apps_export_rejects_missing_cli_session_credentials(self) -> None:
        with mock.patch.object(
            provider_module,
            "run_retool_cli",
            side_effect=AssertionError("unexpected CLI call"),
        ):
            result = provider_module.apps_export_app(
                provider_module.CliAppExportInput(app_name="Ops dashboard"),
                gestalt.Request(
                    credential=gestalt.Credential(connection="cliSession"),
                    connection_params={"origin": "https://example.retool.com"}
                ),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.body,
            {
                "error": (
                    "apps.exportApp requires connected cliSession credentials: "
                    "access_token, xsrf_token"
                )
            },
        )

    def test_apps_export_rejects_default_connection(self) -> None:
        with mock.patch.object(
            provider_module,
            "run_retool_cli",
            side_effect=AssertionError("unexpected CLI call"),
        ):
            result = provider_module.apps_export_app(
                provider_module.CliAppExportInput(app_name="Ops dashboard"),
                gestalt.Request(
                    credential=gestalt.Credential(connection="default"),
                    connection_params={
                        "origin": "https://example.retool.com/",
                        "access_token": "cookie-access",
                        "xsrf_token": "cookie-xsrf",
                    },
                ),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.body,
            {
                "error": (
                    "apps.exportApp must be invoked with _connection=cliSession"
                )
            },
        )

    def test_custom_component_clone_builds_allowlisted_command(self) -> None:
        with mock.patch.object(
            provider_module,
            "run_retool_cli",
            return_value=_cli_result(["retool", "custom-component", "--clone"]),
        ) as run:
            result = provider_module.custom_component_libraries_clone_repository(
                provider_module.CliBaseInput()
            )

        self.assertIsInstance(result, provider_module.CliOutput)
        run.assert_called_once_with(
            ["custom-component", "--clone"],
            stdin="",
            timeout_seconds=120,
            extra_files=None,
            extra_env=None,
            redact_values=[],
        )

    def test_terraform_uses_token_from_request_and_returns_output_files(self) -> None:
        with mock.patch.object(
            provider_module,
            "run_retool_cli",
            return_value=_cli_result(
                [
                    "retool",
                    "terraform",
                    "--imports",
                    "retool-imports.tf",
                    "--config",
                    "retool-config.tf",
                ]
            ),
        ) as run:
            result = provider_module.terraform_generate_configuration(
                provider_module.CliTerraformInput(host="example.retool.com"),
                gestalt.Request(token="api-token"),
            )

        self.assertIsInstance(result, provider_module.CliOutput)
        run.assert_called_once_with(
            [
                "terraform",
                "--imports",
                "retool-imports.tf",
                "--config",
                "retool-config.tf",
            ],
            stdin="",
            timeout_seconds=120,
            extra_files=None,
            extra_env={
                "RETOOL_SCHEME": "https",
                "RETOOL_ACCESS_TOKEN": "api-token",
                "RETOOL_HOST": "example.retool.com",
            },
            redact_values=["api-token"],
        )

    def test_terraform_rejects_invalid_scheme(self) -> None:
        with mock.patch.object(
            provider_module,
            "run_retool_cli",
            side_effect=AssertionError("unexpected CLI call"),
        ):
            result = provider_module.terraform_generate_configuration(
                provider_module.CliTerraformInput(
                    host="example.retool.com",
                    scheme="ftp",
                ),
                gestalt.Request(token="api-token"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.body, {"error": "scheme must be either https or http"}
        )

    def test_terraform_rejects_missing_host(self) -> None:
        with mock.patch.object(
            provider_module,
            "run_retool_cli",
            side_effect=AssertionError("unexpected CLI call"),
        ):
            result = provider_module.terraform_generate_configuration(
                provider_module.CliTerraformInput(),
                gestalt.Request(token="api-token"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(response.body, {"error": "host is required"})

    def test_terraform_rejects_missing_token(self) -> None:
        with mock.patch.object(
            provider_module,
            "run_retool_cli",
            side_effect=AssertionError("unexpected CLI call"),
        ):
            result = provider_module.terraform_generate_configuration(
                provider_module.CliTerraformInput(host="example.retool.com"),
                gestalt.Request(token=""),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(response.body, {"error": "token is required"})

    def test_catalog_lists_only_plugin_sensible_retool_cli_commands_without_yaml_aliases(
        self,
    ) -> None:
        text = (PLUGIN_DIR / "catalog.yaml").read_text()
        self.assertNotIn("&id", text)
        self.assertNotIn("*id", text)

        catalog = yaml.safe_load(text)
        operations = {
            operation["id"]: operation for operation in catalog["operations"]
        }
        ids = set(operations)
        self.assertFalse(any(operation_id.startswith("cli.") for operation_id in ids))
        self.assertEqual(
            ids,
            {
                "apps.exportApp",
                "customComponentLibraries.cloneRepository",
                "terraform.generateConfiguration",
            },
        )
        self.assertEqual(
            [param["name"] for param in operations["apps.exportApp"]["parameters"]],
            ["stdin", "timeout_seconds", "app_name"],
        )
        self.assertEqual(
            [
                param["name"]
                for param in operations["terraform.generateConfiguration"]["parameters"]
            ],
            [
                "stdin",
                "timeout_seconds",
                "generate_imports",
                "generate_config",
                "imports_filename",
                "config_filename",
                "host",
                "scheme",
            ],
        )
        terraform_host = next(
            param
            for param in operations["terraform.generateConfiguration"]["parameters"]
            if param["name"] == "host"
        )
        self.assertTrue(terraform_host["required"])

        excluded_cli_ids = {
            "cli.apps.export",
            "cli.customComponent.clone",
            "cli.terraform.generate",
            "cli.login",
            "cli.logout",
            "cli.signup",
            "cli.whoami",
            "cli.telemetry.disable",
            "cli.telemetry.enable",
            "cli.apps.create",
            "cli.apps.createFromTable",
            "cli.apps.delete",
            "cli.apps.list",
            "cli.apps.listRecursive",
            "cli.database.create",
            "cli.database.delete",
            "cli.database.fromPostgres",
            "cli.database.gendata",
            "cli.database.list",
            "cli.database.upload",
            "cli.scaffold.columns",
            "cli.scaffold.delete",
            "cli.scaffold.fromCsv",
            "cli.scaffold.name",
            "cli.rpc",
            "cli.workflows.delete",
            "cli.workflows.list",
            "cli.workflows.listRecursive",
        }
        self.assertTrue(ids.isdisjoint(excluded_cli_ids))

        spec = yaml.safe_load((PLUGIN_DIR / "openapi.yaml").read_text())
        openapi_ids = {
            operation["operationId"]
            for path_item in spec["paths"].values()
            for method, operation in path_item.items()
            if method.lower() in {"get", "post", "put", "patch", "delete"}
        }
        self.assertTrue(
            {
                "apps.listApps",
                "apps.deleteApp",
                "users.createUser",
                "users.logOutUser",
                "workflows.listWorkflows",
                "workflows.deleteWorkflow",
            }.issubset(openapi_ids)
        )


class RetoolCliRuntimeTests(unittest.TestCase):
    def test_run_retool_cli_captures_stdout_json_and_generated_files(self) -> None:
        def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
            work_dir = pathlib.Path(kwargs["cwd"])
            home_dir = pathlib.Path(kwargs["env"]["HOME"])
            guide_dir = work_dir / "custom-component-guide"
            guide_dir.mkdir()
            (guide_dir / "README.md").write_text("guide")
            (guide_dir / ".git").mkdir()
            (guide_dir / ".git" / "config").write_text("git internals")
            (guide_dir / "node_modules" / "pkg").mkdir(parents=True)
            (guide_dir / "node_modules" / "pkg" / "index.js").write_text("module")
            (work_dir / "inputs" / "ignored.csv").parent.mkdir(exist_ok=True)
            (work_dir / "inputs" / "ignored.csv").write_text("id\n1\n")
            (home_dir / ".retool").mkdir()
            (home_dir / ".retool" / "credentials.json").write_text(
                '{"accessToken":"secret"}'
            )
            return subprocess.CompletedProcess(
                command,
                0,
                stdout='{"ok": true}',
                stderr="",
            )

        with (
            mock.patch.object(cli_module, "_resolve_retool_command", return_value=["retool"]),
            mock.patch.object(cli_module.subprocess, "run", side_effect=fake_run) as run,
        ):
            result = cli_module.run_retool_cli(
                ["custom-component", "--clone"],
                extra_files={"inputs/ignored.csv": b"id\n1\n"},
            )

        self.assertEqual(result["command"], ["retool", "custom-component", "--clone"])
        self.assertEqual(result["parsed_json"], {"ok": True})
        self.assertEqual(
            result["files"],
            [
                {
                    "path": "custom-component-guide/README.md",
                    "encoding": "utf-8",
                    "content": "guide",
                    "content_base64": "",
                    "size_bytes": 5,
                }
            ],
        )
        self.assertEqual(run.call_args.kwargs["input"], "")
        self.assertEqual(run.call_args.kwargs["timeout"], 120)
        self.assertEqual(run.call_args.kwargs["env"]["PUPPETEER_SKIP_DOWNLOAD"], "true")

    def test_run_retool_cli_redacts_secret_command_values_and_sets_extra_env(
        self,
    ) -> None:
        def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
            self.assertEqual(kwargs["env"]["RETOOL_ACCESS_TOKEN"], "secret-token")
            return subprocess.CompletedProcess(
                command,
                0,
                stdout='{"token":"secret-token"}',
                stderr="using secret-token",
            )

        with (
            mock.patch.object(cli_module, "_resolve_retool_command", return_value=["retool"]),
            mock.patch.object(cli_module.subprocess, "run", side_effect=fake_run),
        ):
            result = cli_module.run_retool_cli(
                ["terraform", "--config", "secret.tf"],
                extra_env={"RETOOL_ACCESS_TOKEN": "secret-token"},
                redact_values=["secret.tf", "secret-token"],
            )

        self.assertEqual(
            result["command"],
            ["retool", "terraform", "--config", "[redacted]"],
        )
        self.assertEqual(result["stdout"], '{"token":"[redacted]"}')
        self.assertEqual(result["stderr"], "using [redacted]")
        self.assertEqual(result["parsed_json"], {"token": "[redacted]"})

    def test_run_retool_cli_installs_ephemeral_keyring_shim(self) -> None:
        def fake_run(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
            env = kwargs["env"]
            self.assertIn("--require=", env["NODE_OPTIONS"])
            shim_path = pathlib.Path(
                env["NODE_OPTIONS"].split("--require=", 1)[1].split()[0]
            )
            self.assertTrue(shim_path.is_file())
            self.assertIn("@napi-rs/keyring", shim_path.read_text())

            payload = json.loads(env["RETOOL_CLI_KEYRING_PASSWORD"])
            self.assertEqual(payload["origin"], "https://example.retool.com")
            self.assertEqual(payload["accessToken"], "secret-access")
            self.assertEqual(payload["xsrf"], "secret-xsrf")
            return subprocess.CompletedProcess(
                command,
                1,
                stdout="secret-access",
                stderr="x-xsrf-token secret-xsrf cookie accessToken=secret-access;",
            )

        with (
            mock.patch.object(cli_module, "_resolve_retool_command", return_value=["retool"]),
            mock.patch.object(cli_module.subprocess, "run", side_effect=fake_run),
        ):
            result = cli_module.run_retool_cli(
                ["apps", "--export", "Ops dashboard"],
                keyring_credentials={
                    "origin": "https://example.retool.com",
                    "accessToken": "secret-access",
                    "xsrf": "secret-xsrf",
                },
                redact_values=["secret-access", "secret-xsrf"],
            )

        self.assertEqual(result["stdout"], "[redacted]")
        self.assertEqual(
            result["stderr"],
            "x-xsrf-token [redacted] cookie accessToken=[redacted];",
        )

    def test_run_retool_cli_uses_npx_when_retool_binary_is_missing(self) -> None:
        with (
            mock.patch.dict(cli_module.os.environ, {}, clear=True),
            mock.patch.object(
                cli_module.shutil,
                "which",
                side_effect=lambda name: "/usr/bin/npx" if name == "npx" else None,
            ),
        ):
            self.assertEqual(
                cli_module._resolve_retool_command(),
                [
                    "/usr/bin/npx",
                    "--yes",
                    "--package",
                    "retool-cli@1.0.29",
                    "--",
                    "retool",
                ],
            )

    def test_run_retool_cli_rejects_unsafe_extra_file_paths(self) -> None:
        with mock.patch.object(cli_module, "_resolve_retool_command", return_value=["retool"]):
            with self.assertRaisesRegex(ValueError, "unsafe file path"):
                cli_module.run_retool_cli(
                    ["custom-component", "--clone"],
                    extra_files={"../users.csv": b"id\n"},
                )


def _cli_result(command: list[str]) -> dict[str, Any]:
    return {
        "command": command,
        "exit_code": 0,
        "stdout": "",
        "stderr": "",
        "files": [],
        "parsed_json": None,
    }
