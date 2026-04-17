from __future__ import annotations

import io
import json
import unittest
import urllib.error
import urllib.request
from email.message import Message
from http import HTTPStatus
from typing import Any, cast
from unittest import mock

import gestalt

import provider as provider_module


class FakeHTTPResponse:
    def __init__(self, body: str) -> None:
        self._body = body.encode("utf-8")

    def __enter__(self) -> FakeHTTPResponse:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def make_http_error(url: str, status: int, body: str) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        url=url,
        code=status,
        msg="error",
        hdrs=Message(),
        fp=io.BytesIO(body.encode("utf-8")),
    )


def header_value(request: urllib.request.Request, name: str) -> str | None:
    direct = request.get_header(name)
    if direct is not None:
        return direct

    lowered = name.lower()
    for key, value in request.header_items():
        if key.lower() == lowered:
            return value
    return None


def request_json_body(request: urllib.request.Request) -> dict[str, Any]:
    data = request.data
    if not isinstance(data, bytes):
        raise AssertionError(f"expected bytes request body, got {type(data)!r}")
    payload = json.loads(data.decode("utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError(f"expected object request body, got {type(payload)!r}")
    return payload


class HexProviderTests(unittest.TestCase):
    def test_project_export_requires_token(self) -> None:
        result = provider_module.project_export(
            provider_module.ProjectExportInput(project_id="proj-1"),
            gestalt.Request(token=""),
        )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, Any]], result)
        self.assertEqual(response.status, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(response.body, {"error": "token is required"})

    def test_project_export_uses_cli_endpoint_contract(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(request.full_url, "https://app.hex.tech/api/v1/projects/export")
            self.assertEqual(header_value(request, "Authorization"), "Bearer test-token")
            self.assertEqual(header_value(request, "api-version"), "1.0.0")

            payload = request_json_body(request)
            self.assertEqual(payload, {"projectId": "proj-1", "version": "draft"})

            return FakeHTTPResponse('{"content":"projectId: proj-1\\n","filename":"project.yaml"}')

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=fake_urlopen):
            result = provider_module.project_export(
                provider_module.ProjectExportInput(project_id="proj-1"),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(result["content"], "projectId: proj-1\n")
        self.assertEqual(result["filename"], "project.yaml")

    def test_project_export_coerces_numeric_version(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            payload = request_json_body(request)
            self.assertEqual(payload, {"projectId": "proj-1", "version": 2})
            return FakeHTTPResponse('{"content":"ok","filename":"project.yaml"}')

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=fake_urlopen):
            provider_module.project_export(
                provider_module.ProjectExportInput(project_id="proj-1", version="2"),
                gestalt.Request(token="test-token"),
            )

    def test_project_import_uses_cli_endpoint_contract(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(request.full_url, "https://app.hex.tech/api/v1/projects/import")

            payload = request_json_body(request)
            self.assertEqual(payload, {"content": "title: My Project\n"})

            return FakeHTTPResponse(
                '{"projectId":"proj-1","hexVersionId":"ver-1","warnings":{"projectTitle":"Title mismatch"}}'
            )

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=fake_urlopen):
            result = provider_module.project_import(
                provider_module.ProjectImportInput(content="title: My Project\n"),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(result["projectId"], "proj-1")
        self.assertEqual(result["hexVersionId"], "ver-1")
        self.assertEqual(result["warnings"]["projectTitle"], "Title mismatch")

    def test_project_run_draft_omits_cache_flag_when_unspecified(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(request.full_url, "https://app.hex.tech/api/v1/projects/proj-1/notebook/run")
            payload = request_json_body(request)
            self.assertEqual(payload, {})
            return FakeHTTPResponse(
                '{"projectId":"proj-1","runId":"run-1","status":"PENDING","traceId":"trace-1","url":"https://app.hex.tech/run/1"}'
            )

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=fake_urlopen):
            result = provider_module.project_run_draft(
                provider_module.ProjectRunDraftInput(project_id="proj-1"),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(result["runId"], "run-1")
        self.assertEqual(result["status"], "PENDING")

    def test_project_run_draft_includes_cache_flag_when_specified(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            payload = request_json_body(request)
            self.assertEqual(payload, {"useCachedSqlResults": False})
            return FakeHTTPResponse(
                '{"projectId":"proj-1","runId":"run-1","status":"RUNNING","traceId":"trace-1","url":"https://app.hex.tech/run/1"}'
            )

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=fake_urlopen):
            provider_module.project_run_draft(
                provider_module.ProjectRunDraftInput(project_id="proj-1", use_cached_sql_results=False),
                gestalt.Request(token="test-token"),
            )

    def test_cell_run_uses_cli_endpoint_contract(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(request.full_url, "https://app.hex.tech/api/v1/cells/cell-1/run")
            payload = request_json_body(request)
            self.assertEqual(payload, {"dryRun": True})
            return FakeHTTPResponse(
                '{"cellId":"cell-1","dryRun":true,"runId":null,"runStatusUrl":null,"runUrl":null,"status":"PENDING","traceId":"trace-2"}'
            )

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=fake_urlopen):
            result = provider_module.cell_run(
                provider_module.CellRunInput(cell_id="cell-1", dry_run=True),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(result["cellId"], "cell-1")
        self.assertTrue(result["dryRun"])

    def test_hex_api_errors_preserve_status_and_payload(self) -> None:
        error = make_http_error(
            "https://app.hex.tech/api/v1/projects/export",
            403,
            '{"reason":"Forbidden","details":"Missing permission","traceId":"trace-err"}',
        )

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=error):
            result = provider_module.project_export(
                provider_module.ProjectExportInput(project_id="proj-1"),
                gestalt.Request(token="test-token"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, Any]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertEqual(
            response.body,
            {
                "error": "Forbidden: Missing permission",
                "reason": "Forbidden",
                "details": "Missing permission",
                "traceId": "trace-err",
            },
        )

    def test_project_export_rejects_invalid_version(self) -> None:
        result = provider_module.project_export(
            provider_module.ProjectExportInput(project_id="proj-1", version="not-a-version"),
            gestalt.Request(token="test-token"),
        )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, Any]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(response.body, {"error": "version must be 'draft' or a version number"})


if __name__ == "__main__":
    unittest.main()
