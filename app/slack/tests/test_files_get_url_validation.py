"""Targeted regression tests for Slack ``files.get`` url_private validation.

These tests guard the fix for the gestaltd ``files.get`` 5xx alert: when
``files.info`` returns a ``url_private`` that is not on the Slack download
allowlist, the operation must surface a 4xx (caller-shaped) error rather than
the historical 5xx that paged on-call and encouraged callers to retry.

The tests live in their own module rather than being appended to the much
larger ``tests/test_provider.py`` so the diff stays small and reviewable; they
mirror the existing ``files.get`` test pattern by stubbing ``urllib.request``
at the ``internals.client`` boundary.
"""

from __future__ import annotations

import unittest
import urllib.parse
import urllib.request
from http import HTTPStatus
from typing import Any, cast
from unittest import mock

import gestalt

import internals.client as client_module
import internals.operations as operations_module
import provider as provider_module


class _FakeHTTPResponse:
    """Minimal stand-in for an ``http.client.HTTPResponse`` returned by urlopen."""

    def __init__(self, body: str | bytes, status: int = 200) -> None:
        self._body = body if isinstance(body, bytes) else body.encode("utf-8")
        self.status = status

    def __enter__(self) -> "_FakeHTTPResponse":
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        if size >= 0:
            return self._body[:size]
        return self._body


def _authorization_header(request: urllib.request.Request) -> str | None:
    return request.get_header("Authorization") or dict(request.header_items()).get(
        "Authorization"
    )


class SlackFilesGetUrlValidationTests(unittest.TestCase):
    """Regression coverage for the ``files.get`` url_private allowlist check.

    The motivating alert
    (``gestaltd.operation.error_count gestalt.operation:files.get
    gestalt.provider:slack gestalt.result_status_class:5xx``) fired on three
    file IDs from one subject (F0B6Q3M99GE, F0BAS8EKNBD, F0BBSUZL6CQ) whose
    ``files.info`` response contained a non-Slack ``url_private``.
    """

    def test_files_get_returns_bad_request_for_non_slack_files_info_url_private(
        self,
    ) -> None:
        """Provider-level contract: ``files.info`` returns a non-Slack
        ``url_private`` and ``include_content=True`` must surface 400, never 500.
        """

        call_count = 0

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> _FakeHTTPResponse:
            nonlocal call_count
            call_count += 1
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(_authorization_header(request), "Bearer test-token")
            parsed = urllib.parse.urlsplit(request.full_url)
            self.assertEqual(parsed.path, "/api/files.info")
            return _FakeHTTPResponse(
                """
                {
                  "ok": true,
                  "file": {
                    "id": "F0B6Q3M99GE",
                    "name": "external.bin",
                    "mimetype": "application/octet-stream",
                    "url_private": "https://example.com/files-pri/T-EXT/external.bin",
                    "url_private_download": "https://example.com/files-pri/T-EXT/external.bin"
                  }
                }
                """
            )

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            result = provider_module.files_get(
                provider_module.GetFileInput(
                    file_id="F0B6Q3M99GE",
                    include_content=True,
                    max_bytes=200_000,
                ),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(call_count, 1)
        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.body,
            {"error": "slack file url_private is not a Slack HTTPS file URL"},
        )

    def test_files_get_preserves_non_slack_url_private_when_metadata_only(
        self,
    ) -> None:
        """Negative control: metadata-only consumers
        (``include_content=False``) keep working even when ``files.info``
        returns a non-Slack ``url_private``.
        """

        call_count = 0

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> _FakeHTTPResponse:
            nonlocal call_count
            call_count += 1
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(_authorization_header(request), "Bearer test-token")
            parsed = urllib.parse.urlsplit(request.full_url)
            self.assertEqual(parsed.path, "/api/files.info")
            return _FakeHTTPResponse(
                """
                {
                  "ok": true,
                  "file": {
                    "id": "F0BAS8EKNBD",
                    "name": "external.bin",
                    "mimetype": "application/octet-stream",
                    "url_private": "https://example.com/files-pri/T-EXT/external.bin"
                  }
                }
                """
            )

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            result = provider_module.files_get(
                provider_module.GetFileInput(
                    file_id="F0BAS8EKNBD",
                    include_content=False,
                ),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(call_count, 1)
        self.assertNotIsInstance(result, gestalt.Response)
        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["file"]["id"], "F0BAS8EKNBD")
        self.assertEqual(
            data["file"]["url_private"],
            "https://example.com/files-pri/T-EXT/external.bin",
        )
        self.assertNotIn("content", data)

    def test_get_file_operation_raises_bad_request_for_non_slack_files_info_url(
        self,
    ) -> None:
        """Operations-level contract: ``operations.get_file`` raises
        ``SlackAPIError(BAD_REQUEST)`` rather than reaching ``get_bytes`` for a
        URL that is already known to fail allowlist validation.
        """

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> _FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            parsed = urllib.parse.urlsplit(request.full_url)
            self.assertEqual(parsed.path, "/api/files.info")
            return _FakeHTTPResponse(
                """
                {
                  "ok": true,
                  "file": {
                    "id": "F0BBSUZL6CQ",
                    "name": "external.bin",
                    "mimetype": "application/octet-stream",
                    "url_private_download": "https://example.com/files-pri/T-EXT/external.bin"
                  }
                }
                """
            )

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            with self.assertRaises(client_module.SlackAPIError) as caught:
                operations_module.get_file(
                    "test-token",
                    file_id="F0BBSUZL6CQ",
                    url_private="",
                    include_content=True,
                    max_bytes=200_000,
                )

        self.assertEqual(caught.exception.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            caught.exception.body,
            {"error": "slack file url_private is not a Slack HTTPS file URL"},
        )


if __name__ == "__main__":
    unittest.main()
