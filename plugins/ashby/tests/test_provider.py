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


class AshbyProviderTests(unittest.TestCase):
    def test_list_rejects_empty_token(self) -> None:
        result = provider_module.application_list(
            provider_module.ListInput(),
            gestalt.Request(token="  "),
        )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(response.body, {"error": "token is required"})

    def test_list_passes_limit_and_cursor_to_api(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")

            data = request.data
            self.assertIsInstance(data, bytes)
            body: dict[str, Any] = json.loads(cast(bytes, data))
            self.assertEqual(body, {"limit": 50, "cursor": "abc123"})
            self.assertTrue(request.full_url.endswith("/application.list"))

            auth = request.get_header("Authorization") or ""
            self.assertTrue(auth.startswith("Basic "))

            return FakeHTTPResponse(
                json.dumps({"success": True, "results": [{"id": "1"}]})
            )

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=fake_urlopen):
            result = provider_module.application_list(
                provider_module.ListInput(limit=50, cursor="abc123"),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(result["data"], [{"id": "1"}])

    def test_list_omits_null_limit_and_empty_cursor(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            body: dict[str, Any] = json.loads(cast(bytes, request.data))
            self.assertEqual(body, {})

            return FakeHTTPResponse(
                json.dumps({"success": True, "results": []})
            )

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=fake_urlopen):
            result = provider_module.application_list(
                provider_module.ListInput(),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(result["data"], [])

    def test_list_returns_results_with_pagination(self) -> None:
        response_body = json.dumps({
            "success": True,
            "results": [{"id": "1"}, {"id": "2"}],
            "moreDataAvailable": True,
            "nextCursor": "xyz",
        })

        with mock.patch(
            "internals.client.urllib.request.urlopen",
            return_value=FakeHTTPResponse(response_body),
        ):
            result = provider_module.application_list(
                provider_module.ListInput(),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(result["data"], [{"id": "1"}, {"id": "2"}])
        self.assertEqual(result["pagination"], {"has_more": True, "cursor": "xyz"})

    def test_list_returns_results_without_pagination(self) -> None:
        response_body = json.dumps({
            "success": True,
            "results": [{"id": "1"}],
        })

        with mock.patch(
            "internals.client.urllib.request.urlopen",
            return_value=FakeHTTPResponse(response_body),
        ):
            result = provider_module.application_list(
                provider_module.ListInput(),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(result["data"], [{"id": "1"}])
        self.assertIsNone(result["pagination"])

    def test_list_returns_bad_gateway_on_http_error(self) -> None:
        error = urllib.error.HTTPError(
            url="https://api.ashbyhq.com/application.list",
            code=500,
            msg="error",
            hdrs=Message(),
            fp=None,
        )

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=error):
            result = provider_module.application_list(
                provider_module.ListInput(),
                gestalt.Request(token="test-token"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, 500)

    def test_list_returns_bad_gateway_on_upstream_failure(self) -> None:
        response_body = json.dumps({"success": False, "error": "internal error"})

        with mock.patch(
            "internals.client.urllib.request.urlopen",
            return_value=FakeHTTPResponse(response_body),
        ):
            result = provider_module.application_list(
                provider_module.ListInput(),
                gestalt.Request(token="test-token"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_GATEWAY)


if __name__ == "__main__":
    unittest.main()
