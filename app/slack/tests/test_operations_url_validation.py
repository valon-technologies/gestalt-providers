from __future__ import annotations

import unittest
import urllib.parse
import urllib.request
from unittest import mock

import gestalt
import provider as provider_module

from tests.test_provider import FakeHTTPResponse, FakeOpener, authorization_header


class SlackFileDownloadUrlValidationTests(unittest.TestCase):
    """Regression tests for Slack file URL host validation in _download_file_content.

    Both ``files.get`` and ``conversations.getThreadContext`` route through the
    same helper. When a ``url_private`` returned by Slack is not hosted on a
    Slack file host the helper now short-circuits with an ``omitted`` content
    payload instead of letting ``get_bytes`` raise ``SlackClientError`` which
    the provider would mis-classify as a 500.
    """

    def test_files_get_with_non_slack_url_private_omits_content_with_200_contract(
        self,
    ) -> None:
        call_count = 0

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            nonlocal call_count
            call_count += 1

            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(authorization_header(request), "Bearer test-token")
            parsed = urllib.parse.urlsplit(request.full_url)
            query = urllib.parse.parse_qs(parsed.query)

            if call_count == 1:
                self.assertEqual(parsed.path, "/api/files.info")
                self.assertEqual(query, {"file": ["F123"]})
                return FakeHTTPResponse(
                    """
                    {
                      "ok": true,
                      "file": {
                        "id": "F123",
                        "name": "external.txt",
                        "mimetype": "text/plain",
                        "size": 16,
                        "url_private": "https://example.com/foo",
                        "url_private_download": "https://example.com/foo"
                      }
                    }
                    """
                )

            raise AssertionError(f"unexpected request {request.full_url}")

        with (
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch(
                "internals.client.urllib.request.build_opener",
                return_value=FakeOpener(fake_urlopen),
            ),
        ):
            result = provider_module.files_get(
                provider_module.GetFileInput(file_id="F123", include_content=True),
                gestalt.Request(token="test-token"),
            )

        self.assertNotIsInstance(result, gestalt.Response)
        data = result["data"]
        self.assertEqual(call_count, 1)
        self.assertEqual(data["file"]["url_private"], "https://example.com/foo")
        self.assertEqual(data["content"]["mime_type"], "text/plain")
        self.assertEqual(data["content"]["bytes_read"], 0)
        self.assertFalse(data["content"]["truncated"])
        self.assertEqual(data["content"]["encoding"], "omitted")
        self.assertEqual(
            data["content"]["omitted_reason"],
            "url_private is not a Slack HTTPS file URL",
        )

    def test_get_thread_context_with_non_slack_file_url_omits_file_content_with_200_contract(
        self,
    ) -> None:
        call_count = 0

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            nonlocal call_count
            call_count += 1

            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(authorization_header(request), "Bearer test-token")
            parsed = urllib.parse.urlsplit(request.full_url)

            if call_count == 1:
                self.assertEqual(parsed.path, "/api/conversations.replies")
                return FakeHTTPResponse(
                    """
                    {
                      "ok": true,
                      "messages": [
                        {
                          "ts": "1.0",
                          "user": "U1",
                          "text": "parent",
                          "files": [
                            {
                              "id": "F1",
                              "name": "external.txt",
                              "mimetype": "text/plain",
                              "size": 12,
                              "url_private": "https://example.com/foo"
                            }
                          ]
                        }
                      ],
                      "response_metadata": {"next_cursor": ""}
                    }
                    """
                )

            raise AssertionError(f"unexpected request {request.full_url}")

        with (
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch(
                "internals.client.urllib.request.build_opener",
                return_value=FakeOpener(fake_urlopen),
            ),
        ):
            result = provider_module.conversations_get_thread_context(
                provider_module.GetThreadContextInput(
                    channel="C123",
                    ts="1.0",
                    include_files=True,
                    include_file_content=True,
                    include_image_data=False,
                    max_file_bytes=64,
                ),
                gestalt.Request(token="test-token"),
            )

        self.assertNotIsInstance(result, gestalt.Response)
        data = result["data"]
        self.assertEqual(call_count, 1)
        self.assertEqual(data["messages_returned"], 1)
        self.assertEqual(data["participant_count"], 1)
        self.assertEqual(data["file_count"], 1)
        file_content = data["messages"][0]["files"][0]["content"]
        self.assertEqual(file_content["mime_type"], "text/plain")
        self.assertEqual(file_content["bytes_read"], 0)
        self.assertFalse(file_content["truncated"])
        self.assertEqual(file_content["encoding"], "omitted")
        self.assertEqual(
            file_content["omitted_reason"],
            "url_private is not a Slack HTTPS file URL",
        )


if __name__ == "__main__":
    unittest.main()
