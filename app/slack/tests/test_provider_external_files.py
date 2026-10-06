from __future__ import annotations

import unittest
from typing import Any
from unittest import mock

import gestalt

import provider as provider_module


class SlackExternalFileContentTests(unittest.TestCase):
    """Regression coverage for externally hosted Slack file attachments.

    When Slack file metadata returned by `files.info` (or attached to a
    message in `conversations.replies`) points to a non-Slack host
    (e.g. an externally shared Box / Google Drive link), the Slack
    provider must respond with structured "omitted" content rather than
    bubbling a `SlackClientError` up to the operation dispatcher as a
    500. These tests pin that contract for `slack.files.get` and
    `slack.conversations.getThreadContext`.
    """

    def test_files_get_external_url_omits_content_without_500(self) -> None:
        def fake_slack_get(
            endpoint: str, query: dict[str, str], token: str
        ) -> dict[str, Any]:
            self.assertEqual(endpoint, "files.info")
            self.assertEqual(query, {"file": "F123"})
            self.assertEqual(token, "test-token")
            return {
                "ok": True,
                "file": {
                    "id": "F123",
                    "mimetype": "application/pdf",
                    "url_private": "https://example.box.com/secret.pdf",
                },
            }

        def fake_get_bytes(
            url: str, token: str, max_bytes: int
        ) -> tuple[bytes, bool]:
            raise AssertionError(
                f"get_bytes must not be called for externally hosted file: {url}"
            )

        with (
            mock.patch(
                "internals.operations.slack_get", side_effect=fake_slack_get
            ),
            mock.patch(
                "internals.operations.get_bytes", side_effect=fake_get_bytes
            ),
        ):
            result = provider_module.files_get(
                provider_module.GetFileInput(
                    file_id="F123", include_content=True
                ),
                gestalt.Request(token="test-token"),
            )

        self.assertNotIsInstance(result, gestalt.Response)
        data = result["data"]
        self.assertEqual(data["file"]["id"], "F123")
        self.assertEqual(data["content"]["mime_type"], "application/pdf")
        self.assertEqual(data["content"]["encoding"], "omitted")
        self.assertEqual(
            data["content"]["omitted_reason"], "file is hosted outside Slack"
        )
        self.assertEqual(data["content"]["bytes_read"], 0)
        self.assertFalse(data["content"]["truncated"])

    def test_get_thread_context_omits_external_file_content(self) -> None:
        slack_url = "https://files.slack.com/files-pri/T-F1/notes.txt"
        external_url = "https://example.box.com/external.pdf"

        def fake_slack_get(
            endpoint: str, query: dict[str, str], token: str
        ) -> dict[str, Any]:
            self.assertEqual(endpoint, "conversations.replies")
            self.assertEqual(token, "test-token")
            return {
                "ok": True,
                "messages": [
                    {
                        "ts": "1.0",
                        "user": "U1",
                        "text": "root",
                        "files": [
                            {
                                "id": "F_SLACK",
                                "name": "notes.txt",
                                "mimetype": "text/plain",
                                "url_private": slack_url,
                            },
                            {
                                "id": "F_EXT",
                                "name": "external.pdf",
                                "mimetype": "application/pdf",
                                "url_private": external_url,
                            },
                        ],
                    }
                ],
            }

        def fake_get_bytes(
            url: str, token: str, max_bytes: int
        ) -> tuple[bytes, bool]:
            if url == slack_url:
                return b"hello", False
            raise AssertionError(
                f"get_bytes must not be called for external URL: {url}"
            )

        with (
            mock.patch(
                "internals.operations.slack_get", side_effect=fake_slack_get
            ),
            mock.patch(
                "internals.operations.get_bytes", side_effect=fake_get_bytes
            ),
        ):
            result = provider_module.conversations_get_thread_context(
                provider_module.GetThreadContextInput(
                    channel="C123",
                    ts="1.0",
                    limit=10,
                    include_user_info=False,
                    include_bots=True,
                    include_files=True,
                    include_file_content=True,
                    max_file_bytes=64,
                ),
                gestalt.Request(token="test-token"),
            )

        self.assertNotIsInstance(result, gestalt.Response)
        data = result["data"]
        self.assertEqual(data["file_count"], 2)
        files_by_id = {item["id"]: item for item in data["files"]}
        slack_file = files_by_id["F_SLACK"]
        external_file = files_by_id["F_EXT"]
        self.assertEqual(slack_file["content"]["encoding"], "utf-8")
        self.assertEqual(slack_file["content"]["text"], "hello")
        self.assertEqual(slack_file["content"]["bytes_read"], 5)
        self.assertEqual(external_file["content"]["encoding"], "omitted")
        self.assertEqual(
            external_file["content"]["omitted_reason"],
            "file is hosted outside Slack",
        )
        self.assertEqual(
            external_file["content"]["mime_type"], "application/pdf"
        )
        self.assertEqual(external_file["content"]["bytes_read"], 0)
        self.assertFalse(external_file["content"]["truncated"])


if __name__ == "__main__":
    unittest.main()
