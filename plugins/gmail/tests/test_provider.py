import unittest
from http import HTTPStatus
from typing import cast
from unittest import mock

import gestalt

from internals import MIMEParams, build_mime, decode_base64url, ensure_reply_prefix
from internals import operations as operations_module
import provider as provider_module


class GmailProviderTests(unittest.TestCase):
    def test_send_requires_token(self) -> None:
        result = provider_module.messages_send(
            provider_module.SendMessageInput(to="a@b.com", subject="Hi", body="Hi"),
            gestalt.Request(token=""),
        )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(response.body, {"error": "token is required"})

    def test_send_message_wraps_gmail_response(self) -> None:
        with mock.patch.object(operations_module, "post_json", return_value={"id": "msg-new", "threadId": "t-1"}) as post_json:
            result = provider_module.messages_send(
                provider_module.SendMessageInput(
                    to="bob@example.com",
                    subject="Hello",
                    body="Hi Bob",
                ),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(result["data"]["message"]["id"], "msg-new")
        post_json.assert_called_once()
        url, payload, token = post_json.call_args.args
        self.assertTrue(url.endswith("/messages/send"))
        self.assertEqual(token, "test-token")

        decoded = decode_base64url(payload["raw"]).decode("utf-8")
        self.assertIn("To: bob@example.com", decoded)
        self.assertIn("Subject: Hello", decoded)
        self.assertIn("MIME-Version: 1.0", decoded)

    def test_send_message_with_html_builds_multipart_body(self) -> None:
        with mock.patch.object(operations_module, "post_json", return_value={"id": "msg-new"}) as post_json:
            provider_module.messages_send(
                provider_module.SendMessageInput(
                    to="bob@example.com",
                    subject="Hello",
                    body="Hi Bob",
                    html_body="<p>Hi Bob</p>",
                ),
                gestalt.Request(token="test-token"),
            )

        decoded = decode_base64url(post_json.call_args.args[1]["raw"]).decode("utf-8")
        self.assertIn("multipart/alternative", decoded)
        self.assertIn("text/html", decoded)

    def test_create_draft_wraps_gmail_response(self) -> None:
        with mock.patch.object(operations_module, "post_json", return_value={"id": "draft-1", "message": {"id": "msg-1"}}) as post_json:
            result = provider_module.messages_create_draft(
                provider_module.CreateDraftInput(
                    to="bob@example.com",
                    subject="Draft",
                    body="Draft body",
                ),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(result["data"]["draft"]["id"], "draft-1")
        self.assertTrue(post_json.call_args.args[0].endswith("/drafts"))
        self.assertIn("message", post_json.call_args.args[1])

    def test_reply_uses_original_thread_headers(self) -> None:
        original = {
            "threadId": "t-1",
            "payload": {
                "headers": [
                    {"name": "From", "value": "alice@example.com"},
                    {"name": "To", "value": "me@example.com"},
                    {"name": "Subject", "value": "Original Subject"},
                    {"name": "Message-ID", "value": "<msg-id-123@example.com>"},
                    {"name": "References", "value": "<older@example.com>"},
                ]
            },
        }

        with (
            mock.patch.object(operations_module, "get_json", return_value=original) as get_json,
            mock.patch.object(operations_module, "post_json", return_value={"id": "reply-1", "threadId": "t-1"}) as post_json,
        ):
            provider_module.messages_reply(
                provider_module.ReplyMessageInput(
                    message_id="orig-1",
                    body="Thanks!",
                ),
                gestalt.Request(token="test-token"),
            )

        self.assertIn("/messages/orig-1?", get_json.call_args.args[0])
        payload = post_json.call_args.args[1]
        self.assertEqual(payload["threadId"], "t-1")
        decoded = decode_base64url(payload["raw"]).decode("utf-8")
        self.assertIn("Re: Original Subject", decoded)
        self.assertIn("In-Reply-To: <msg-id-123@example.com>", decoded)
        self.assertIn("<older@example.com> <msg-id-123@example.com>", decoded)

    def test_reply_all_filters_delivered_to_address_from_cc(self) -> None:
        original = {
            "threadId": "t-1",
            "payload": {
                "headers": [
                    {"name": "From", "value": "alice@example.com"},
                    {"name": "To", "value": "me@example.com, bob@example.com"},
                    {"name": "Cc", "value": "team@example.com"},
                    {"name": "Subject", "value": "Original Subject"},
                    {"name": "Message-ID", "value": "<msg-id-123@example.com>"},
                    {"name": "Delivered-To", "value": "me@example.com"},
                ]
            },
        }

        with (
            mock.patch.object(operations_module, "get_json", return_value=original),
            mock.patch.object(operations_module, "post_json", return_value={"id": "reply-1"}) as post_json,
        ):
            provider_module.messages_reply(
                provider_module.ReplyMessageInput(
                    message_id="orig-1",
                    body="Thanks!",
                    cc="extra@example.com",
                    reply_all=True,
                ),
                gestalt.Request(token="test-token"),
            )

        decoded = decode_base64url(post_json.call_args.args[1]["raw"]).decode("utf-8")
        self.assertIn("Cc: bob@example.com, team@example.com, extra@example.com", decoded)
        self.assertNotIn("Cc: me@example.com", decoded)

    def test_forward_includes_original_message_body(self) -> None:
        original = {
            "payload": {
                "headers": [
                    {"name": "From", "value": "alice@example.com"},
                    {"name": "Date", "value": "Tue, 7 Apr 2026 10:00:00 -0400"},
                    {"name": "Subject", "value": "Original Subject"},
                ],
                "parts": [
                    {
                        "mimeType": "text/plain",
                        "body": {"data": "SGkgQm9i"},
                    }
                ],
                "mimeType": "multipart/alternative",
            }
        }

        with (
            mock.patch.object(operations_module, "get_json", return_value=original) as get_json,
            mock.patch.object(operations_module, "post_json", return_value={"id": "forward-1"}) as post_json,
        ):
            provider_module.messages_forward(
                provider_module.ForwardMessageInput(
                    message_id="orig-1",
                    to="carol@example.com",
                    additional_text="FYI",
                ),
                gestalt.Request(token="test-token"),
            )

        self.assertIn("/messages/orig-1?format=full", get_json.call_args.args[0])
        decoded = decode_base64url(post_json.call_args.args[1]["raw"]).decode("utf-8")
        self.assertIn("Subject: Fwd: Original Subject", decoded)
        self.assertIn("FYI", decoded)
        self.assertIn("Forwarded message", decoded)
        self.assertIn("Hi Bob", decoded)

    def test_mime_header_injection_is_sanitized(self) -> None:
        raw = build_mime(
            MIMEParams(
                to="user@example.com\r\nBcc: hidden@attacker.com",
                subject="Test\r\nX-Injected: true",
                body="Hello",
            )
        )
        decoded = decode_base64url(raw).decode("utf-8")
        for line in decoded.split("\r\n"):
            self.assertFalse(line.startswith("Bcc: hidden@attacker.com"))
            self.assertFalse(line.startswith("X-Injected: true"))

    def test_mime_plain_text_body_is_not_multipart(self) -> None:
        raw = build_mime(MIMEParams(to="bob@example.com", subject="Test", body="Hello"))
        decoded = decode_base64url(raw).decode("utf-8")
        self.assertIn("MIME-Version: 1.0", decoded)
        self.assertIn("Content-Type: text/plain", decoded)
        self.assertNotIn("multipart", decoded)

    def test_ensure_reply_prefix_does_not_double_prefix(self) -> None:
        self.assertEqual(ensure_reply_prefix("Hello"), "Re: Hello")
        self.assertEqual(ensure_reply_prefix("Re: Hello"), "Re: Hello")


if __name__ == "__main__":
    unittest.main()
