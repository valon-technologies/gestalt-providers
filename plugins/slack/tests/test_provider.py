from __future__ import annotations

import io
import unittest
import urllib.error
import urllib.parse
import urllib.request
from email.message import Message
from http import HTTPStatus
from typing import cast
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


def authorization_header(request: urllib.request.Request) -> str | None:
    return request.get_header("Authorization") or dict(request.header_items()).get("Authorization")


class SlackProviderTests(unittest.TestCase):
    def test_post_connect_maps_default_connection_to_external_identity(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(request.full_url, provider_module.SLACK_AUTH_TEST_URL)
            self.assertEqual(authorization_header(request), "Bearer user-token")
            return FakeHTTPResponse(
                """
                {
                  "ok": true,
                  "team_id": "T123ABC456",
                  "user_id": "U123ABC456"
                }
                """
            )

        with mock.patch("provider.urllib.request.urlopen", side_effect=fake_urlopen):
            metadata = provider_module.post_connect(
                gestalt.ConnectedToken(
                    access_token="user-token",
                    connection=provider_module.SLACK_DEFAULT_CONNECTION,
                    subject_id="subject-1",
                )
            )

        self.assertEqual(
            metadata,
            {
                "gestalt.external_identity.type": "slack_identity",
                "gestalt.external_identity.id": "team:T123ABC456:user:U123ABC456",
                "slack.team_id": "T123ABC456",
                "slack.user_id": "U123ABC456",
            },
        )

    def test_post_connect_skips_bot_connection(self) -> None:
        with mock.patch("provider.urllib.request.urlopen") as urlopen:
            metadata = provider_module.post_connect(
                gestalt.ConnectedToken(access_token="bot-token", connection="bot", subject_id="subject-1")
            )

        self.assertEqual(metadata, {})
        urlopen.assert_not_called()

    def test_post_connect_rejects_slack_identity_failure(self) -> None:
        def fake_urlopen(_request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            return FakeHTTPResponse('{"ok": false, "error": "invalid_auth"}')

        with mock.patch("provider.urllib.request.urlopen", side_effect=fake_urlopen):
            with self.assertRaisesRegex(RuntimeError, "slack auth.test failed: invalid_auth"):
                provider_module.post_connect(
                    gestalt.ConnectedToken(
                        access_token="bad-token",
                        connection=provider_module.SLACK_DEFAULT_CONNECTION,
                        subject_id="subject-1",
                    )
                )

    def test_get_message_uses_history_lookup_contract(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(authorization_header(request), "Bearer test-token")

            parsed = urllib.parse.urlsplit(request.full_url)
            self.assertEqual(parsed.path, "/api/conversations.history")
            query = urllib.parse.parse_qs(parsed.query)
            self.assertEqual(
                query,
                {
                    "channel": ["C123ABC456"],
                    "oldest": ["1712161829.000300"],
                    "latest": ["1712161829.000300"],
                    "inclusive": ["true"],
                    "limit": ["1"],
                },
            )

            return FakeHTTPResponse(
                """
                {
                  "ok": true,
                  "messages": [
                    {"ts": "1712161829.000300", "text": "hello", "user": "U123"}
                  ]
                }
                """
            )

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=fake_urlopen):
            result = provider_module.conversations_get_message(
                provider_module.GetMessageInput(
                    url="https://valon.slack.com/archives/C123ABC456/p1712161829000300"
                ),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(result["data"]["message"]["ts"], "1712161829.000300")
        self.assertEqual(result["data"]["message"]["text"], "hello")

    def test_find_user_mentions_uses_history_contract(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(authorization_header(request), "Bearer test-token")

            parsed = urllib.parse.urlsplit(request.full_url)
            self.assertEqual(parsed.path, "/api/conversations.history")
            query = urllib.parse.parse_qs(parsed.query)
            self.assertEqual(
                query,
                {
                    "channel": ["C123"],
                    "limit": ["25"],
                    "oldest": ["100.0"],
                    "latest": ["200.0"],
                },
            )

            return FakeHTTPResponse(
                """
                {
                  "ok": true,
                  "messages": [
                    {"ts":"101.0","user":"UPOSTER1","text":"hello <@UKEEP123>"},
                    {"ts":"102.0","user":"UPOSTER2","text":"again <@UKEEP123> <@UOTHER999>"},
                    {"ts":"103.0","user":"UPOSTER3","bot_id":"B123","text":"bot <@UKEEP123>"},
                    {"ts":"104.0","user":"UPOSTER4","text":"no mention"}
                  ]
                }
                """
            )

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=fake_urlopen):
            result = provider_module.conversations_find_user_mentions(
                provider_module.FindUserMentionsInput(
                    channel="C123",
                    user_id="UKEEP123",
                    limit=25,
                    oldest="100.0",
                    latest="200.0",
                    include_bots=False,
                ),
                gestalt.Request(token="test-token"),
            )

        data = result["data"]
        self.assertEqual(data["total_mentions"], 2)
        self.assertEqual(data["messages_scanned"], 4)
        self.assertEqual(data["mentioned_user_ids"], ["UKEEP123"])
        self.assertEqual(len(data["mentions"]), 2)
        self.assertEqual(data["mentions"][0]["message_ts"], "101.0")
        self.assertEqual(data["mentions"][0]["mentioned_by"], "UPOSTER1")

    def test_get_thread_participants_uses_replies_and_users_info_contract(self) -> None:
        call_count = 0

        def fake_urlopen(request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            nonlocal call_count
            call_count += 1

            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(authorization_header(request), "Bearer test-token")

            parsed = urllib.parse.urlsplit(request.full_url)
            query = urllib.parse.parse_qs(parsed.query)

            if call_count == 1:
                self.assertEqual(parsed.path, "/api/conversations.replies")
                self.assertEqual(
                    query,
                    {
                        "channel": ["C123"],
                        "ts": ["1.0"],
                        "limit": ["1000"],
                    },
                )
                return FakeHTTPResponse(
                    """
                    {
                      "ok": true,
                      "messages": [
                        {"ts":"1.0","user":"U1","text":"parent"},
                        {"ts":"2.0","user":"U2","text":"reply"},
                        {"ts":"3.0","user":"U2","text":"reply again"},
                        {"ts":"4.0","user":"U3","bot_id":"B3","text":"bot reply"}
                      ]
                    }
                    """
                )

            if call_count == 2:
                self.assertEqual(parsed.path, "/api/users.info")
                self.assertEqual(query, {"user": ["U1"]})
                return FakeHTTPResponse(
                    """
                    {
                      "ok": true,
                      "user": {"real_name":"Alice","is_bot":false,"profile":{"display_name":"alice"}}
                    }
                    """
                )

            if call_count == 3:
                self.assertEqual(parsed.path, "/api/users.info")
                self.assertEqual(query, {"user": ["U2"]})
                return FakeHTTPResponse(
                    """
                    {
                      "ok": true,
                      "user": {"real_name":"Bob","is_bot":false,"profile":{"display_name":"bob"}}
                    }
                    """
                )

            raise AssertionError(f"unexpected request {request.full_url}")

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=fake_urlopen):
            result = provider_module.conversations_get_thread_participants(
                provider_module.GetThreadParticipantsInput(
                    channel="C123",
                    ts="1.0",
                    include_user_info=True,
                    include_bots=False,
                ),
                gestalt.Request(token="test-token"),
            )

        data = result["data"]
        self.assertEqual(call_count, 3)
        self.assertEqual(data["participant_count"], 2)
        self.assertEqual(data["total_replies"], 3)
        self.assertEqual(len(data["participants"]), 2)

        first = data["participants"][0]
        self.assertEqual(first["user_id"], "U1")
        self.assertTrue(first["is_thread_starter"])

        second = data["participants"][1]
        self.assertEqual(second["message_count"], 2)
        self.assertEqual(second["display_name"], "bob")

    def test_get_message_propagates_slack_api_http_error(self) -> None:
        error = make_http_error(
            "https://slack.com/api/conversations.history?channel=C123&oldest=1234567890.123456&latest=1234567890.123456&inclusive=true&limit=1",
            429,
            '{"ok": false, "error": "rate_limited"}',
        )

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=error):
            result = provider_module.conversations_get_message(
                provider_module.GetMessageInput(channel="C123", ts="1234567890.123456"),
                gestalt.Request(token="test-token"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(
            response.body,
            {"error": 'slack API error (status 429): {"ok": false, "error": "rate_limited"}'},
        )


if __name__ == "__main__":
    unittest.main()
