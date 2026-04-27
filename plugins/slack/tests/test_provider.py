from __future__ import annotations

import io
import unittest
import urllib.error
import urllib.parse
import urllib.request
from email.message import Message
from http import HTTPStatus
from typing import Any, cast
from unittest import mock

import gestalt
from google.protobuf import json_format
from gestalt.gen.v1 import agent_pb2 as _agent_pb2
from gestalt.gen.v1 import authorization_pb2 as _authorization_pb2

import provider as provider_module

agent_pb2: Any = _agent_pb2
authorization_pb2: Any = _authorization_pb2


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
    return request.get_header("Authorization") or dict(request.header_items()).get(
        "Authorization"
    )


class FakeAuthorization:
    def __init__(self, subjects: list[Any]) -> None:
        self.subjects = subjects
        self.requests: list[Any] = []

    def search_subjects(self, request: Any) -> Any:
        self.requests.append(request)
        return authorization_pb2.SubjectSearchResponse(subjects=self.subjects)


class FakeAgentManager:
    def __init__(self) -> None:
        self.session_requests: list[Any] = []
        self.turn_requests: list[Any] = []
        self.requests = self.turn_requests

    def __enter__(self) -> FakeAgentManager:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def create_session(self, request: Any) -> Any:
        self.session_requests.append(request)
        return agent_pb2.AgentSession(
            id="session-123",
            provider_name=request.provider_name or "simple",
            model=request.model,
            client_ref=request.client_ref,
            state=agent_pb2.AGENT_SESSION_STATE_ACTIVE,
        )

    def create_turn(self, request: Any) -> Any:
        self.turn_requests.append(request)
        return agent_pb2.AgentTurn(
            id="turn-123",
            session_id=request.session_id,
            provider_name="simple",
            model=request.model,
            status=agent_pb2.AGENT_EXECUTION_STATUS_RUNNING,
        )


class SlackProviderTests(unittest.TestCase):
    def test_post_connect_maps_default_connection_to_external_identity(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
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
                gestalt.ConnectedToken(
                    access_token="bot-token", connection="bot", subject_id="subject-1"
                )
            )

        self.assertEqual(metadata, {})
        urlopen.assert_not_called()

    def test_post_connect_rejects_slack_identity_failure(self) -> None:
        def fake_urlopen(
            _request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            return FakeHTTPResponse('{"ok": false, "error": "invalid_auth"}')

        with mock.patch("provider.urllib.request.urlopen", side_effect=fake_urlopen):
            with self.assertRaisesRegex(
                RuntimeError, "slack auth.test failed: invalid_auth"
            ):
                provider_module.post_connect(
                    gestalt.ConnectedToken(
                        access_token="bad-token",
                        connection=provider_module.SLACK_DEFAULT_CONNECTION,
                        subject_id="subject-1",
                    )
                )

    def test_http_subject_resolves_slack_user_through_managed_external_identity(
        self,
    ) -> None:
        subject = authorization_pb2.Subject(type="user", id="user:gestalt-123")
        subject.properties.update({"email": "ada@example.com"})
        authorization = FakeAuthorization([subject])
        payload = {
            "type": "event_callback",
            "event_id": "Ev123",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C789",
                "text": "<@UBOT> hello",
                "ts": "1712161829.000300",
            },
        }

        with mock.patch.object(
            gestalt.Request, "authorization", return_value=authorization
        ):
            resolved = provider_module.resolve_http_subject(
                gestalt.HTTPSubjectRequest(params=payload),
                gestalt.Request(),
            )

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved.id, "user:gestalt-123")
        self.assertEqual(resolved.kind, "user")
        self.assertEqual(resolved.display_name, "ada@example.com")

        self.assertEqual(len(authorization.requests), 1)
        request = authorization.requests[0]
        self.assertEqual(request.resource.type, "external_identity")
        self.assertEqual(
            request.resource.id,
            provider_module.external_identity_resource_id(
                "slack_identity",
                "team:T123:user:U456",
            ),
        )
        self.assertEqual(request.action.name, "assume")
        self.assertEqual(request.subject_type, "user")

    def test_slack_event_handler_starts_agent_run_with_inherited_tools(self) -> None:
        provider_module.configure(
            "slack",
            {
                "agentProvider": "simple",
                "agentModel": "deep",
                "agentProviderOptions": {"temperature": 0},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        agent_manager = FakeAgentManager()
        payload = {
            "type": "event_callback",
            "event_id": "Ev123",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> summarize deploy status",
                "ts": "1712161829.000300",
            },
        }
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )

        with mock.patch.object(
            gestalt.Request, "agent_manager", return_value=agent_manager
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(response["ok"], True)
        self.assertEqual(response["agent_session_id"], "session-123")
        self.assertEqual(response["agent_turn_id"], "turn-123")
        self.assertEqual(response["status"], "AGENT_EXECUTION_STATUS_RUNNING")
        self.assertEqual(len(agent_manager.session_requests), 1)
        self.assertEqual(len(agent_manager.turn_requests), 1)

        session_request = agent_manager.session_requests[0]
        self.assertEqual(session_request.provider_name, "simple")
        self.assertEqual(session_request.model, "deep")
        self.assertEqual(
            session_request.client_ref, "slack:T123:C789:1712161829.000300"
        )
        self.assertEqual(
            session_request.idempotency_key,
            "slack:session:slack:T123:C789:1712161829.000300",
        )
        session_metadata = json_format.MessageToDict(session_request.metadata)
        self.assertEqual(session_metadata["slack"]["team_id"], "T123")
        self.assertEqual(session_metadata["slack"]["channel_id"], "C789")
        self.assertEqual(
            session_metadata["slack"]["root_message_ts"], "1712161829.000300"
        )
        self.assertNotIn("event_id", session_metadata["slack"])
        self.assertNotIn("user_id", session_metadata["slack"])
        self.assertNotIn("reply_thread_ts", session_metadata["slack"])
        self.assertEqual(json_format.MessageToDict(session_request.provider_options), {})

        turn_request = agent_manager.turn_requests[0]
        self.assertEqual(turn_request.session_id, "session-123")
        self.assertEqual(turn_request.model, "deep")
        self.assertEqual(
            turn_request.tool_source, agent_pb2.AGENT_TOOL_SOURCE_MODE_INHERIT_INVOKES
        )
        self.assertEqual(turn_request.idempotency_key, "slack:event:Ev123")
        self.assertIn("slack.chat.postMessage", turn_request.messages[0].text)
        self.assertIn(
            "reply_thread_ts: 1712161829.000300", turn_request.messages[1].text
        )

        metadata = json_format.MessageToDict(turn_request.metadata)
        self.assertEqual(metadata["slack"]["team_id"], "T123")
        self.assertEqual(metadata["slack"]["user_id"], "U456")
        self.assertEqual(metadata["slack"]["reply_thread_ts"], "1712161829.000300")
        provider_options = json_format.MessageToDict(turn_request.provider_options)
        self.assertEqual(provider_options["temperature"], 0)

    def test_nested_agent_config_selects_route_by_channel(self) -> None:
        provider_module.configure(
            "supportSlackbot",
            {
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "systemPrompt": "Follow the global Slack policy.",
                    "providerOptions": {"temperature": 0},
                    "routes": [
                        {
                            "id": "triage",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventType": "message",
                            },
                            "agent": {
                                "prompt": "Triage support requests.",
                                "providerOptions": {"max_output_tokens": 2000},
                            },
                        }
                    ],
                }
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        agent_manager = FakeAgentManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvRoute",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C_SUPPORT",
                "channel_type": "channel",
                "text": "please triage this",
                "ts": "1712161829.000300",
            },
        }
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )

        with mock.patch.object(
            gestalt.Request, "agent_manager", return_value=agent_manager
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(response["ok"], True)
        self.assertEqual(len(agent_manager.session_requests), 1)
        self.assertEqual(len(agent_manager.turn_requests), 1)
        session_request = agent_manager.session_requests[0]
        turn_request = agent_manager.turn_requests[0]
        self.assertEqual(session_request.provider_name, "simple")
        self.assertEqual(session_request.model, "deep")
        self.assertEqual(turn_request.model, "deep")
        self.assertIn("supportSlackbot.chat.postMessage", turn_request.messages[0].text)
        self.assertIn("Follow the global Slack policy.", turn_request.messages[0].text)
        self.assertIn("Triage support requests.", turn_request.messages[0].text)

        metadata = json_format.MessageToDict(turn_request.metadata)
        self.assertEqual(metadata["slack"]["agent_route_id"], "triage")
        provider_options = json_format.MessageToDict(turn_request.provider_options)
        self.assertEqual(provider_options["temperature"], 0)
        self.assertEqual(provider_options["max_output_tokens"], 2000)

    def test_repeated_slack_events_reuse_session_key_but_keep_event_metadata_on_turns(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {"agent": {"provider": "simple", "model": "deep"}},
        )
        self.addCleanup(provider_module.configure, "slack", {})
        agent_manager = FakeAgentManager()
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )
        first = {
            "type": "event_callback",
            "event_id": "EvFirst",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> first",
                "ts": "1712161829.000300",
            },
        }
        second = {
            "type": "event_callback",
            "event_id": "EvSecond",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U999",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> follow up",
                "ts": "1712161835.000400",
                "thread_ts": "1712161829.000300",
            },
        }

        with mock.patch.object(
            gestalt.Request, "agent_manager", return_value=agent_manager
        ):
            provider_module.slack_events_handle(first, request)
            provider_module.slack_events_handle(second, request)

        self.assertEqual(len(agent_manager.session_requests), 2)
        self.assertEqual(len(agent_manager.turn_requests), 2)
        self.assertEqual(
            agent_manager.session_requests[0].idempotency_key,
            agent_manager.session_requests[1].idempotency_key,
        )
        for session_request in agent_manager.session_requests:
            session_metadata = json_format.MessageToDict(session_request.metadata)
            self.assertEqual(
                session_metadata["slack"]["root_message_ts"], "1712161829.000300"
            )
            self.assertNotIn("event_id", session_metadata["slack"])
            self.assertNotIn("user_id", session_metadata["slack"])

        self.assertEqual(
            agent_manager.turn_requests[0].idempotency_key, "slack:event:EvFirst"
        )
        self.assertEqual(
            agent_manager.turn_requests[1].idempotency_key, "slack:event:EvSecond"
        )
        first_metadata = json_format.MessageToDict(agent_manager.turn_requests[0].metadata)
        second_metadata = json_format.MessageToDict(agent_manager.turn_requests[1].metadata)
        self.assertEqual(first_metadata["slack"]["user_id"], "U456")
        self.assertEqual(second_metadata["slack"]["user_id"], "U999")
        self.assertEqual(
            second_metadata["slack"]["message_ts"], "1712161835.000400"
        )

    def test_configured_routes_ignore_non_matching_channels(self) -> None:
        provider_module.configure(
            "slack",
            {
                "agent": {
                    "routes": [
                        {
                            "id": "triage",
                            "match": {"channels": ["C_SUPPORT"]},
                            "agent": {"systemPrompt": "Triage support requests."},
                        }
                    ]
                }
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        agent_manager = FakeAgentManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvIgnored",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C_OTHER",
                "channel_type": "channel",
                "text": "<@UBOT> hello",
                "ts": "1712161829.000300",
            },
        }

        with mock.patch.object(
            gestalt.Request, "agent_manager", return_value=agent_manager
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response, {"ok": True, "ignored": "no_matching_agent_route"})
        self.assertEqual(agent_manager.requests, [])

    def test_default_routing_still_ignores_plain_channel_messages(self) -> None:
        provider_module.configure("slack", {})
        event, ignored = provider_module._slack_agent_event_from_payload(
            {
                "type": "event_callback",
                "event_id": "EvChannelMessage",
                "team_id": "T123",
                "event": {
                    "type": "message",
                    "user": "U456",
                    "channel": "C789",
                    "channel_type": "channel",
                    "text": "plain channel message",
                    "ts": "1712161829.000300",
                },
            }
        )

        self.assertEqual(ignored, "")
        self.assertIsNotNone(event)
        assert event is not None
        _route, ignored_route = provider_module._select_agent_route(event)
        self.assertEqual(ignored_route, "unsupported_event_type")

    def test_dm_event_does_not_invent_thread_reply_target(self) -> None:
        event, ignored = provider_module._slack_agent_event_from_payload(
            {
                "type": "event_callback",
                "event_id": "Ev456",
                "team_id": "T123",
                "event": {
                    "type": "message",
                    "user": "U456",
                    "channel": "D789",
                    "channel_type": "im",
                    "text": "hello agent",
                    "ts": "1712161900.000100",
                },
            }
        )

        self.assertEqual(ignored, "")
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.thread_ts, "")
        self.assertEqual(event.reply_thread_ts, "")
        self.assertEqual(provider_module._agent_session_ref(event), "slack:T123:D789")

    def test_url_verification_returns_challenge_without_agent_run(self) -> None:
        agent_manager = FakeAgentManager()
        payload = {"type": "url_verification", "challenge": "challenge-token"}

        with mock.patch.object(
            gestalt.Request, "agent_manager", return_value=agent_manager
        ):
            response = provider_module.slack_events_handle(payload, gestalt.Request())

        self.assertEqual(response, {"challenge": "challenge-token"})
        self.assertEqual(agent_manager.requests, [])

    def test_get_message_uses_history_lookup_contract(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
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

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            result = provider_module.conversations_get_message(
                provider_module.GetMessageInput(
                    url="https://valon.slack.com/archives/C123ABC456/p1712161829000300"
                ),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(result["data"]["message"]["ts"], "1712161829.000300")
        self.assertEqual(result["data"]["message"]["text"], "hello")

    def test_find_user_mentions_uses_history_contract(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
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

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
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

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
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
            {
                "error": 'slack API error (status 429): {"ok": false, "error": "rate_limited"}'
            },
        )


if __name__ == "__main__":
    unittest.main()
