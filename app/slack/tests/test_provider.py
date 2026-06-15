from __future__ import annotations

import hashlib
import io
import json
import pathlib
import types
import unittest
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import asdict, is_dataclass
from email.message import Message
from http import HTTPStatus
from http.client import HTTPMessage
from typing import Any, cast
from unittest import mock

import gestalt
import yaml
from gestalt.authorization import RelationshipTargetSubject

import internals.client as client_module
from internals.agent_links import agent_session_url
import provider as provider_module

PLUGIN_DIR = pathlib.Path(__file__).resolve().parents[1]


def sdk_value_to_dict(value: Any) -> Any:
    if value is None:
        return {}
    if isinstance(value, dict):
        return {str(key): sdk_value_to_dict(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [sdk_value_to_dict(item) for item in value]
    if is_dataclass(value):
        return sdk_value_to_dict(asdict(value))
    return value


def operation_body(result: Any) -> dict[str, Any]:
    if isinstance(result, gestalt.Response):
        return cast(dict[str, Any], result.body)
    return cast(dict[str, Any], result)


def delivered_event_data(request: Any) -> dict[str, Any]:
    return cast(dict[str, Any], sdk_value_to_dict(request.event.data))


def delivered_event_metadata(request: Any) -> dict[str, Any]:
    return cast(dict[str, Any], delivered_event_data(request)["metadata"])


def authorization_subject(
    *, type: str, id: str, properties: dict[str, Any] | None = None
) -> gestalt.AuthorizationSubject:
    return gestalt.AuthorizationSubject(type=type, id=id, properties=properties or {})


class FakeWorkflowDeliverEvent:
    def __init__(
        self,
        event: Any = None,
        provider_name: str = "",
        **_kwargs: Any,
    ) -> None:
        self.event = event
        self.provider_name = provider_name


class FakeHTTPResponse:
    def __init__(self, body: str | bytes, status: int = 200) -> None:
        self._body = body if isinstance(body, bytes) else body.encode("utf-8")
        self.status = status

    def __enter__(self) -> FakeHTTPResponse:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def read(self, size: int = -1) -> bytes:
        if size >= 0:
            return self._body[:size]
        return self._body


class FakeOpener:
    def __init__(self, callback: Any) -> None:
        self._callback = callback

    def open(
        self, request: urllib.request.Request, timeout: float = 30
    ) -> FakeHTTPResponse:
        return self._callback(request, timeout)


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


def request_header(request: urllib.request.Request, name: str) -> str:
    headers = {key.lower(): value for key, value in request.header_items()}
    return str(request.get_header(name) or headers.get(name.lower()) or "")


def request_json(request: urllib.request.Request) -> dict[str, Any]:
    return json.loads(cast(bytes, request.data).decode("utf-8"))


def request_form(request: urllib.request.Request) -> dict[str, list[str]]:
    return urllib.parse.parse_qs(cast(bytes, request.data).decode("utf-8"))


def signed_block_action_payload(
    subject_id: str = "user:gestalt-123",
) -> dict[str, Any]:
    event = provider_module.SlackAgentEvent(
        callback_type="event_callback",
        event_type="app_mention",
        event_id="Ev123",
        team_id="T123",
        user_id="U456",
        channel_id="C789",
        channel_type="channel",
        text="<@UBOT> approve deployment",
        message_ts="1712161829.000300",
        thread_ts="",
        reply_thread_ts="1712161829.000300",
    )
    reply_ref = provider_module._agent._sign_reply_ref(event, subject_id)
    verified_ref = provider_module._agent._verify_reply_ref(reply_ref, subject_id)
    interaction_ref = provider_module._agent._sign_interaction_ref(
        verified_ref,
        action_id="approve",
        action_value="approved",
        expires_in_seconds=300,
    )
    return {
        "type": "block_actions",
        "team": {"id": "T123"},
        "user": {"id": "U456"},
        "channel": {"id": "C789"},
        "container": {
            "type": "message",
            "channel_id": "C789",
            "message_ts": "1712161831.000500",
        },
        "trigger_id": "1337.abcdef",
        "actions": [
            {
                "action_id": "approve",
                "value": interaction_ref,
                "action_ts": "1712161832.000600",
            }
        ],
    }


def signed_route_block_action_payload(
    route_id: str,
    *,
    subject_id: str = "user:gestalt-123",
    event_user_id: str = "U456",
    interaction_user_id: str = "U456",
    channel_id: str = "C_ROUTE",
) -> dict[str, Any]:
    event = provider_module.SlackAgentEvent(
        callback_type="event_callback",
        event_type="app_mention",
        event_id="EvRouteInteraction",
        team_id="T123",
        user_id=event_user_id,
        channel_id=channel_id,
        channel_type="channel",
        text="<@UBOT> approve operation",
        message_ts="1712161829.000300",
        thread_ts="",
        reply_thread_ts="1712161829.000300",
    )
    route = provider_module._agent._agent_route_by_id(route_id)
    reply_ref = provider_module._agent._sign_reply_ref(event, subject_id, route)
    verified_ref = provider_module._agent._verify_reply_ref(reply_ref, subject_id)
    interaction_ref = provider_module._agent._sign_interaction_ref(
        verified_ref,
        action_id="approve",
        action_value="approved",
        expires_in_seconds=300,
    )
    return {
        "type": "block_actions",
        "team": {"id": "T123"},
        "user": {"id": interaction_user_id},
        "channel": {"id": channel_id},
        "container": {
            "type": "message",
            "channel_id": channel_id,
            "message_ts": "1712161831.000500",
        },
        "trigger_id": "1337.abcdef",
        "actions": [
            {
                "action_id": "approve",
                "value": interaction_ref,
                "action_ts": "1712161832.000600",
            }
        ],
    }


def _catalog_parameter_names(operation: dict[str, Any]) -> list[str]:
    return [parameter["name"] for parameter in operation.get("parameters", [])]


def _catalog_parameter(operation: dict[str, Any], name: str) -> dict[str, Any]:
    for parameter in operation.get("parameters", []):
        if parameter["name"] == name:
            return cast(dict[str, Any], parameter)
    raise AssertionError(f"missing catalog parameter {name}")


def _manifest_parameter_names(operation: dict[str, Any]) -> list[str]:
    return [parameter["name"] for parameter in operation.get("parameters", [])]


def _manifest_parameter_types(operation: dict[str, Any], name: str) -> list[str]:
    return [
        parameter["type"]
        for parameter in operation.get("parameters", [])
        if parameter["name"] == name
    ]


class FakeAuthorization:
    def __init__(
        self,
        subjects: list[gestalt.AuthorizationSubject] | None = None,
        resources: list[gestalt.AuthorizationResource] | None = None,
    ) -> None:
        self.subjects = subjects or []
        self.resources = resources or []
        self.requests: list[gestalt.ListRelationshipsRequest] = []
        self.relationship_adds: list[gestalt.AddRelationshipRequest] = []

    def list_relationships(
        self, request: gestalt.ListRelationshipsRequest
    ) -> gestalt.ListRelationshipsResponse:
        self.requests.append(request)
        relationship_filter = request.filter
        resource = relationship_filter.resource if relationship_filter else None
        target = relationship_filter.target if relationship_filter else None
        relation = relationship_filter.relation if relationship_filter else ""
        relationships: list[gestalt.Relationship] = []

        for subject in self.subjects:
            relationships.append(
                gestalt.Relationship(
                    tuple=gestalt.RelationshipTuple(
                        target=gestalt.RelationshipTarget(
                            kind=RelationshipTargetSubject(value=subject)
                        ),
                        relation=relation,
                        resource=resource,
                    )
                )
            )

        for linked_resource in self.resources:
            relationships.append(
                gestalt.Relationship(
                    tuple=gestalt.RelationshipTuple(
                        target=target,
                        relation=relation,
                        resource=linked_resource,
                    )
                )
            )

        return gestalt.ListRelationshipsResponse(relationships=relationships)

    def add_relationship(
        self, request: gestalt.AddRelationshipRequest
    ) -> gestalt.AddRelationshipResponse:
        self.relationship_adds.append(request)
        return gestalt.AddRelationshipResponse(relationship=request.relationship)


class FakeWorkflowClient:
    def __init__(self) -> None:
        self.deliver_event_requests: list[Any] = []
        self.deliver_event_error: Exception | None = None

    def __enter__(self) -> FakeWorkflowClient:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def deliver_event(self, request: Any) -> Any:
        self.deliver_event_requests.append(request)
        if self.deliver_event_error is not None:
            raise self.deliver_event_error
        return request.event


class ExplodingDeliveryResponseWorkflowClient(FakeWorkflowClient):
    def deliver_event(self, request: Any) -> Any:
        self.deliver_event_requests.append(request)

        class Response:
            @property
            def id(self) -> str:
                raise RuntimeError("bad deliver response")

        return Response()


class FailingSecondDeliveryWorkflowClient(FakeWorkflowClient):
    def deliver_event(self, request: Any) -> Any:
        self.deliver_event_requests.append(request)
        if len(self.deliver_event_requests) == 2:
            raise RuntimeError("boom")
        return request.event


class SlackProviderTests(unittest.TestCase):
    def test_agent_session_url_preserves_public_base_path(self) -> None:
        url = agent_session_url(
            "https://gestalt.example.test/team-a/",
            "agent session/123",
        )
        parsed = urllib.parse.urlparse(url)
        self.assertEqual(parsed.scheme, "https")
        self.assertEqual(parsed.netloc, "gestalt.example.test")
        self.assertEqual(parsed.path, "/team-a/agents")
        self.assertEqual(
            urllib.parse.parse_qs(parsed.query),
            {"session": ["agent session/123"]},
        )

    def _capture_chat_post_message(
        self,
        input: provider_module.ChatPostMessageInput,
        req: gestalt.Request,
    ) -> tuple[Any, dict[str, Any]]:
        captured: dict[str, Any] = {}

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(request.full_url, "https://slack.com/api/chat.postMessage")
            captured["authorization"] = authorization_header(request)
            captured["payload"] = request_json(request)
            return FakeHTTPResponse(
                '{"ok": true, "channel": "C123", "ts": "1712161830.000400"}'
            )

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            result = provider_module.chat_post_message(input, req)
        return result, captured

    def _signed_reply_ref(
        self,
        *,
        subject_id: str = "user:gestalt-123",
        reply_thread_ts: str = "1712161829.000300",
    ) -> str:
        event = provider_module.SlackAgentEvent(
            callback_type="event_callback",
            event_type="app_mention",
            event_id="EvUpload",
            team_id="T123",
            user_id="U456",
            channel_id="C789",
            channel_type="channel",
            text="<@UBOT> attach report",
            message_ts="1712161829.000300",
            thread_ts="",
            reply_thread_ts=reply_thread_ts,
        )
        return provider_module._agent._sign_reply_ref(event, subject_id)

    def _slack_agent_workflow_context(
        self,
        reply_ref: str,
        *,
        channel_id: str = "C789",
        event_type: str | None = None,
        slack_fields: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        slack = {
            "team_id": "T123",
            "channel_id": channel_id,
            "message_ts": "1712161829.000300",
            "reply_thread_ts": "1712161829.000300",
        }
        if slack_fields:
            slack.update(slack_fields)
        return {
            "runId": "run-slack-1",
            "provider": "local",
            "trigger": {
                "kind": "event",
                "activationId": "slack-event",
                "event": {
                    "id": "slack:event:T123:C789:1712161829.000300:U456",
                    "source": "slack",
                    "type": event_type
                    or provider_module._agent.SLACK_AGENT_WORKFLOW_EVENT_TYPE,
                    "subject": "route:gestalt-support",
                    "data": {
                        "routeId": "",
                        "reply_ref": reply_ref,
                        "slack": slack,
                    },
                },
            },
        }

    def _slack_bot_service_account_request(
        self, token: str = "xoxb-resolved-bot"
    ) -> gestalt.Request:
        return gestalt.Request(
            token=token,
            credential=gestalt.Credential(mode="subject", connection="bot"),
            subject=gestalt.Subject(id="service_account:slack-bot"),
        )

    def _handle_event_with_workflow(
        self, payload: dict[str, Any]
    ) -> tuple[Any, FakeWorkflowClient]:
        workflow_client = FakeWorkflowClient()
        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )
        return response, workflow_client

    def test_agent_module_reexports_model_interfaces(self) -> None:
        import internals.agent as agent_module
        import internals.models as models_module

        for name in (
            "SlackAgentConfig",
            "SlackAgentEvent",
            "SlackAgentRoute",
            "SlackAgentRouteMatch",
            "SlackAssistantConfig",
            "SlackBotConfig",
            "SlackCallbackType",
            "SlackChannelType",
            "SlackEventDeliveryConfig",
            "SlackEventDeliveryRoute",
            "SlackEventDeliveryRouteMatch",
            "SlackEventType",
            "SlackInteractionRef",
            "SlackReplyRef",
            "SlackWorkflowConfig",
        ):
            self.assertIs(getattr(agent_module, name), getattr(models_module, name))

    def test_identity_link_self_links_current_subject_to_authenticated_slack_user(
        self,
    ) -> None:
        authorization = FakeAuthorization()

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(request.full_url, "https://slack.com/api/auth.test")
            self.assertEqual(authorization_header(request), "Bearer xoxp-user")
            self.assertEqual(request_form(request), {})
            return FakeHTTPResponse(
                '{"ok": true, "team_id": "T123", "user_id": "U456"}'
            )

        with (
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch.object(
                gestalt.Request, "authorization", return_value=authorization, create=True
            ),
        ):
            response = provider_module.slack_identity_link_self(
                {},
                gestalt.Request(
                    token="xoxp-user",
                    subject=gestalt.Subject(id="user:gestalt-123"),
                ),
            )

        self.assertEqual(
            operation_body(response),
            {
                "ok": True,
                "team_id": "T123",
                "user_id": "U456",
                "resource": {
                    "type": provider_module._agent.SLACK_USER_RESOURCE_TYPE,
                    "id": "T123/U456",
                },
            },
        )
        self.assertEqual(len(authorization.relationship_adds), 1)
        relationship = authorization.relationship_adds[0].relationship
        self.assertIsNotNone(relationship)
        assert relationship is not None
        relationship_tuple = relationship.tuple
        self.assertIsNotNone(relationship_tuple)
        assert relationship_tuple is not None
        self.assertIsNotNone(relationship_tuple.target)
        assert relationship_tuple.target is not None
        self.assertIsInstance(relationship_tuple.target.kind, RelationshipTargetSubject)
        self.assertIsNotNone(relationship_tuple.resource)
        subject = cast(
            RelationshipTargetSubject, relationship_tuple.target.kind
        ).value
        resource = cast(gestalt.AuthorizationResource, relationship_tuple.resource)
        self.assertEqual(subject.type, "subject")
        self.assertEqual(subject.id, "user:gestalt-123")
        self.assertEqual(
            relationship_tuple.relation,
            provider_module._agent.SLACK_USER_LINKED_ACTION,
        )
        self.assertEqual(resource.type, provider_module._agent.SLACK_USER_RESOURCE_TYPE)
        self.assertEqual(resource.id, "T123/U456")
        self.assertEqual(
            relationship.source_layer, gestalt.SourceLayerValues.RUNTIME
        )

    def test_agent_routes_reject_duplicate_ids(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicates another agent route"):
            provider_module.configure(
                "slack",
                {
                    "agent": {
                        "routes": [
                            {"id": "duplicate", "match": {"channel": "C1"}},
                            {"id": "duplicate", "match": {"channel": "C2"}},
                        ]
                    }
                },
            )

    def test_agent_routes_require_service_account_run_as_subjects(self) -> None:
        invalid_run_as = [
            {"subject": {"id": "system:http_binding:slack:events"}},
            {"subject": {"id": "user:gestalt-123"}},
            {"subject": {"id": "service_account:"}},
            {"subject": {"id": "automation-without-kind"}},
            {"subject": {"id": "bot:gestalt-alerts", "kind": "bot"}},
        ]

        for run_as in invalid_run_as:
            with self.subTest(run_as=run_as):
                with self.assertRaisesRegex(
                    ValueError,
                    "runAs.subject must identify a service_account subject",
                ):
                    provider_module.configure(
                        "slack",
                        {
                            "agent": {
                                "routes": [
                                    {
                                        "id": "alerts",
                                        "runAs": run_as,
                                        "match": {"channel": "C_ALERTS"},
                                    }
                                ]
                            }
                        },
                    )

    def test_agent_routes_validate_run_as_shape(self) -> None:
        invalid_run_as = [
            ("user", "runAs must be an object"),
            ({}, "runAs.subject is required"),
            ({"subject": "service_account:slack-bot"}, "runAs.subject is required"),
            ({"subject": {}}, "runAs.subject.id is required"),
            (
                {"subject": {"kind": "service_account"}},
                "runAs.subject.id is required",
            ),
        ]

        for run_as, error in invalid_run_as:
            with self.subTest(run_as=run_as):
                with self.assertRaisesRegex(ValueError, error):
                    provider_module.configure(
                        "slack",
                        {
                            "agent": {
                                "routes": [
                                    {
                                        "id": "alerts",
                                        "runAs": run_as,
                                        "match": {"channel": "C_ALERTS"},
                                    }
                                ]
                            }
                        },
                    )

    def test_agent_routes_require_guarded_match_for_run_as_subjects(self) -> None:
        invalid_matches = [
            {"channel": "C_ALERTS"},
            {"channel": "C_ALERTS", "botIds": []},
            {"channel": "C_ALERTS", "includeBotEvents": True},
            {
                "channel": "C_ALERTS",
                "eventTypes": ["message.channels"],
                "thread": "root",
            },
            {
                "channel": "C_ALERTS",
                "eventTypes": ["message.channels"],
                "thread": "root",
                "addressedToBot": True,
            },
            {
                "eventTypes": ["message.channels"],
                "thread": "root",
                "addressedToBot": False,
            },
            {
                "channel": "C_ALERTS",
                "eventTypes": ["app_mention"],
                "thread": "root",
                "addressedToBot": False,
            },
            {
                "channel": "C_ALERTS",
                "eventTypes": ["message.channels"],
                "thread": "any",
                "addressedToBot": False,
            },
        ]

        for match in invalid_matches:
            with self.subTest(match=match):
                with self.assertRaisesRegex(
                    ValueError,
                    "runAs requires match.botIds or an explicit top-level "
                    "unaddressed channel message match",
                ):
                    provider_module.configure(
                        "slack",
                        {
                            "agent": {
                                "routes": [
                                    {
                                        "id": "alerts",
                                        "runAs": {
                                            "subject": {
                                                "id": "service_account:slack-bot"
                                            }
                                        },
                                        "match": match,
                                    }
                                ]
                            }
                        },
                    )

        provider_module.configure(
            "slack",
            {
                "agent": {
                    "routes": [
                        {
                            "id": "support-background",
                            "runAs": {
                                "subject": {
                                    "id": "service_account:eng-background-agent"
                                }
                            },
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["message.channels"],
                                "thread": "root",
                                "addressedToBot": False,
                            },
                        }
                    ]
                }
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})

    def test_agent_routes_validate_event_type_literals(self) -> None:
        invalid_match_configs = [
            {"eventTypes": ["not_an_event"]},
            {"eventTypes": ["Message.Channels"]},
            {"eventTypes": ["message.channels", 123]},
            {"eventTypes": 123},
        ]

        for match_config in invalid_match_configs:
            with self.subTest(match=match_config):
                with self.assertRaisesRegex(
                    ValueError,
                    "eventTypes|must be one of|must be a string",
                ):
                    provider_module.configure(
                        "slack",
                        {"agent": {"routes": [{"id": "bad", "match": match_config}]}},
                    )

    def test_agent_routes_validate_thread_literals(self) -> None:
        invalid_match_configs = [
            {"thread": "Root"},
            {"thread": ""},
            {"thread": ["root"]},
            {"thread": True},
            {"thread": 123},
        ]

        for match_config in invalid_match_configs:
            with self.subTest(match=match_config):
                with self.assertRaisesRegex(
                    ValueError,
                    "thread|must be one of|must be a string|must not be empty",
                ):
                    provider_module.configure(
                        "slack",
                        {"agent": {"routes": [{"id": "bad", "match": match_config}]}},
                    )

    def test_agent_routes_accept_event_type_aliases(self) -> None:
        self.addCleanup(provider_module.configure, "slack", {})
        alias_cases = [
            ("eventType", "message.channels"),
            ("event_type", "message.channels"),
            ("event_types", ["message.channels"]),
        ]

        for key, value in alias_cases:
            with self.subTest(key=key):
                provider_module.configure(
                    "slack",
                    {
                        "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                        "workflow": {"provider": "local"},
                        "agent": {
                            "routes": [
                                {
                                    "id": "all-channel-messages",
                                    "match": {
                                        "channel": "C_SUPPORT",
                                        key: value,
                                    },
                                }
                            ],
                        },
                    },
                )
                payload = {
                    "type": "event_callback",
                    "event_id": f"EvAlias{key}",
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

                response, workflow_client = self._handle_event_with_workflow(payload)

                self.assertEqual(operation_body(response)["ok"], True)
                self.assertEqual(len(workflow_client.deliver_event_requests), 1)
                event_metadata = delivered_event_metadata(
                    workflow_client.deliver_event_requests[0]
                )
                self.assertEqual(
                    event_metadata["slack"]["agent_route_id"],
                    "all-channel-messages",
                )

    def test_missing_workflow_provider_rejects_matched_route(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "agent": {
                    "routes": [
                        {
                            "id": "matched",
                            "match": {"channel": "C_STEPS"},
                        }
                    ],
                },
            },
        )
        workflow_client = FakeWorkflowClient()
        payload = {
            "type": "event_callback",
            "event_id": "EvMissingDefinition",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C_STEPS",
                "channel_type": "channel",
                "text": "<@UBOT> plan this",
                "ts": "1712161829.000300",
            },
        }

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertIsInstance(response, gestalt.Response)
        error = cast(gestalt.Response[dict[str, str]], response)
        self.assertEqual(error.status, HTTPStatus.PRECONDITION_FAILED)
        self.assertEqual(
            error.body,
            {"error": "Slack workflow provider is not configured"},
        )
        self.assertEqual(workflow_client.deliver_event_requests, [])

    def test_route_workflow_config_maps_to_delivered_agent_event(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "raw-workflow",
                            "match": {"channel": "C_WORKFLOW"},
                            "workflow": {
                                "provider": "route-provider",
                                "workflowEventType": "slack.route.agent.received",
                                "subject": "route:raw-workflow",
                            },
                        }
                    ]
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        payload = {
            "type": "event_callback",
            "event_id": "EvRawWorkflow",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C_WORKFLOW",
                "channel_type": "channel",
                "text": "<@UBOT> classify this",
                "ts": "1712161829.000300",
            },
        }

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        workflow_request = workflow_client.deliver_event_requests[0]
        self.assertEqual(workflow_request.provider_name, "route-provider")
        self.assertEqual(workflow_request.event.type, "slack.route.agent.received")
        self.assertEqual(workflow_request.event.subject, "route:raw-workflow")
        self.assertEqual(
            workflow_request.event.id,
            "slack:event:T123:C_WORKFLOW:1712161829.000300:U456",
        )
        event_data = delivered_event_data(workflow_request)
        self.assertEqual(
            event_data["workflowKey"],
            "slack:T123:C_WORKFLOW:1712161829.000300",
        )
        self.assertEqual(event_data["routeId"], "raw-workflow")
        self.assertEqual(event_data["slack"]["channel_id"], "C_WORKFLOW")
        self.assertIn("<@UBOT> classify this", event_data["user_prompt"])
        verified_ref = provider_module._verify_reply_ref(
            event_data["reply_ref"], "user:gestalt-123"
        )
        self.assertEqual(verified_ref.workflow_key, event_data["workflowKey"])

    def test_workflow_config_rejects_unsupported_keys(self) -> None:
        invalid_configs = [
            {"workflow": {"target": {"steps": []}}},
            {"workflow": {"unexpected": "value"}},
            {
                "agent": {
                    "routes": [
                        {
                            "id": "bad-workflow",
                            "workflow": {"target": {"steps": []}},
                        }
                    ]
                }
            },
            {
                "agent": {
                    "routes": [
                        {
                            "id": "bad-workflow",
                            "workflow": {"unexpected": "value"},
                        }
                    ]
                }
            },
            {
                "events": {
                    "deliver": {
                        "routes": [
                            {
                                "id": "bad-deliver",
                                "unexpected": "value",
                            }
                        ]
                    }
                }
            },
        ]

        for config in invalid_configs:
            with self.subTest(config=config):
                with self.assertRaisesRegex(ValueError, "unsupported key"):
                    provider_module.configure("slack", config)

    def test_top_level_workflow_config_maps_to_delivered_agent_event(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        payload = {
            "type": "event_callback",
            "event_id": "EvDefaultWorkflow",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> use global workflow",
                "ts": "1712161829.000300",
            },
        }

        with mock.patch.object(
            gestalt.Request,
            "workflows",
            return_value=workflow_client,
            create=True,
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        workflow_request = workflow_client.deliver_event_requests[0]
        self.assertEqual(workflow_request.provider_name, "local")
        self.assertEqual(workflow_request.event.type, "slack.agent.event.received")
        self.assertEqual(workflow_request.event.subject, "route:default")
        event_data = delivered_event_data(workflow_request)
        self.assertEqual(event_data["routeId"], "default")
        self.assertIn("<@UBOT> use global workflow", event_data["user_prompt"])

    def test_catalog_and_manifest_expose_native_assistant_contracts(self) -> None:
        catalog = yaml.safe_load((PLUGIN_DIR / "catalog.yaml").read_text())
        manifest = yaml.safe_load((PLUGIN_DIR / "manifest.yaml").read_text())
        catalog_ops = {op["id"]: op for op in catalog["operations"]}
        rest_ops = {
            op["name"]: op for op in manifest["spec"]["surfaces"]["rest"]["operations"]
        }
        http_routes = manifest["spec"]["http"]

        self.assertIn(provider_module.SLACK_IDENTITY_LINK_SELF_OPERATION, catalog_ops)
        self.assertIn(
            "Slack user",
            catalog_ops[provider_module.SLACK_IDENTITY_LINK_SELF_OPERATION][
                "description"
            ],
        )
        self.assertEqual(
            _catalog_parameter_names(catalog_ops["events.reply"]),
            ["reply_ref", "message"],
        )
        self.assertEqual(
            _catalog_parameter_names(catalog_ops["events.uploadFile"]),
            [
                "reply_ref",
                "filename",
                "content_base64",
                "title",
                "initial_comment",
                "content_type",
                "alt_txt",
                "snippet_type",
                "blocks",
            ],
        )
        self.assertFalse(catalog_ops["events.uploadFile"]["visible"])
        self.assertNotIn(
            "default", _catalog_parameter(catalog_ops["events.uploadFile"], "filename")
        )
        self.assertEqual(
            _catalog_parameter(catalog_ops["events.uploadFile"], "content_base64")[
                "required"
            ],
            True,
        )
        self.assertIn(
            "UTF-8 text files",
            _catalog_parameter(catalog_ops["events.uploadFile"], "content_base64")[
                "description"
            ],
        )
        self.assertNotIn(
            "default",
            _catalog_parameter(catalog_ops["events.uploadFile"], "content_base64"),
        )
        self.assertIn(
            "requires reply_ref and message", catalog_ops["events.reply"]["description"]
        )
        reply_parameters = {
            parameter["name"]: parameter
            for parameter in catalog_ops["events.reply"]["parameters"]
        }
        self.assertIn(
            "current Slack workflow event",
            reply_parameters["reply_ref"]["description"],
        )
        self.assertIn(
            "Structured Slack message", reply_parameters["message"]["description"]
        )
        self.assertEqual(
            _catalog_parameter_names(catalog_ops["events.replySessionStarted"]),
            ["reply_ref", "session_id"],
        )
        self.assertFalse(catalog_ops["events.replySessionStarted"]["visible"])
        self.assertEqual(
            _catalog_parameter_names(catalog_ops["events.clearAssistantStatus"]),
            ["reply_ref"],
        )
        self.assertEqual(
            _catalog_parameter_names(catalog_ops["interactions.request"]),
            ["reply_ref", "text", "actions", "expires_in_seconds"],
        )
        self.assertEqual(
            _catalog_parameter_names(catalog_ops["chat.postMessage"]),
            [
                "channel",
                "text",
                "thread_ts",
                "unfurl_links",
                "unfurl_media",
                "blocks",
                "metadata",
            ],
        )
        self.assertEqual(
            _catalog_parameter_names(catalog_ops["files.upload"]),
            [
                "channel",
                "filename",
                "content_base64",
                "thread_ts",
                "title",
                "initial_comment",
                "content_type",
                "alt_txt",
                "snippet_type",
                "blocks",
            ],
        )
        self.assertNotIn(
            "default", _catalog_parameter(catalog_ops["files.upload"], "channel")
        )
        self.assertNotIn(
            "default", _catalog_parameter(catalog_ops["files.upload"], "filename")
        )
        self.assertIn(
            "UTF-8 text files",
            _catalog_parameter(catalog_ops["files.upload"], "content_base64")[
                "description"
            ],
        )
        self.assertEqual(
            _catalog_parameter(catalog_ops["files.upload"], "content_base64")[
                "required"
            ],
            True,
        )
        self.assertNotIn(
            "default",
            _catalog_parameter(catalog_ops["files.upload"], "content_base64"),
        )
        self.assertNotIn("assistant.reconcileStuckRequests", catalog_ops)
        self.assertNotIn("chat.postMessage", rest_ops)
        self.assertNotIn("files.upload", rest_ops)
        self.assertNotIn("files.getUploadURLExternal", rest_ops)
        self.assertNotIn("files.completeUploadExternal", rest_ops)
        self.assertEqual(http_routes["interactions"]["path"], "/interactions")
        self.assertEqual(http_routes["interactions"]["target"], "interactions.handle")

        self.assertEqual(
            _manifest_parameter_types(
                rest_ops["assistant.threads.setStatus"], "loading_messages"
            ),
            ["array"],
        )
        self.assertEqual(
            _manifest_parameter_types(
                rest_ops["assistant.threads.setSuggestedPrompts"], "prompts"
            ),
            ["array"],
        )
        for operation_name in (
            "chat.startStream",
            "chat.appendStream",
            "chat.stopStream",
        ):
            operation = rest_ops[operation_name]
            self.assertEqual(operation["connection"], "bot")
            self.assertNotIn("connectionSelector", operation)
            self.assertNotIn("actor", _manifest_parameter_names(operation))
        self.assertEqual(
            _manifest_parameter_types(rest_ops["chat.stopStream"], "blocks"),
            ["array"],
        )

    def test_manifest_models_bot_connection_as_user_owned_bearer(self) -> None:
        manifest = yaml.safe_load((PLUGIN_DIR / "manifest.yaml").read_text())
        rest_ops = {
            op["name"]: op for op in manifest["spec"]["surfaces"]["rest"]["operations"]
        }
        connections = manifest["spec"]["connections"]

        self.assertEqual(connections["default"]["auth"]["type"], "oauth2")
        self.assertEqual(
            connections["default"]["auth"]["accessTokenPath"],
            "authed_user.access_token",
        )
        bot_connection = connections["bot"]
        self.assertNotIn("mode", bot_connection)
        self.assertNotIn("exposure", bot_connection)
        self.assertEqual(bot_connection["auth"], {"type": "bearer"})
        self.assertNotIn("instance" + "Selector", json.dumps(manifest))

        user_default_selector_operations = (
            "conversations.list",
            "conversations.open",
            "conversations.history",
            "conversations.replies",
        )
        for operation_name in user_default_selector_operations:
            operation = rest_ops[operation_name]
            self.assertEqual(
                operation["connectionSelector"],
                {
                    "parameter": "actor",
                    "default": "user",
                    "values": {"bot": "bot", "user": "default"},
                },
            )
            self.assertIn("actor", _manifest_parameter_names(operation))

        self.assertNotIn("chat.postMessage", rest_ops)

        self.assertEqual(rest_ops["search.messages"]["connection"], "default")
        self.assertNotIn("connectionSelector", rest_ops["search.messages"])
        self.assertIn("im:write", connections["default"]["auth"]["scopes"])
        self.assertIn("mpim:write", connections["default"]["auth"]["scopes"])
        self.assertIn("users:read.email", connections["default"]["auth"]["scopes"])
        self.assertIn("files:write", connections["default"]["auth"]["scopes"])
        docs = (PLUGIN_DIR / "docs" / "index.mdx").read_text()
        self.assertIn('app.invoke("slack", "files.upload"', docs)
        self.assertIn('app.invoke("slack", "events.uploadFile"', docs)
        self.assertIn("content_base64", docs)
        self.assertIn("reconnect or reauthorize", docs)
        self.assertEqual(
            _manifest_parameter_names(rest_ops["conversations.open"]),
            ["actor", "users", "channel", "return_im", "prevent_creation"],
        )
        for operation_name in (
            "assistant.threads.setStatus",
            "assistant.threads.setTitle",
            "assistant.threads.setSuggestedPrompts",
            "chat.startStream",
            "chat.appendStream",
            "chat.stopStream",
        ):
            operation = rest_ops[operation_name]
            self.assertEqual(operation["connection"], "bot")
            self.assertNotIn("connectionSelector", operation)
            self.assertNotIn("actor", _manifest_parameter_names(operation))

    def test_chat_post_message_uses_resolved_bot_token_for_slack_bot_service_account(
        self,
    ) -> None:
        provider_module.configure("slack", {})

        result, captured = self._capture_chat_post_message(
            provider_module.ChatPostMessageInput(
                channel="C123",
                text="hello from gestalt",
            ),
            self._slack_bot_service_account_request(),
        )

        self.assertEqual(
            result, {"ok": True, "channel": "C123", "ts": "1712161830.000400"}
        )
        self.assertEqual(captured["authorization"], "Bearer xoxb-resolved-bot")
        payload = captured["payload"]
        self.assertEqual(payload["channel"], "C123")
        self.assertEqual(payload["text"], "hello from gestalt")
        self.assertNotIn("unfurl_links", payload)
        self.assertNotIn("unfurl_media", payload)
        self.assertEqual(payload["blocks"][0]["type"], "section")
        self.assertEqual(payload["blocks"][0]["text"]["text"], "hello from gestalt")
        self.assertEqual(payload["blocks"][-1]["type"], "context")
        self.assertEqual(
            payload["blocks"][-1]["elements"][0]["text"], "Sent with Gestalt"
        )
        self.assertEqual(
            payload["metadata"],
            {
                "event_type": "gestalt_message",
                "event_payload": {"sent_with": "gestalt"},
            },
        )

    def test_chat_post_message_service_account_uses_request_token(self) -> None:
        provider_module.configure("slack", {"bot": {"token": "xoxb-configured-bot"}})

        _, captured = self._capture_chat_post_message(
            provider_module.ChatPostMessageInput(channel="C123", text="hello"),
            self._slack_bot_service_account_request(token="xoxb-managed-bot"),
        )

        self.assertEqual(captured["authorization"], "Bearer xoxb-managed-bot")

    def test_chat_post_message_service_account_footer_identifies_agent_actor(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {"bot": {"token": "xoxb-configured-bot", "userId": "UBOT"}},
        )

        authorization = FakeAuthorization(
            [],
            resources=[
                gestalt.AuthorizationResource(
                    type=provider_module._agent.SLACK_USER_RESOURCE_TYPE,
                    id="T123/U456",
                )
            ],
        )
        with mock.patch.object(
            gestalt.Request, "authorization", return_value=authorization, create=True
        ):
            _, captured = self._capture_chat_post_message(
                provider_module.ChatPostMessageInput(
                    channel="C123",
                    text="hello from gestalt",
                ),
                self._slack_bot_service_account_request(),
            )

        self.assertEqual(
            captured["payload"]["blocks"][-1]["elements"][0]["text"],
            "Sent by <@U456> with <@UBOT>",
        )

    def test_chat_post_message_uses_request_token_for_non_bot_subjects(self) -> None:
        provider_module.configure(
            "slack",
            {"bot": {"token": "xoxb-configured-bot", "userId": "UBOT"}},
        )

        _, captured = self._capture_chat_post_message(
            provider_module.ChatPostMessageInput(
                channel="C123",
                text="hello",
                unfurl_links=True,
                unfurl_media=False,
            ),
            gestalt.Request(
                token="xoxp-user",
                credential=gestalt.Credential(connection="default"),
                subject=gestalt.Subject(id="user:gestalt-123"),
            ),
        )

        self.assertEqual(captured["authorization"], "Bearer xoxp-user")
        self.assertEqual(captured["payload"]["unfurl_links"], True)
        self.assertEqual(captured["payload"]["unfurl_media"], False)
        self.assertEqual(
            captured["payload"]["blocks"][-1]["elements"][0]["text"],
            "Sent with <@UBOT>",
        )

        _, other_service_account = self._capture_chat_post_message(
            provider_module.ChatPostMessageInput(channel="C123", text="hello"),
            gestalt.Request(
                token="xoxp-service-account",
                credential=gestalt.Credential(mode="subject"),
                subject=gestalt.Subject(id="service_account:slack-bot-2"),
            ),
        )

        self.assertEqual(
            other_service_account["authorization"], "Bearer xoxp-service-account"
        )

    def test_chat_post_message_validates_request_token(self) -> None:
        provider_module.configure("slack", {})

        missing_service_account_token = provider_module.chat_post_message(
            provider_module.ChatPostMessageInput(channel="C123", text="hello"),
            self._slack_bot_service_account_request(token=""),
        )
        self.assertIsInstance(missing_service_account_token, gestalt.Response)
        self.assertEqual(
            cast(
                gestalt.Response[dict[str, str]], missing_service_account_token
            ).status,
            HTTPStatus.UNAUTHORIZED,
        )

        other_service_account_missing_token = provider_module.chat_post_message(
            provider_module.ChatPostMessageInput(channel="C123", text="hello"),
            gestalt.Request(
                credential=gestalt.Credential(mode="none"),
                subject=gestalt.Subject(id="service_account:slack-bot-2"),
            ),
        )
        self.assertIsInstance(other_service_account_missing_token, gestalt.Response)
        self.assertEqual(
            cast(
                gestalt.Response[dict[str, str]],
                other_service_account_missing_token,
            ).status,
            HTTPStatus.UNAUTHORIZED,
        )

        missing_token = provider_module.chat_post_message(
            provider_module.ChatPostMessageInput(channel="C123", text="hello"),
            gestalt.Request(credential=gestalt.Credential(connection="default")),
        )
        self.assertIsInstance(missing_token, gestalt.Response)
        self.assertEqual(
            cast(gestalt.Response[dict[str, str]], missing_token).status,
            HTTPStatus.UNAUTHORIZED,
        )

    def test_chat_post_message_preserves_caller_blocks_and_metadata(self) -> None:
        provider_module.configure("slack", {"bot": {"token": "xoxb-configured-bot"}})

        caller_blocks = [{"type": "divider"}]
        caller_metadata = {
            "event_type": "caller_event",
            "event_payload": {"request_id": "req-123"},
        }

        _, captured = self._capture_chat_post_message(
            provider_module.ChatPostMessageInput(
                channel="C123",
                text="fallback",
                blocks=caller_blocks,
                metadata=caller_metadata,
            ),
            self._slack_bot_service_account_request(token="xoxb-managed-bot"),
        )

        payload = captured["payload"]
        self.assertEqual(payload["blocks"][:-1], caller_blocks)
        self.assertEqual(payload["blocks"][-1]["type"], "context")
        self.assertEqual(payload["metadata"], caller_metadata)

    def test_chat_post_message_empty_metadata_sends_no_metadata(self) -> None:
        _, captured = self._capture_chat_post_message(
            provider_module.ChatPostMessageInput(
                channel="C123", text="hello", blocks=[], metadata={}
            ),
            gestalt.Request(token="xoxp-user"),
        )

        payload = captured["payload"]
        self.assertNotIn("metadata", payload)
        self.assertEqual(payload["blocks"][0]["type"], "section")
        self.assertEqual(payload["blocks"][-1]["type"], "context")

    def test_chat_post_message_rejects_block_and_text_limit_violations(self) -> None:
        too_many_blocks = provider_module.chat_post_message(
            provider_module.ChatPostMessageInput(
                channel="C123",
                text="fallback",
                blocks=[{"type": "divider"} for _ in range(50)],
            ),
            gestalt.Request(token="xoxp-user"),
        )
        self.assertIsInstance(too_many_blocks, gestalt.Response)
        self.assertEqual(
            cast(gestalt.Response[dict[str, str]], too_many_blocks).status,
            HTTPStatus.BAD_REQUEST,
        )

        malformed_blocks = provider_module.chat_post_message(
            provider_module.ChatPostMessageInput(
                channel="C123",
                text="fallback",
                blocks=cast(Any, ["not-a-block"]),
            ),
            gestalt.Request(token="xoxp-user"),
        )
        self.assertIsInstance(malformed_blocks, gestalt.Response)
        self.assertEqual(
            cast(gestalt.Response[dict[str, str]], malformed_blocks).status,
            HTTPStatus.BAD_REQUEST,
        )

        too_long_text = provider_module.chat_post_message(
            provider_module.ChatPostMessageInput(
                channel="C123",
                text="x" * (49 * 3000 + 1),
            ),
            gestalt.Request(token="xoxp-user"),
        )
        self.assertIsInstance(too_long_text, gestalt.Response)
        self.assertEqual(
            cast(gestalt.Response[dict[str, str]], too_long_text).status,
            HTTPStatus.BAD_REQUEST,
        )

    def test_http_subject_resolves_slack_user_through_linked_resource(
        self,
    ) -> None:
        subject = authorization_subject(type="subject", id="user:gestalt-123")
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
            gestalt.Request, "authorization", return_value=authorization, create=True
        ):
            resolved = provider_module.resolve_http_subject(
                gestalt.HTTPSubjectRequest(params=payload),
                gestalt.Request(),
            )

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved.id, "user:gestalt-123")

        self.assertEqual(len(authorization.requests), 1)
        request = authorization.requests[0]
        self.assertIsNotNone(request.filter)
        assert request.filter is not None
        resource = request.filter.resource
        self.assertIsNotNone(resource)
        self.assertEqual(resource.type, provider_module._agent.SLACK_USER_RESOURCE_TYPE)
        self.assertEqual(
            resource.id,
            provider_module.slack_user_resource_id("T123", "U456"),
        )
        self.assertEqual(
            request.filter.relation,
            provider_module._agent.SLACK_USER_LINKED_ACTION,
        )
        self.assertEqual(
            request.filter.target_type,
            gestalt.RelationshipTargetTypeValues.SUBJECT,
        )

    def test_http_subject_dedupes_equivalent_linked_slack_user_subjects(
        self,
    ) -> None:
        canonical = authorization_subject(type="subject", id="user:gestalt-123")
        canonical.properties.update({"email": "ada@example.com"})
        equivalent_user = authorization_subject(type="user", id="user:gestalt-123")
        authorization = FakeAuthorization([equivalent_user, canonical])
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
            gestalt.Request, "authorization", return_value=authorization, create=True
        ):
            resolved = provider_module.resolve_http_subject(
                gestalt.HTTPSubjectRequest(params=payload),
                gestalt.Request(),
            )

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved.id, "user:gestalt-123")

    def test_http_subject_defers_unlinked_slack_user_to_handler(self) -> None:
        authorization = FakeAuthorization([])
        payload = {
            "type": "event_callback",
            "event_id": "EvUnlinked",
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
            gestalt.Request, "authorization", return_value=authorization, create=True
        ):
            resolved = provider_module.resolve_http_subject(
                gestalt.HTTPSubjectRequest(params=payload),
                gestalt.Request(),
        )

        self.assertIsNone(resolved)
        self.assertEqual(len(authorization.requests), 1)

    def test_http_subject_resolves_plain_event_type_route_message(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "agent": {
                    "routes": [
                        {
                            "id": "all-channel-messages",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["message.channels"],
                            },
                        }
                    ]
                }
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        subject = authorization_subject(type="subject", id="user:gestalt-123")
        subject.properties.update({"email": "ada@example.com"})
        authorization = FakeAuthorization([subject])
        payload = {
            "type": "event_callback",
            "event_id": "EvPlainSubject",
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

        with mock.patch.object(
            gestalt.Request, "authorization", return_value=authorization, create=True
        ):
            resolved = provider_module.resolve_http_subject(
                gestalt.HTTPSubjectRequest(params=payload),
                gestalt.Request(),
            )

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved.id, "user:gestalt-123")
        self.assertEqual(len(authorization.requests), 1)

    def test_http_subject_uses_matching_route_run_as_before_linked_slack_user_lookup(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "agent": {
                    "routes": [
                        {
                            "id": "alert-bot-messages",
                            "runAs": {
                                "subject": {
                                    "id": "service_account:slack-bot",
                                    "displayName": "Platform Slack Bot",
                                },
                            },
                            "match": {
                                "channel": "C_ALERTS",
                                "eventTypes": ["message.channels"],
                                "botIds": ["B123"],
                                "thread": "root",
                            },
                        }
                    ]
                }
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        authorization = FakeAuthorization([])
        payload = {
            "type": "event_callback",
            "event_id": "EvBotRouteSubject",
            "team_id": "T123",
            "event": {
                "type": "message",
                "subtype": "bot_message",
                "bot_id": "B123",
                "user": "U_BOT_USER",
                "channel": "C_ALERTS",
                "channel_type": "channel",
                "text": "alert fired",
                "ts": "1712161829.000300",
            },
        }

        with mock.patch.object(
            gestalt.Request, "authorization", return_value=authorization, create=True
        ):
            resolved = provider_module.resolve_http_subject(
                gestalt.HTTPSubjectRequest(params=payload),
                gestalt.Request(),
            )

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved.id, "service_account:slack-bot")
        self.assertEqual(authorization.requests, [])

    def test_http_subject_uses_route_run_as_for_unaddressed_channel_root(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "agent": {
                    "routes": [
                        {
                            "id": "support-background",
                            "runAs": {
                                "subject": {
                                    "id": "service_account:eng-background-agent",
                                    "kind": "service_account",
                                    "displayName": "Engineering Background Agent",
                                },
                            },
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["message.channels"],
                                "thread": "root",
                                "addressedToBot": False,
                            },
                        },
                        {
                            "id": "support-mentions",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["app_mention"],
                                "thread": "any",
                            },
                        },
                    ]
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        authorization = FakeAuthorization([])
        payload = {
            "type": "event_callback",
            "event_id": "EvSupportRoot",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C_SUPPORT",
                "channel_type": "channel",
                "text": "please inspect this",
                "ts": "1712161829.000300",
            },
        }

        with mock.patch.object(
            gestalt.Request, "authorization", return_value=authorization, create=True
        ):
            resolved = provider_module.resolve_http_subject(
                gestalt.HTTPSubjectRequest(params=payload),
                gestalt.Request(),
            )

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved.id, "service_account:eng-background-agent")
        self.assertEqual(authorization.requests, [])

    def test_addressed_channel_message_skips_unaddressed_run_as_route(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "support-background",
                            "runAs": {
                                "subject": {
                                    "id": "service_account:eng-background-agent"
                                },
                            },
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["message.channels"],
                                "thread": "root",
                                "addressedToBot": False,
                            },
                        },
                        {
                            "id": "support-mentions",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["app_mention"],
                                "thread": "any",
                            },
                        },
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})

        response, workflow_client = self._handle_event_with_workflow(
            {
                "type": "event_callback",
                "event_id": "EvMentionDuplicateMessage",
                "team_id": "T123",
                "event": {
                    "type": "message",
                    "user": "U456",
                    "channel": "C_SUPPORT",
                    "channel_type": "channel",
                    "text": "<@UBOT> please inspect this",
                    "ts": "1712161829.000300",
                },
            }
        )

        self.assertEqual(
            operation_body(response), {"ok": True, "ignored": "no_matching_agent_route"}
        )
        self.assertEqual(workflow_client.deliver_event_requests, [])

    def test_app_mention_route_keeps_requester_subject_in_run_as_channel(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "agent": {
                    "routes": [
                        {
                            "id": "support-background",
                            "runAs": {
                                "subject": {
                                    "id": "service_account:eng-background-agent"
                                },
                            },
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["message.channels"],
                                "thread": "root",
                                "addressedToBot": False,
                            },
                        },
                        {
                            "id": "support-mentions",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["app_mention"],
                                "thread": "any",
                            },
                        },
                    ]
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        subject = authorization_subject(type="subject", id="user:gestalt-123")
        authorization = FakeAuthorization([subject])
        payload = {
            "type": "event_callback",
            "event_id": "EvSupportMention",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C_SUPPORT",
                "channel_type": "channel",
                "text": "<@UBOT> please inspect this",
                "ts": "1712161829.000300",
            },
        }

        with mock.patch.object(
            gestalt.Request, "authorization", return_value=authorization, create=True
        ):
            resolved = provider_module.resolve_http_subject(
                gestalt.HTTPSubjectRequest(params=payload),
                gestalt.Request(),
            )

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved.id, "user:gestalt-123")
        self.assertEqual(len(authorization.requests), 1)

    def test_http_subject_does_not_use_route_run_as_for_route_mismatches(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "agent": {
                    "routes": [
                        {
                            "id": "alert-bot-messages",
                            "runAs": {
                                "subject": {
                                    "id": "service_account:slack-bot",
                                },
                            },
                            "match": {
                                "channel": "C_ALERTS",
                                "eventTypes": ["message.channels"],
                                "botIds": ["B123"],
                                "thread": "root",
                            },
                        }
                    ]
                }
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        base_event = {
            "type": "message",
            "subtype": "bot_message",
            "bot_id": "B123",
            "user": "U_BOT_USER",
            "channel": "C_ALERTS",
            "channel_type": "channel",
            "text": "alert fired",
            "ts": "1712161829.000300",
        }
        cases = [
            ("wrong_channel", {"channel": "C_OTHER"}),
            ("wrong_bot_id", {"bot_id": "B999"}),
            ("thread_reply", {"thread_ts": "1712161800.000100"}),
        ]

        for name, overrides in cases:
            with self.subTest(name=name):
                authorization = FakeAuthorization([])
                event = {**base_event, **overrides}
                payload = {
                    "type": "event_callback",
                    "event_id": f"EvBotRouteMismatch{name}",
                    "team_id": "T123",
                    "event": event,
                }

                with mock.patch.object(
                    gestalt.Request, "authorization", return_value=authorization, create=True
                ):
                    resolved = provider_module.resolve_http_subject(
                        gestalt.HTTPSubjectRequest(params=payload),
                        gestalt.Request(),
                    )

                self.assertIsNone(resolved)
                self.assertEqual(authorization.requests, [])

    def test_http_subject_uses_signed_interaction_route_run_as_before_linked_slack_user_lookup(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "agent": {
                    "routes": [
                        {
                            "id": "alert-actions",
                            "runAs": {
                                "subject": {
                                    "id": "service_account:slack-bot",
                                    "kind": "service_account",
                                    "displayName": "Platform Slack Bot",
                                },
                            },
                            "match": {
                                "channel": "C_ALERTS",
                                "eventTypes": ["message.channels"],
                                "botIds": ["B123"],
                            },
                        }
                    ]
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        authorization = FakeAuthorization([])
        payload = {
            "payload": json.dumps(
                signed_route_block_action_payload(
                    "alert-actions",
                    subject_id="service_account:slack-bot",
                    channel_id="C_ALERTS",
                )
            )
        }

        with mock.patch.object(
            gestalt.Request, "authorization", return_value=authorization, create=True
        ):
            resolved = provider_module.resolve_http_subject(
                gestalt.HTTPSubjectRequest(params=payload),
                gestalt.Request(),
            )

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved.id, "service_account:slack-bot")
        self.assertEqual(authorization.requests, [])

    def test_http_subject_uses_linked_slack_user_for_user_signed_interaction_on_run_as_route(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "agent": {
                    "routes": [
                        {
                            "id": "alert-actions",
                            "runAs": {
                                "subject": {
                                    "id": "service_account:slack-bot",
                                    "kind": "service_account",
                                },
                            },
                            "match": {
                                "channel": "C_ALERTS",
                                "eventTypes": ["message.channels"],
                                "botIds": ["B123"],
                            },
                        }
                    ]
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        subject = authorization_subject(type="subject", id="user:gestalt-123")
        subject.properties.update({"email": "ada@example.com"})
        authorization = FakeAuthorization([subject])
        payload = {
            "payload": json.dumps(
                signed_route_block_action_payload(
                    "alert-actions",
                    subject_id="user:gestalt-123",
                    channel_id="C_ALERTS",
                )
            )
        }

        with mock.patch.object(
            gestalt.Request, "authorization", return_value=authorization, create=True
        ):
            resolved = provider_module.resolve_http_subject(
                gestalt.HTTPSubjectRequest(params=payload),
                gestalt.Request(),
            )

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved.id, "user:gestalt-123")
        self.assertEqual(len(authorization.requests), 1)

    def test_slack_event_handler_delivers_configured_workflow_event(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        payload = {
            "type": "event_callback",
            "event_id": "Ev123",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> summarize deploy status https://example.slack.com/archives/C123/p1712161800000100",
                "ts": "1712161829.000300",
                "files": [
                    {
                        "id": "F123",
                        "name": "diagram.png",
                        "mimetype": "image/png",
                        "size": 12,
                    }
                ],
            },
        }
        request = gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123"))

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(
            operation_body(response),
            {
                "ok": True,
                "delivered": True,
                "delivered_event_count": 1,
                "workflow_event_ids": [
                    "slack:event:T123:C789:1712161829.000300:U456"
                ],
                "route_ids": ["default"],
            },
        )
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)

        workflow_request = workflow_client.deliver_event_requests[0]
        self.assertEqual(workflow_request.provider_name, "local")
        expected_idempotency_key = "slack:event:T123:C789:1712161829.000300:U456"
        self.assertEqual(workflow_request.event.id, expected_idempotency_key)
        self.assertEqual(workflow_request.event.source, "slack")
        self.assertEqual(workflow_request.event.spec_version, "1.0")
        self.assertEqual(workflow_request.event.type, "slack.agent.event.received")
        self.assertEqual(workflow_request.event.subject, "route:default")

        event_data = delivered_event_data(workflow_request)
        self.assertEqual(
            event_data["workflowKey"], "slack:T123:C789:1712161829.000300"
        )
        self.assertEqual(event_data["routeId"], "default")
        agent_request = event_data["agent_request"]
        self.assertEqual(
            set(agent_request.keys()), {"kind", "user_prompt", "current_message"}
        )
        self.assertEqual(agent_request["kind"], "slack.event")
        self.assertEqual(agent_request["user_prompt"], event_data["user_prompt"])
        current_message = agent_request["current_message"]
        self.assertEqual(
            set(current_message.keys()),
            {
                "text",
                "user_id",
                "bot_id",
                "is_bot_event",
                "message_ts",
                "file_ids",
            },
        )
        self.assertEqual(current_message["user_id"], "U456")
        self.assertEqual(current_message["bot_id"], "")
        self.assertEqual(current_message["is_bot_event"], False)
        self.assertEqual(current_message["message_ts"], "1712161829.000300")
        self.assertEqual(current_message["file_ids"], ["F123"])
        self.assertEqual(event_data["slack"]["event_id"], "Ev123")
        self.assertEqual(event_data["slack"]["file_ids"], ["F123"])
        self.assertEqual(event_data["slack"]["addressed_to_bot"], True)
        self.assertEqual(event_data["slack"]["assistant_context_present"], False)
        self.assertEqual(
            event_data["slack"]["text"],
            "<@UBOT> summarize deploy status"
            " https://example.slack.com/archives/C123/p1712161800000100",
        )
        self.assertEqual(current_message["text"], event_data["slack"]["text"])
        self.assertIn(
            "operation: slack.conversations.getThreadContext",
            event_data["user_prompt"],
        )
        self.assertIn("Slack message permalink tools:", event_data["user_prompt"])
        self.assertIn(
            "- url: https://example.slack.com/archives/C123/p1712161800000100",
            event_data["user_prompt"],
        )
        self.assertIn(
            'input: {"url": "https://example.slack.com/archives/C123/p1712161800000100"}',
            event_data["user_prompt"],
        )
        self.assertIn(
            "id=F123 name=diagram.png mimetype=image/png size=12",
            event_data["user_prompt"],
        )
        self.assertIn(
            "reply_thread_ts: 1712161829.000300", event_data["user_prompt"]
        )
        reply_ref = event_data["reply_ref"]
        self.assertIn(f"reply_ref: {reply_ref}", event_data["user_prompt"])
        self.assertNotIn("Final reply tool:", event_data["user_prompt"])
        self.assertNotIn("operation: slack.events.reply", event_data["user_prompt"])
        verified_ref = provider_module._verify_reply_ref(reply_ref, "user:gestalt-123")
        self.assertEqual(verified_ref.team_id, "T123")
        self.assertEqual(verified_ref.channel_id, "C789")
        self.assertEqual(verified_ref.message_ts, "1712161829.000300")
        self.assertEqual(verified_ref.reply_thread_ts, "1712161829.000300")
        self.assertEqual(verified_ref.user_id, "U456")
        self.assertEqual(verified_ref.channel_type, "channel")
        self.assertEqual(verified_ref.subject_id, "user:gestalt-123")

        event_metadata = event_data["metadata"]
        self.assertEqual(event_metadata["slack"]["event_id"], "Ev123")
        self.assertEqual(event_metadata["slack"]["user_id"], "U456")
        self.assertEqual(event_metadata["slack"]["file_ids"], ["F123"])
        self.assertEqual(event_metadata["slack"]["addressed_to_bot"], True)

    def test_group_message_with_assistant_context_starts_agent_thread(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        payload = {
            "type": "event_callback",
            "event_id": "EvAssistantMessage",
            "team_id": "T123",
            "authorizations": [
                {"team_id": "T123", "user_id": "U0A8T4M41NY", "is_bot": True}
            ],
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C0AH7JWFYM8",
                "channel_type": "group",
                "text": "show me my linear tickets",
                "ts": "1777853873.601629",
                "client_msg_id": "163efdd3-cb7d-4348-92fc-e6e2815b2bcb",
                "assistant_thread": {"action_token": "xoxe-assistant"},
            },
        }
        request = gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123"))

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        workflow_request = workflow_client.deliver_event_requests[0]
        event_data = delivered_event_data(workflow_request)
        self.assertEqual(
            event_data["workflowKey"],
            "slack:T123:C0AH7JWFYM8:1777853873.601629",
        )
        self.assertEqual(
            event_data["slack"]["reply_thread_ts"], "1777853873.601629"
        )
        self.assertEqual(event_data["slack"]["addressed_to_bot"], True)
        self.assertEqual(event_data["slack"]["assistant_context_present"], True)
        self.assertEqual(event_data["slack"]["bot_user_id"], "U0A8T4M41NY")
        self.assertEqual(
            event_data["slack"]["client_msg_id"],
            "163efdd3-cb7d-4348-92fc-e6e2815b2bcb",
        )

    def test_authorized_bot_mention_starts_group_message(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        payload = {
            "type": "event_callback",
            "event_id": "EvAuthorizedMention",
            "team_id": "T123",
            "authorizations": [{"team_id": "T123", "user_id": "UBOT", "is_bot": True}],
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> show me my linear tickets",
                "ts": "1712161829.000300",
            },
        }
        request = gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123"))

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        workflow_request = workflow_client.deliver_event_requests[0]
        event_data = delivered_event_data(workflow_request)
        self.assertEqual(
            event_data["slack"]["reply_thread_ts"], "1712161829.000300"
        )
        self.assertEqual(event_data["slack"]["addressed_to_bot"], True)
        self.assertEqual(event_data["slack"]["assistant_context_present"], False)
        self.assertEqual(event_data["slack"]["bot_user_id"], "UBOT")

    def test_app_mention_and_message_event_share_agent_event_idempotency(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        base_event = {
            "user": "U456",
            "channel": "C789",
            "channel_type": "channel",
            "text": "<@UBOT> show me my linear tickets",
            "ts": "1712161829.000300",
            "client_msg_id": "163efdd3-cb7d-4348-92fc-e6e2815b2bcb",
        }
        app_mention = {
            "type": "event_callback",
            "event_id": "EvMention",
            "team_id": "T123",
            "event": {"type": "app_mention", **base_event},
        }
        message = {
            "type": "event_callback",
            "event_id": "EvMessage",
            "team_id": "T123",
            "event": {"type": "message", **base_event},
        }
        request = gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123"))

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            provider_module.slack_events_handle(app_mention, request)
            provider_module.slack_events_handle(message, request)

        self.assertEqual(len(workflow_client.deliver_event_requests), 2)
        first, second = workflow_client.deliver_event_requests
        expected_idempotency_key = "slack:event:T123:C789:1712161829.000300:U456"
        first_data = delivered_event_data(first)
        second_data = delivered_event_data(second)
        self.assertEqual(first_data["workflowKey"], "slack:T123:C789:1712161829.000300")
        self.assertEqual(second_data["workflowKey"], first_data["workflowKey"])
        self.assertEqual(first.event.id, expected_idempotency_key)
        self.assertEqual(second.event.id, expected_idempotency_key)
        first_metadata = first_data["metadata"]
        second_metadata = second_data["metadata"]
        self.assertEqual(first_metadata["slack"]["event_type"], "app_mention")
        self.assertEqual(second_metadata["slack"]["event_type"], "message")

    def test_slack_event_delivery_ack_does_not_read_provider_response(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = ExplodingDeliveryResponseWorkflowClient()
        payload = {
            "type": "event_callback",
            "event_id": "EvBadAckNoDelivery",
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
        request = gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123"))

        with mock.patch.object(
            gestalt.Request,
            "workflows",
            return_value=workflow_client,
            create=True,
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(
            operation_body(response),
            {
                "ok": True,
                "delivered": True,
                "delivered_event_count": 1,
                "workflow_event_ids": [
                    "slack:event:T123:C789:1712161829.000300:U456"
                ],
                "route_ids": ["default"],
            },
        )
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)

    def test_slack_event_delivery_failure_returns_server_error(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        workflow_client.deliver_event_error = RuntimeError("deliver failed")
        payload = {
            "type": "event_callback",
            "event_id": "EvDeliveryFailNoDelivery",
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
        request = gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123"))

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
            self.assertLogs(provider_module._agent.logger, level="INFO") as logs,
        ):
            result = provider_module.slack_events_handle(payload, request)

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(
            response.body,
            {"error": "failed to deliver workflow event: deliver failed"},
        )
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        raw_idempotency_key = "slack:event:T123:C789:1712161829.000300:U456"
        expected_hash = hashlib.sha256(raw_idempotency_key.encode()).hexdigest()
        log_text = "\n".join(logs.output)
        self.assertIn("delivering Slack agent workflow event", log_text)
        self.assertIn("failed to deliver Slack agent workflow event", log_text)
        self.assertIn(f"workflow_event_id_sha256={expected_hash}", log_text)
        self.assertIn("error_type=RuntimeError", log_text)
        self.assertIn("error=deliver failed", log_text)
        self.assertNotIn(raw_idempotency_key, log_text)
        self.assertNotIn("<@UBOT> summarize deploy status", log_text)

    def test_slack_event_handler_acks_unlinked_user_without_slack_io(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        payload = {
            "type": "event_callback",
            "event_id": "EvUnlinked",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> hello",
                "ts": "1712161829.000300",
            },
        }
        request = types.SimpleNamespace(
            subject=gestalt.Subject(id="system:http_binding:slack:events"),
            host=types.SimpleNamespace(public_base_url="https://gestalt.example.test/"),
        )
        with mock.patch(
            "internals.client.urllib.request.urlopen",
            side_effect=AssertionError("unexpected Slack notification"),
        ):
            response = provider_module.slack_events_handle(payload, cast(Any, request))

        self.assertEqual(operation_body(response), {"ok": True, "unlinked": True})

    def test_slack_event_handler_suppresses_unlinked_notice_for_plain_channel_route(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "agent": {
                    "routes": [
                        {
                            "id": "all-channel-messages",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["message.channels"],
                            },
                        }
                    ]
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        payload = {
            "type": "event_callback",
            "event_id": "EvUnlinkedPlainChannel",
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

        request = types.SimpleNamespace(
            subject=gestalt.Subject(id="system:http_binding:slack:events"),
            host=types.SimpleNamespace(public_base_url="https://gestalt.example.test/"),
        )
        with mock.patch(
            "internals.client.urllib.request.urlopen",
            side_effect=AssertionError("unexpected Slack notification"),
        ):
            response = provider_module.slack_events_handle(payload, cast(Any, request))

        self.assertEqual(operation_body(response), {"ok": True, "unlinked": True})

    def test_slack_event_handler_allows_configured_bot_route_system_subject(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "alert-bot-messages",
                            "match": {
                                "channel": "C_ALERTS",
                                "eventTypes": ["message.channels"],
                                "botIds": ["B123"],
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        payload = {
            "type": "event_callback",
            "event_id": "EvLinkedBotRoute",
            "team_id": "T123",
            "event": {
                "type": "message",
                "subtype": "bot_message",
                "bot_id": "B123",
                "channel": "C_ALERTS",
                "channel_type": "channel",
                "text": "alert fired",
                "ts": "1712161829.000300",
            },
        }
        workflow_client = FakeWorkflowClient()
        request = gestalt.Request(
            subject=gestalt.Subject(id="system:http_binding:slack:events")
        )

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
            mock.patch(
                "internals.client.urllib.request.urlopen",
                side_effect=AssertionError("unexpected Slack notification"),
            ),
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        workflow_request = workflow_client.deliver_event_requests[0]
        event_metadata = delivered_event_metadata(workflow_request)
        self.assertEqual(
            event_metadata["slack"]["agent_route_id"], "alert-bot-messages"
        )
        self.assertEqual(event_metadata["slack"]["bot_id"], "B123")

    def test_slack_event_handler_acks_unlinked_dm_route_without_slack_io(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "agent": {
                    "routes": [
                        {
                            "id": "direct-messages",
                            "match": {"eventTypes": ["message.im"]},
                        }
                    ]
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        payload = {
            "type": "event_callback",
            "event_id": "EvUnlinkedDm",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "D789",
                "channel_type": "im",
                "text": "hello agent",
                "ts": "1712161829.000300",
            },
        }
        request = types.SimpleNamespace(
            subject=gestalt.Subject(id="system:http_binding:slack:events"),
            host=types.SimpleNamespace(public_base_url="https://gestalt.example.test/"),
        )
        with mock.patch(
            "internals.client.urllib.request.urlopen",
            side_effect=AssertionError("unexpected Slack notification"),
        ):
            response = provider_module.slack_events_handle(payload, cast(Any, request))

        self.assertEqual(operation_body(response), {"ok": True, "unlinked": True})

    def test_slack_event_handler_requires_bot_token_before_delivering_workflow(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "workflow": {"provider": "local"},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        payload = {
            "type": "event_callback",
            "event_id": "EvNoBot",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> hello",
                "ts": "1712161829.000300",
            },
        }

        result = provider_module.slack_events_handle(
            payload,
            gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
        )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.PRECONDITION_FAILED)
        self.assertEqual(response.body, {"error": "Slack bot token is not configured"})

    def test_assistant_thread_started_delivers_workflow_event_without_slack_io(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "assistant": {
                    "enabled": True,
                    "suggestedPrompts": {
                        "title": "Try next",
                        "prompts": [
                            {
                                "title": "Summarize deploys",
                                "message": "Summarize the latest deploy status",
                            }
                        ],
                    }
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        payload = {
            "type": "event_callback",
            "event_id": "EvAssistantThreadStarted",
            "team_id": "T123",
            "event": {
                "type": "assistant_thread_started",
                "assistant_thread": {
                    "user_id": "U456",
                    "channel_id": "D789",
                    "thread_ts": "1712161829.000300",
                    "context": {"channel_id": "C789", "team_id": "T123"},
                },
                "event_ts": "1712161829.000400",
            },
        }
        workflow_client = FakeWorkflowClient()

        with (
            mock.patch(
                "internals.client.urllib.request.urlopen",
                side_effect=AssertionError("unexpected Slack assistant API call"),
            ),
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        event_data = delivered_event_data(workflow_client.deliver_event_requests[0])
        self.assertEqual(
            event_data["slack"]["event_type"], "assistant_thread_started"
        )
        self.assertEqual(event_data["slack"]["channel_id"], "D789")
        self.assertEqual(event_data["slack"]["reply_thread_ts"], "1712161829.000300")
        self.assertEqual(
            event_data["assistant"],
            {
                "enabled": True,
                "suggestedPrompts": {
                    "title": "Try next",
                    "prompts": [
                        {
                            "title": "Summarize deploys",
                            "message": "Summarize the latest deploy status",
                        }
                    ],
                },
            },
        )

    def test_assistant_thread_started_uses_route_specific_delivery_config(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "assistant": {
                    "suggestedPrompts": {
                        "prompts": [
                            {
                                "title": "Global prompt",
                                "message": "Use the global prompt",
                            }
                        ],
                    }
                },
                "agent": {
                    "routes": [
                        {
                            "id": "assistant-route",
                            "match": {
                                "channel": "C789",
                                "eventType": "assistant_thread_started",
                            },
                            "agent": {
                                "assistant": {
                                    "enabled": True,
                                    "suggestedPrompts": {
                                        "title": "Route prompts",
                                        "prompts": [
                                            {
                                                "title": "Route prompt",
                                                "message": "Use the route prompt",
                                            }
                                        ],
                                    },
                                }
                            },
                            "workflow": {
                                "provider": "route-provider",
                                "workflowEventType": "slack.route.assistant",
                                "subject": "route:assistant-route",
                            },
                        }
                    ]
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        payload = {
            "type": "event_callback",
            "event_id": "EvAssistantThreadStarted",
            "team_id": "T123",
            "event": {
                "type": "assistant_thread_started",
                "assistant_thread": {
                    "user_id": "U456",
                    "channel_id": "D789",
                    "thread_ts": "1712161829.000300",
                    "context": {"channel_id": "C789", "team_id": "T123"},
                },
                "event_ts": "1712161829.000400",
            },
        }
        workflow_client = FakeWorkflowClient()

        with (
            mock.patch(
                "internals.client.urllib.request.urlopen",
                side_effect=AssertionError("unexpected Slack assistant API call"),
            ),
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        workflow_request = workflow_client.deliver_event_requests[0]
        self.assertEqual(workflow_request.provider_name, "route-provider")
        self.assertEqual(workflow_request.event.type, "slack.route.assistant")
        self.assertEqual(workflow_request.event.subject, "route:assistant-route")
        event_data = delivered_event_data(workflow_request)
        self.assertEqual(
            event_data["metadata"]["slack"]["agent_route_id"], "assistant-route"
        )
        self.assertEqual(
            event_data["assistant"]["suggestedPrompts"]["prompts"],
            [{"title": "Route prompt", "message": "Use the route prompt"}],
        )
        self.assertEqual(
            event_data["assistant"]["suggestedPrompts"]["title"], "Route prompts"
        )

    def test_assistant_thread_started_route_can_disable_inherited_prompt_io(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "assistant": {
                    "suggestedPrompts": {
                        "prompts": [
                            {
                                "title": "Global prompt",
                                "message": "Use the global prompt",
                            }
                        ],
                    }
                },
                "agent": {
                    "routes": [
                        {
                            "id": "assistant-disabled-route",
                            "match": {
                                "channel": "C789",
                                "eventType": "assistant_thread_started",
                            },
                            "agent": {"assistant": {"enabled": False}},
                        }
                    ]
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        payload = {
            "type": "event_callback",
            "event_id": "EvAssistantThreadStarted",
            "team_id": "T123",
            "event": {
                "type": "assistant_thread_started",
                "assistant_thread": {
                    "user_id": "U456",
                    "channel_id": "D789",
                    "thread_ts": "1712161829.000300",
                    "context": {"channel_id": "C789", "team_id": "T123"},
                },
                "event_ts": "1712161829.000400",
            },
        }
        workflow_client = FakeWorkflowClient()

        with (
            mock.patch(
                "internals.client.urllib.request.urlopen",
                side_effect=AssertionError("unexpected Slack assistant API call"),
            ),
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        event_data = delivered_event_data(workflow_client.deliver_event_requests[0])
        self.assertEqual(
            event_data["assistant"],
            {"enabled": False, "suggestedPrompts": {"title": "", "prompts": []}},
        )

    def test_slack_events_reply_posts_with_bot_token_and_reply_ref_scope(self) -> None:
        provider_module.configure("slack", {"bot": {"token": "xoxb-test-bot"}})
        self.addCleanup(provider_module.configure, "slack", {})
        event = provider_module.SlackAgentEvent(
            callback_type="event_callback",
            event_type="app_mention",
            event_id="Ev123",
            team_id="T123",
            user_id="U456",
            channel_id="C789",
            channel_type="channel",
            text="<@UBOT> hello",
            message_ts="1712161829.000300",
            thread_ts="",
            reply_thread_ts="1712161829.000300",
        )
        reply_ref = provider_module._sign_reply_ref(event, "user:gestalt-123")
        captured: dict[str, Any] = {}
        idempotency_key = "workflow:local:run-123:output:event-batch-abc"
        expected_client_msg_id = str(
            uuid.UUID(
                hex=hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:32]
            )
        )

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(request.full_url, "https://slack.com/api/chat.postMessage")
            self.assertEqual(authorization_header(request), "Bearer xoxb-test-bot")
            captured["payload"] = json.loads(cast(bytes, request.data).decode("utf-8"))
            return FakeHTTPResponse(
                '{"ok": true, "channel": "C789", "ts": "1712161830.000400"}'
            )

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            result = provider_module.slack_events_reply(
                provider_module.SlackEventReplyInput(
                    reply_ref=reply_ref,
                    message=provider_module.SlackEventMessageInput(
                        fallback_text="Here is the answer"
                    ),
                ),
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123"),
                    idempotency_key=idempotency_key,
                ),
            )

        self.assertEqual(
            captured["payload"],
            {
                "channel": "C789",
                "text": "Here is the answer",
                "thread_ts": "1712161829.000300",
                "unfurl_links": False,
                "unfurl_media": False,
                "metadata": {
                    "event_type": "gestalt_message",
                    "event_payload": {"sent_with": "gestalt"},
                },
                "client_msg_id": expected_client_msg_id,
            },
        )
        self.assertEqual(
            result,
            {
                "ok": True,
                "channel": "C789",
                "ts": "1712161830.000400",
                "thread_ts": "1712161829.000300",
            },
        )

        denied = provider_module.slack_events_reply(
            provider_module.SlackEventReplyInput(
                reply_ref=reply_ref,
                message=provider_module.SlackEventMessageInput(
                    fallback_text="wrong subject"
                ),
            ),
            gestalt.Request(subject=gestalt.Subject(id="user:other")),
        )
        self.assertIsInstance(denied, gestalt.Response)
        denied_response = cast(gestalt.Response[dict[str, str]], denied)
        self.assertEqual(denied_response.status, HTTPStatus.FORBIDDEN)
        self.assertEqual(
            denied_response.body,
            {"error": "reply_ref does not belong to this subject"},
        )

    def test_slack_events_reply_renders_structured_message_blocks(self) -> None:
        provider_module.configure("slack", {"bot": {"token": "xoxb-test-bot"}})
        self.addCleanup(provider_module.configure, "slack", {})
        reply_ref = self._signed_reply_ref(subject_id="user:gestalt-123")
        captured: dict[str, Any] = {}

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(request.full_url, "https://slack.com/api/chat.postMessage")
            captured["payload"] = json.loads(cast(bytes, request.data).decode("utf-8"))
            return FakeHTTPResponse(
                '{"ok": true, "channel": "C789", "ts": "1712161830.000400"}'
            )

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            result = provider_module.slack_events_reply(
                provider_module.SlackEventReplyInput(
                    reply_ref=reply_ref,
                    message=provider_module.SlackEventMessageInput(
                        fallback_text="Diagnosis: webhook auth failed. Next: rotate the secret.",
                        sections=[
                            provider_module.SlackEventMessageSectionInput(
                                heading="Diagnosis",
                                body="Webhook auth failed because requests use the old secret.",
                            ),
                            provider_module.SlackEventMessageSectionInput(
                                heading="Next",
                                body="Rotate the Slack signing secret and redeploy.",
                            ),
                        ],
                        metadata={},
                        unfurl_links=True,
                    ),
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertEqual(operation_body(result)["ok"], True)
        self.assertEqual(
            captured["payload"],
            {
                "channel": "C789",
                "text": "Diagnosis: webhook auth failed. Next: rotate the secret.",
                "thread_ts": "1712161829.000300",
                "unfurl_links": True,
                "unfurl_media": False,
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*Diagnosis*\nWebhook auth failed because requests use the old secret.",
                        },
                    },
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*Next*\nRotate the Slack signing secret and redeploy.",
                        },
                    },
                ],
            },
        )

    def test_slack_events_reply_rejects_invalid_structured_messages(self) -> None:
        provider_module.configure("slack", {"bot": {"token": "xoxb-test-bot"}})
        self.addCleanup(provider_module.configure, "slack", {})
        reply_ref = self._signed_reply_ref(subject_id="user:gestalt-123")

        blank_fallback = provider_module.slack_events_reply(
            provider_module.SlackEventReplyInput(
                reply_ref=reply_ref,
                message=provider_module.SlackEventMessageInput(fallback_text=" "),
            ),
            gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
        )
        self.assertEqual(
            cast(gestalt.Response[dict[str, str]], blank_fallback).body,
            {"error": "message.fallback_text is required"},
        )

        ambiguous_body = provider_module.slack_events_reply(
            provider_module.SlackEventReplyInput(
                reply_ref=reply_ref,
                message=provider_module.SlackEventMessageInput(
                    fallback_text="fallback",
                    body="body",
                    sections=[
                        provider_module.SlackEventMessageSectionInput(body="section")
                    ],
                ),
            ),
            gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
        )
        self.assertEqual(
            cast(gestalt.Response[dict[str, str]], ambiguous_body).body,
            {
                "error": "message.body cannot be combined with message.sections"
            },
        )

    def test_slack_events_reply_allows_matching_workflow_event_context_for_invocation_subject(
        self,
    ) -> None:
        provider_module.configure("slack", {"bot": {"token": "xoxb-test-bot"}})
        self.addCleanup(provider_module.configure, "slack", {})
        reply_ref = self._signed_reply_ref(subject_id="user:gestalt-123")
        captured: dict[str, Any] = {}

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.full_url, "https://slack.com/api/chat.postMessage")
            self.assertEqual(authorization_header(request), "Bearer xoxb-test-bot")
            captured["payload"] = json.loads(cast(bytes, request.data).decode("utf-8"))
            return FakeHTTPResponse(
                '{"ok": true, "channel": "C789", "ts": "1712161830.000400"}'
            )

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            result = provider_module.slack_events_reply(
                provider_module.SlackEventReplyInput(
                    reply_ref=reply_ref,
                    message=provider_module.SlackEventMessageInput(
                        fallback_text="workflow answer"
                    ),
                ),
                gestalt.Request(
                    subject=gestalt.Subject(id="user:workflow-invoker"),
                    workflow=self._slack_agent_workflow_context(reply_ref),
                ),
            )

        self.assertEqual(captured["payload"]["channel"], "C789")
        self.assertEqual(captured["payload"]["thread_ts"], "1712161829.000300")
        self.assertEqual(operation_body(result)["ok"], True)

    def test_slack_events_reply_allows_custom_workflow_event_type_context(
        self,
    ) -> None:
        provider_module.configure("slack", {"bot": {"token": "xoxb-test-bot"}})
        self.addCleanup(provider_module.configure, "slack", {})
        reply_ref = self._signed_reply_ref(subject_id="user:gestalt-123")
        captured: dict[str, Any] = {}

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.full_url, "https://slack.com/api/chat.postMessage")
            self.assertEqual(authorization_header(request), "Bearer xoxb-test-bot")
            captured["payload"] = json.loads(cast(bytes, request.data).decode("utf-8"))
            return FakeHTTPResponse(
                '{"ok": true, "channel": "C789", "ts": "1712161830.000400"}'
            )

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            result = provider_module.slack_events_reply(
                provider_module.SlackEventReplyInput(
                    reply_ref=reply_ref,
                    message=provider_module.SlackEventMessageInput(
                        fallback_text="custom workflow answer"
                    ),
                ),
                gestalt.Request(
                    subject=gestalt.Subject(id="user:workflow-invoker"),
                    workflow=self._slack_agent_workflow_context(
                        reply_ref,
                        event_type="slack.route.agent.received",
                    ),
                ),
            )

        self.assertEqual(captured["payload"]["channel"], "C789")
        self.assertEqual(captured["payload"]["thread_ts"], "1712161829.000300")
        self.assertEqual(operation_body(result)["ok"], True)

    def test_slack_events_reply_allows_matching_interaction_workflow_event_context(
        self,
    ) -> None:
        provider_module.configure("slack", {"bot": {"token": "xoxb-test-bot"}})
        self.addCleanup(provider_module.configure, "slack", {})
        reply_ref = self._signed_reply_ref(subject_id="user:gestalt-123")
        captured: dict[str, Any] = {}

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.full_url, "https://slack.com/api/chat.postMessage")
            self.assertEqual(authorization_header(request), "Bearer xoxb-test-bot")
            captured["payload"] = json.loads(cast(bytes, request.data).decode("utf-8"))
            return FakeHTTPResponse(
                '{"ok": true, "channel": "C789", "ts": "1712161830.000400"}'
            )

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            result = provider_module.slack_events_reply(
                provider_module.SlackEventReplyInput(
                    reply_ref=reply_ref,
                    message=provider_module.SlackEventMessageInput(
                        fallback_text="interaction workflow answer"
                    ),
                ),
                gestalt.Request(
                    subject=gestalt.Subject(id="user:workflow-invoker"),
                    workflow=self._slack_agent_workflow_context(
                        reply_ref,
                        event_type=(
                            provider_module._agent.SLACK_INTERACTION_WORKFLOW_EVENT_TYPE
                        ),
                        slack_fields={
                            "container_message_ts": "1712161831.000500",
                        },
                    ),
                ),
            )

        self.assertEqual(captured["payload"]["channel"], "C789")
        self.assertEqual(captured["payload"]["thread_ts"], "1712161829.000300")
        self.assertEqual(operation_body(result)["ok"], True)

    def test_slack_events_reply_rejects_mismatched_workflow_event_context(
        self,
    ) -> None:
        provider_module.configure("slack", {"bot": {"token": "xoxb-test-bot"}})
        self.addCleanup(provider_module.configure, "slack", {})
        reply_ref = self._signed_reply_ref(subject_id="user:gestalt-123")

        result = provider_module.slack_events_reply(
            provider_module.SlackEventReplyInput(
                reply_ref=reply_ref,
                message=provider_module.SlackEventMessageInput(
                    fallback_text="wrong workflow"
                ),
            ),
            gestalt.Request(
                subject=gestalt.Subject(id="user:workflow-invoker"),
                workflow=self._slack_agent_workflow_context(
                    reply_ref, channel_id="C_OTHER"
                ),
            ),
        )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertEqual(
            response.body,
            {"error": "reply_ref does not belong to this subject"},
        )

    def test_slack_events_reply_session_started_posts_session_link(self) -> None:
        provider_module.configure("slack", {"bot": {"token": "xoxb-test-bot"}})
        self.addCleanup(provider_module.configure, "slack", {})
        event = provider_module.SlackAgentEvent(
            callback_type="event_callback",
            event_type="app_mention",
            event_id="Ev123",
            team_id="T123",
            user_id="U456",
            channel_id="C789",
            channel_type="channel",
            text="<@UBOT> hello",
            message_ts="1712161829.000300",
            thread_ts="",
            reply_thread_ts="1712161829.000300",
        )
        reply_ref = provider_module._sign_reply_ref(event, "user:gestalt-123")
        captured: dict[str, Any] = {}
        idempotency_key = "workflow:local:run-123:session-ready:event-batch-abc"
        expected_client_msg_id = str(
            uuid.UUID(
                hex=hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:32]
            )
        )

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(request.full_url, "https://slack.com/api/chat.postMessage")
            self.assertEqual(authorization_header(request), "Bearer xoxb-test-bot")
            captured["payload"] = json.loads(cast(bytes, request.data).decode("utf-8"))
            return FakeHTTPResponse(
                '{"ok": true, "channel": "C789", "ts": "1712161830.000400"}'
            )

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            result = provider_module.slack_events_reply_session_started(
                provider_module.SlackEventSessionStartedInput(
                    reply_ref=reply_ref, session_id="agent session/123"
                ),
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123"),
                    host=gestalt.Host(public_base_url="https://gestalt.example.test/"),
                    idempotency_key=idempotency_key,
                ),
            )

        expected_url = agent_session_url(
            "https://gestalt.example.test/",
            "agent session/123",
        )
        self.assertEqual(
            captured["payload"],
            {
                "channel": "C789",
                "text": f"Started a Gestalt session: <{expected_url}|open session>",
                "thread_ts": "1712161829.000300",
                "unfurl_links": False,
                "unfurl_media": False,
                "client_msg_id": expected_client_msg_id,
            },
        )
        self.assertEqual(
            result,
            {
                "ok": True,
                "channel": "C789",
                "ts": "1712161830.000400",
                "thread_ts": "1712161829.000300",
                "session_url": expected_url,
            },
        )

        missing_base_url = provider_module.slack_events_reply_session_started(
            provider_module.SlackEventSessionStartedInput(
                reply_ref=reply_ref, session_id="agent-session-123"
            ),
            gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
        )
        self.assertIsInstance(missing_base_url, gestalt.Response)
        missing_base_url_response = cast(
            gestalt.Response[dict[str, str]], missing_base_url
        )
        self.assertEqual(
            missing_base_url_response.status, HTTPStatus.PRECONDITION_FAILED
        )
        self.assertEqual(
            missing_base_url_response.body,
            {"error": "host.public_base_url is required"},
        )

    def test_slack_events_reply_session_started_skips_thread_replies(self) -> None:
        provider_module.configure("slack", {"bot": {"token": "xoxb-test-bot"}})
        self.addCleanup(provider_module.configure, "slack", {})
        event = provider_module.SlackAgentEvent(
            callback_type="event_callback",
            event_type="app_mention",
            event_id="Ev123",
            team_id="T123",
            user_id="U456",
            channel_id="C789",
            channel_type="channel",
            text="<@UBOT> follow up",
            message_ts="1712161835.000500",
            thread_ts="1712161829.000300",
            reply_thread_ts="1712161829.000300",
        )
        reply_ref = provider_module._sign_reply_ref(event, "user:gestalt-123")

        with mock.patch(
            "internals.client.urllib.request.urlopen",
            side_effect=AssertionError("thread replies should not post session links"),
        ):
            result = provider_module.slack_events_reply_session_started(
                provider_module.SlackEventSessionStartedInput(
                    reply_ref=reply_ref, session_id="agent-session-123"
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertEqual(
            result,
            {
                "ok": True,
                "skipped": True,
                "reason": "thread_reply",
                "thread_ts": "1712161829.000300",
            },
        )

    def test_slack_interaction_request_posts_buttons_and_handler_delivers_workflow_event(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        event = provider_module.SlackAgentEvent(
            callback_type="event_callback",
            event_type="app_mention",
            event_id="Ev123",
            team_id="T123",
            user_id="U456",
            channel_id="C789",
            channel_type="channel",
            text="<@UBOT> deploy?",
            message_ts="1712161829.000300",
            thread_ts="",
            reply_thread_ts="1712161829.000300",
        )
        reply_ref = provider_module._sign_reply_ref(event, "user:gestalt-123")
        captured: dict[str, Any] = {}

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.full_url, "https://slack.com/api/chat.postMessage")
            self.assertEqual(authorization_header(request), "Bearer xoxb-test-bot")
            captured["payload"] = json.loads(cast(bytes, request.data).decode("utf-8"))
            return FakeHTTPResponse(
                '{"ok": true, "channel": "C789", "ts": "1712161831.000500"}'
            )

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            request_result = provider_module.slack_interactions_request(
                provider_module.SlackInteractionRequestInput(
                    reply_ref=reply_ref,
                    text="Approve deployment?",
                    actions=[
                        provider_module.SlackInteractionActionInput(
                            action_id="approve",
                            label="Approve",
                            value="approved",
                            style="primary",
                        )
                    ],
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertEqual(request_result["ok"], True)
        self.assertEqual(
            request_result["workflow_key"], "slack:T123:C789:1712161829.000300"
        )
        post_payload = captured["payload"]
        self.assertEqual(post_payload["channel"], "C789")
        self.assertEqual(post_payload["thread_ts"], "1712161829.000300")
        self.assertEqual(post_payload["text"], "Approve deployment?")
        button = post_payload["blocks"][1]["elements"][0]
        self.assertEqual(button["action_id"], "approve")
        self.assertEqual(button["style"], "primary")
        interaction_ref = button["value"]

        workflow_client = FakeWorkflowClient()
        interaction_payload = {
            "type": "block_actions",
            "team": {"id": "T123"},
            "user": {"id": "U456"},
            "channel": {"id": "C789"},
            "container": {
                "type": "message",
                "channel_id": "C789",
                "message_ts": "1712161831.000500",
            },
            "trigger_id": "1337.abcdef",
            "actions": [
                {
                    "action_id": "approve",
                    "value": interaction_ref,
                    "action_ts": "1712161832.000600",
                }
            ],
        }

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_interactions_handle(
                {"payload": json.dumps(interaction_payload)},
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        body = operation_body(response)
        self.assertEqual(body["ok"], True)
        self.assertEqual(body["delivered"], True)
        self.assertEqual(body["delivered_event_count"], 1)
        self.assertEqual(body["route_ids"], ["default"])
        self.assertEqual(body["action_id"], "approve")
        self.assertEqual(
            body["workflow_event_ids"],
            [workflow_client.deliver_event_requests[0].event.id],
        )
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        workflow_request = workflow_client.deliver_event_requests[0]
        self.assertEqual(workflow_request.provider_name, "local")
        self.assertEqual(workflow_request.event.source, "slack")
        self.assertEqual(workflow_request.event.type, "slack.agent.interaction.received")
        self.assertEqual(workflow_request.event.subject, "route:default")
        self.assertTrue(workflow_request.event.id.startswith("slack:interaction:"))
        event_data = delivered_event_data(workflow_request)
        self.assertEqual(
            event_data["workflowKey"], "slack:T123:C789:1712161829.000300"
        )
        self.assertEqual(event_data["slack"]["action_id"], "approve")
        self.assertEqual(event_data["slack"]["action_value"], "approved")
        self.assertEqual(event_data["slack"]["message_ts"], "1712161829.000300")
        self.assertEqual(
            event_data["slack"]["container_message_ts"], "1712161831.000500"
        )
        self.assertEqual(event_data["slack"]["trigger_id"], "1337.abcdef")
        self.assertIn("reply_ref: ", event_data["user_prompt"])
        self.assertNotIn("Final reply tool:", event_data["user_prompt"])
        self.assertNotIn("operation: slack.events.reply", event_data["user_prompt"])
        self.assertNotIn("Native assistant status tool:", event_data["user_prompt"])

        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {},
                "assistant": {"enabled": True},
            },
        )
        workflow_client = FakeWorkflowClient()
        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_interactions_handle(
                {"payload": json.dumps(interaction_payload)},
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        workflow_request = workflow_client.deliver_event_requests[0]
        event_data = delivered_event_data(workflow_request)
        self.assertNotIn("Native assistant status tool:", event_data["user_prompt"])

    def test_slack_interaction_request_uses_route_delivery_config(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "templated-route",
                            "match": {"channel": "C_ROUTE"},
                            "workflow": {
                                "provider": "route-provider",
                                "workflowInteractionEventType": (
                                    "slack.route.interaction.received"
                                ),
                                "interactionSubject": "route:templated-route",
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        event = provider_module.SlackAgentEvent(
            callback_type="event_callback",
            event_type="app_mention",
            event_id="EvRouteButtons",
            team_id="T123",
            user_id="U456",
            channel_id="C_ROUTE",
            channel_type="channel",
            text="<@UBOT> deploy?",
            message_ts="1712161829.000300",
            thread_ts="",
            reply_thread_ts="1712161829.000300",
        )
        route = provider_module._agent._agent_route_by_id("templated-route")
        reply_ref = provider_module._sign_reply_ref(event, "user:gestalt-123", route)
        captured: dict[str, Any] = {}

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.full_url, "https://slack.com/api/chat.postMessage")
            captured["payload"] = json.loads(cast(bytes, request.data).decode("utf-8"))
            return FakeHTTPResponse(
                '{"ok": true, "channel": "C_ROUTE", "ts": "1712161831.000500"}'
            )

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            request_result = provider_module.slack_interactions_request(
                provider_module.SlackInteractionRequestInput(
                    reply_ref=reply_ref,
                    text="Approve deployment?",
                    actions=[
                        provider_module.SlackInteractionActionInput(
                            action_id="approve",
                            label="Approve",
                            value="approved",
                        )
                    ],
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        workflow_key = "slack:T123:C_ROUTE:1712161829.000300"
        self.assertEqual(request_result["workflow_key"], workflow_key)
        interaction_ref = captured["payload"]["blocks"][1]["elements"][0]["value"]
        workflow_client = FakeWorkflowClient()
        interaction_payload = {
            "type": "block_actions",
            "team": {"id": "T123"},
            "user": {"id": "U456"},
            "channel": {"id": "C_ROUTE"},
            "container": {
                "type": "message",
                "channel_id": "C_ROUTE",
                "message_ts": "1712161831.000500",
            },
            "trigger_id": "1337.abcdef",
            "actions": [
                {
                    "action_id": "approve",
                    "value": interaction_ref,
                    "action_ts": "1712161832.000600",
                }
            ],
        }

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_interactions_handle(
                {"payload": json.dumps(interaction_payload)},
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        workflow_request = workflow_client.deliver_event_requests[0]
        self.assertEqual(workflow_request.provider_name, "route-provider")
        self.assertEqual(
            workflow_request.event.type, "slack.route.interaction.received"
        )
        self.assertEqual(workflow_request.event.subject, "route:templated-route")
        event_data = delivered_event_data(workflow_request)
        self.assertEqual(event_data["workflowKey"], workflow_key)
        self.assertEqual(event_data["routeId"], "templated-route")

    def test_slack_interaction_delivery_ack_does_not_read_provider_response(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = ExplodingDeliveryResponseWorkflowClient()
        interaction_payload = signed_block_action_payload()

        with mock.patch.object(
            gestalt.Request,
            "workflows",
            return_value=workflow_client,
            create=True,
        ):
            response = provider_module.slack_interactions_handle(
                {"payload": json.dumps(interaction_payload)},
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        body = operation_body(response)
        self.assertEqual(body["ok"], True)
        self.assertEqual(body["delivered"], True)
        self.assertEqual(body["delivered_event_count"], 1)
        self.assertEqual(body["workflow_event_ids"], [workflow_client.deliver_event_requests[0].event.id])
        self.assertEqual(body["route_ids"], ["default"])
        self.assertEqual(body["action_id"], "approve")
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)

    def test_slack_interaction_delivery_failure_returns_server_error(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        workflow_client.deliver_event_error = RuntimeError("deliver failed")
        interaction_payload = signed_block_action_payload()

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
            self.assertLogs(provider_module._agent.logger, level="INFO") as logs,
        ):
            result = provider_module.slack_interactions_handle(
                {"payload": json.dumps(interaction_payload)},
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(
            response.body,
            {"error": "failed to deliver workflow event: deliver failed"},
        )
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        workflow_request = workflow_client.deliver_event_requests[0]
        raw_idempotency_key = workflow_request.event.id
        expected_hash = hashlib.sha256(raw_idempotency_key.encode()).hexdigest()
        raw_interaction_ref = interaction_payload["actions"][0]["value"]
        log_text = "\n".join(logs.output)
        self.assertIn("delivering Slack interaction workflow event", log_text)
        self.assertIn("failed to deliver Slack interaction workflow event", log_text)
        self.assertIn(f"workflow_event_id_sha256={expected_hash}", log_text)
        self.assertIn("error_type=RuntimeError", log_text)
        self.assertIn("error=deliver failed", log_text)
        self.assertNotIn(raw_idempotency_key, log_text)
        self.assertNotIn(raw_interaction_ref, log_text)
        self.assertNotIn("response_url", log_text)
        self.assertNotIn("<@UBOT> approve deployment", log_text)

    def test_slack_interaction_workflow_client_failure_returns_server_error(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        interaction_payload = signed_block_action_payload()

        with mock.patch.object(
            gestalt.Request,
            "workflows",
            side_effect=RuntimeError("workflow client unavailable"),
            create=True,
        ):
            result = provider_module.slack_interactions_handle(
                {"payload": json.dumps(interaction_payload)},
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(
            response.body,
            {"error": "failed to deliver workflow event: workflow client unavailable"},
        )

    def test_slack_event_status_and_reactions_use_reply_ref_contract(self) -> None:
        provider_module.configure("slack", {"bot": {"token": "xoxb-test-bot"}})
        self.addCleanup(provider_module.configure, "slack", {})
        event = provider_module.SlackAgentEvent(
            callback_type="event_callback",
            event_type="app_mention",
            event_id="Ev123",
            team_id="T123",
            user_id="U456",
            channel_id="C789",
            channel_type="channel",
            text="<@UBOT> hello",
            message_ts="1712161829.000300",
            thread_ts="",
            reply_thread_ts="1712161829.000300",
        )
        reply_ref = provider_module._sign_reply_ref(event, "user:gestalt-123")
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(authorization_header(request), "Bearer xoxb-test-bot")
            parsed = urllib.parse.urlsplit(request.full_url)
            payload = json.loads(cast(bytes, request.data).decode("utf-8"))
            calls.append((parsed.path, payload))
            if parsed.path == "/api/chat.postMessage":
                return FakeHTTPResponse(
                    '{"ok": true, "channel": "C789", "ts": "1712161830.000400"}'
                )
            if parsed.path == "/api/chat.update":
                return FakeHTTPResponse(
                    '{"ok": true, "channel": "C789", "ts": "1712161830.000400"}'
                )
            if parsed.path == "/api/chat.delete":
                return FakeHTTPResponse(
                    '{"ok": true, "channel": "C789", "ts": "1712161830.000400"}'
                )
            if parsed.path in {"/api/reactions.add", "/api/reactions.remove"}:
                return FakeHTTPResponse('{"ok": true}')
            if parsed.path in {
                "/api/assistant.threads.setStatus",
                "/api/assistant.threads.setTitle",
                "/api/assistant.threads.setSuggestedPrompts",
            }:
                return FakeHTTPResponse('{"ok": true}')
            if parsed.path in {"/api/chat.startStream", "/api/chat.appendStream"}:
                return FakeHTTPResponse(
                    '{"ok": true, "channel": "C789", "ts": "1712161831.000500"}'
                )
            if parsed.path == "/api/chat.stopStream":
                return FakeHTTPResponse(
                    """
                    {
                      "ok": true,
                      "channel": "C789",
                      "ts": "1712161831.000500",
                      "message": {"type": "message", "text": "Done"}
                    }
                    """
                )
            raise AssertionError(f"unexpected request {request.full_url}")

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            created = provider_module.slack_events_set_status(
                provider_module.SlackEventStatusInput(
                    reply_ref=reply_ref,
                    text="Working on it",
                    unfurl_links=True,
                    unfurl_media=False,
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )
            updated = provider_module.slack_events_set_status(
                provider_module.SlackEventStatusInput(
                    reply_ref=reply_ref,
                    text="Still working",
                    status_ts="1712161830.000400",
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )
            added = provider_module.slack_events_add_reaction(
                provider_module.SlackEventReactionInput(
                    reply_ref=reply_ref,
                    name="eyes",
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )
            removed = provider_module.slack_events_remove_reaction(
                provider_module.SlackEventReactionInput(
                    reply_ref=reply_ref,
                    name=":eyes:",
                    target_ts="1712161830.000400",
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )
            deleted = provider_module.slack_events_delete_status(
                provider_module.SlackEventDeleteStatusInput(
                    reply_ref=reply_ref,
                    status_ts="1712161830.000400",
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )
            assistant_status = provider_module.slack_events_set_assistant_status(
                provider_module.SlackEventAssistantStatusInput(
                    reply_ref=reply_ref,
                    status="is checking deployment status",
                    loading_messages=["Reading the thread", "Checking deploys"],
                    icon_emoji=":hourglass_flowing_sand:",
                    username="Example Assistant",
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )
            cleared_assistant_status = (
                provider_module.slack_events_clear_assistant_status(
                    provider_module.SlackEventReplyRefInput(reply_ref=reply_ref),
                    gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
                )
            )
            title = provider_module.slack_events_set_thread_title(
                provider_module.SlackEventThreadTitleInput(
                    reply_ref=reply_ref,
                    title="Deploy status",
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )
            prompts = provider_module.slack_events_set_suggested_prompts(
                provider_module.SlackEventSuggestedPromptsInput(
                    reply_ref=reply_ref,
                    title="Try next",
                    prompts=[
                        {
                            "title": "Summarize deploys",
                            "message": "Summarize the latest deploy status",
                        }
                    ],
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )
            stream = provider_module.slack_events_start_stream(
                provider_module.SlackEventStreamStartInput(
                    reply_ref=reply_ref,
                    markdown_text="Starting deploy checks",
                    chunks=[
                        {
                            "type": "task_update",
                            "id": "check-deploys",
                            "title": "Check deploy status",
                            "status": "in_progress",
                        }
                    ],
                    task_display_mode="plan",
                    icon_emoji=":hourglass_flowing_sand:",
                    username="Example Assistant",
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )
            appended = provider_module.slack_events_append_stream(
                provider_module.SlackEventStreamAppendInput(
                    reply_ref=reply_ref,
                    stream_ts="1712161831.000500",
                    markdown_text="Still checking",
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )
            stopped = provider_module.slack_events_stop_stream(
                provider_module.SlackEventStreamStopInput(
                    reply_ref=reply_ref,
                    stream_ts="1712161831.000500",
                    markdown_text="Done",
                    blocks=[
                        {
                            "type": "section",
                            "text": {
                                "type": "mrkdwn",
                                "text": "Deploy status complete",
                            },
                        }
                    ],
                    metadata={
                        "event_type": "deploy_status",
                        "event_payload": {"source": "test"},
                    },
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertEqual(
            calls,
            [
                (
                    "/api/chat.postMessage",
                    {
                        "channel": "C789",
                        "text": "Working on it",
                        "thread_ts": "1712161829.000300",
                        "unfurl_links": True,
                        "unfurl_media": False,
                    },
                ),
                (
                    "/api/chat.update",
                    {
                        "channel": "C789",
                        "ts": "1712161830.000400",
                        "text": "Still working",
                    },
                ),
                (
                    "/api/reactions.add",
                    {
                        "channel": "C789",
                        "timestamp": "1712161829.000300",
                        "name": "eyes",
                    },
                ),
                (
                    "/api/reactions.remove",
                    {
                        "channel": "C789",
                        "timestamp": "1712161830.000400",
                        "name": "eyes",
                    },
                ),
                (
                    "/api/chat.delete",
                    {
                        "channel": "C789",
                        "ts": "1712161830.000400",
                    },
                ),
                (
                    "/api/assistant.threads.setStatus",
                    {
                        "channel_id": "C789",
                        "thread_ts": "1712161829.000300",
                        "status": "is checking deployment status",
                        "loading_messages": [
                            "Reading the thread",
                            "Checking deploys",
                        ],
                        "icon_emoji": ":hourglass_flowing_sand:",
                        "username": "Example Assistant",
                    },
                ),
                (
                    "/api/assistant.threads.setStatus",
                    {
                        "channel_id": "C789",
                        "thread_ts": "1712161829.000300",
                        "status": "",
                    },
                ),
                (
                    "/api/assistant.threads.setTitle",
                    {
                        "channel_id": "C789",
                        "thread_ts": "1712161829.000300",
                        "title": "Deploy status",
                    },
                ),
                (
                    "/api/assistant.threads.setSuggestedPrompts",
                    {
                        "channel_id": "C789",
                        "thread_ts": "1712161829.000300",
                        "prompts": [
                            {
                                "title": "Summarize deploys",
                                "message": "Summarize the latest deploy status",
                            }
                        ],
                        "title": "Try next",
                    },
                ),
                (
                    "/api/chat.startStream",
                    {
                        "channel": "C789",
                        "thread_ts": "1712161829.000300",
                        "markdown_text": "Starting deploy checks",
                        "chunks": [
                            {
                                "type": "task_update",
                                "id": "check-deploys",
                                "title": "Check deploy status",
                                "status": "in_progress",
                            }
                        ],
                        "recipient_user_id": "U456",
                        "recipient_team_id": "T123",
                        "task_display_mode": "plan",
                        "icon_emoji": ":hourglass_flowing_sand:",
                        "username": "Example Assistant",
                    },
                ),
                (
                    "/api/chat.appendStream",
                    {
                        "channel": "C789",
                        "ts": "1712161831.000500",
                        "markdown_text": "Still checking",
                    },
                ),
                (
                    "/api/chat.stopStream",
                    {
                        "channel": "C789",
                        "ts": "1712161831.000500",
                        "markdown_text": "Done",
                        "blocks": [
                            {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": "Deploy status complete",
                                },
                            }
                        ],
                        "metadata": {
                            "event_type": "deploy_status",
                            "event_payload": {"source": "test"},
                        },
                    },
                ),
            ],
        )
        self.assertEqual(created["created"], True)
        self.assertEqual(created["status_ts"], "1712161830.000400")
        self.assertEqual(updated["created"], False)
        self.assertEqual(added["target_ts"], "1712161829.000300")
        self.assertEqual(removed["removed"], True)
        self.assertEqual(deleted["deleted_ts"], "1712161830.000400")
        self.assertEqual(assistant_status["thread_ts"], "1712161829.000300")
        self.assertEqual(cleared_assistant_status["status"], "")
        self.assertEqual(title["title"], "Deploy status")
        self.assertEqual(prompts["suggested_prompt_count"], 1)
        self.assertEqual(stream["stream_ts"], "1712161831.000500")
        self.assertEqual(appended["stream_ts"], "1712161831.000500")
        self.assertEqual(stopped["message"]["text"], "Done")

    def test_agent_route_selects_delivery_route_by_channel(self) -> None:
        provider_module.configure(
            "supportSlackbot",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "triage",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventType": "message",
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        payload = {
            "type": "event_callback",
            "event_id": "EvRoute",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C_SUPPORT",
                "channel_type": "channel",
                "text": "<@UBOT> please triage this",
                "ts": "1712161829.000300",
            },
        }
        request = gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123"))

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        workflow_request = workflow_client.deliver_event_requests[0]
        self.assertEqual(workflow_request.provider_name, "local")
        self.assertEqual(workflow_request.event.type, "slack.agent.event.received")
        self.assertEqual(workflow_request.event.subject, "route:triage")
        event_metadata = delivered_event_metadata(workflow_request)
        self.assertEqual(event_metadata["slack"]["agent_route_id"], "triage")
        self.assertEqual(event_metadata["slack"]["addressed_to_bot"], True)

    def test_agent_route_workflow_provider_handles_events_without_global_provider(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "agent": {
                    "routes": [
                        {
                            "id": "route-local",
                            "match": {"channel": "C_ROUTE"},
                            "workflow": {
                                "provider": "route-provider",
                                "workflowEventType": "slack.route.agent.received",
                                "subject": "route:route-local",
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        payload = {
            "type": "event_callback",
            "event_id": "EvRouteWorkflow",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C_ROUTE",
                "channel_type": "channel",
                "text": "<@UBOT> route this",
                "ts": "1712161829.000300",
            },
        }

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        workflow_request = workflow_client.deliver_event_requests[0]
        self.assertEqual(workflow_request.provider_name, "route-provider")
        self.assertEqual(workflow_request.event.type, "slack.route.agent.received")
        self.assertEqual(workflow_request.event.subject, "route:route-local")

    def test_agent_route_workflow_provider_handles_interactions_without_global_provider(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "agent": {
                    "routes": [
                        {
                            "id": "route-local",
                            "match": {"channel": "C_ROUTE"},
                            "workflow": {
                                "provider": "route-provider",
                                "workflowInteractionEventType": (
                                    "slack.route.interaction.received"
                                ),
                                "interactionSubject": "route:route-local",
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        interaction_payload = signed_route_block_action_payload("route-local")
        workflow_client = FakeWorkflowClient()

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_interactions_handle(
                {"payload": json.dumps(interaction_payload)},
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        workflow_request = workflow_client.deliver_event_requests[0]
        self.assertEqual(workflow_request.provider_name, "route-provider")
        self.assertEqual(
            workflow_request.event.type, "slack.route.interaction.received"
        )
        self.assertEqual(workflow_request.event.subject, "route:route-local")

    def test_user_scoped_interaction_rejects_different_slack_user(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [{"id": "route-local", "match": {"channel": "C_ROUTE"}}],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        interaction_payload = signed_route_block_action_payload(
            "route-local",
            subject_id="user:gestalt-123",
            event_user_id="U456",
            interaction_user_id="U_OTHER",
        )

        response = provider_module.slack_interactions_handle(
            {"payload": json.dumps(interaction_payload)},
            gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
        )

        self.assertIsInstance(response, gestalt.Response)
        forbidden = cast(gestalt.Response[dict[str, str]], response)
        self.assertEqual(forbidden.status, HTTPStatus.FORBIDDEN)
        self.assertEqual(
            forbidden.body,
            {"error": "interaction_ref user does not match Slack payload"},
        )

    def test_agent_route_run_as_subject_handles_interactions(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "alert-actions",
                            "runAs": {
                                "subject": {
                                    "id": "service_account:slack-bot",
                                    "kind": "service_account",
                                    "displayName": "Platform Slack Bot",
                                },
                            },
                            "match": {"channel": "C_ALERTS", "botIds": ["B123"]},
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        interaction_payload = signed_route_block_action_payload(
            "alert-actions",
            subject_id="service_account:slack-bot",
            event_user_id="U_BOT_USER",
            interaction_user_id="U_HUMAN",
            channel_id="C_ALERTS",
        )
        workflow_client = FakeWorkflowClient()

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_interactions_handle(
                {"payload": json.dumps(interaction_payload)},
                gestalt.Request(
                    subject=gestalt.Subject(id="service_account:slack-bot")
                ),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        event_data = delivered_event_data(workflow_client.deliver_event_requests[0])
        self.assertEqual(event_data["slack"]["user_id"], "U_HUMAN")

    def test_signed_interaction_rejects_route_id_that_no_longer_resolves(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "agent": {
                    "routes": [
                        {
                            "id": "stale-route",
                            "match": {"channel": "C_ROUTE"},
                            "workflow": {"provider": "route-provider"},
                        }
                    ],
                },
            },
        )
        interaction_payload = signed_route_block_action_payload("stale-route")
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [{"id": "new-route", "match": {"channel": "C_ROUTE"}}],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()

        with mock.patch.object(
            gestalt.Request,
            "workflows",
            return_value=workflow_client,
            create=True,
        ):
            result = provider_module.slack_interactions_handle(
                {"payload": json.dumps(interaction_payload)},
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertEqual(
            response.body,
            {"error": "Slack interaction route is no longer configured"},
        )
        self.assertEqual(workflow_client.deliver_event_requests, [])

    def test_configured_route_ignores_unaddressed_channel_message(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "agent": {
                    "routes": [
                        {
                            "id": "triage",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventType": "message",
                            },
                        }
                    ]
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        payload = {
            "type": "event_callback",
            "event_id": "EvRouteIgnored",
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

        response = provider_module.slack_events_handle(
            payload,
            gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
        )

        self.assertEqual(
            operation_body(response), {"ok": True, "ignored": "unsupported_event_type"}
        )

    def test_event_type_route_starts_plain_channel_message_agent(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "support-all-messages",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["message.channels"],
                                "subtypes": [],
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        payload = {
            "type": "event_callback",
            "event_id": "EvPlainChannel",
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

        response, workflow_client = self._handle_event_with_workflow(payload)

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        workflow_request = workflow_client.deliver_event_requests[0]
        event_data = delivered_event_data(workflow_request)
        self.assertEqual(
            event_data["workflowKey"],
            "slack:T123:C_SUPPORT:1712161829.000300",
        )
        self.assertEqual(event_data["slack"]["addressed_to_bot"], False)
        self.assertEqual(event_data["slack"]["subtype"], "")
        self.assertEqual(
            event_data["slack"]["reply_thread_ts"], "1712161829.000300"
        )
        self.assertIn(
            "reply_thread_ts: 1712161829.000300", event_data["user_prompt"]
        )
        verified_ref = provider_module._verify_reply_ref(
            event_data["reply_ref"], "user:gestalt-123"
        )
        self.assertEqual(verified_ref.reply_thread_ts, "1712161829.000300")
        event_metadata = event_data["metadata"]
        self.assertEqual(
            event_metadata["slack"]["agent_route_id"], "support-all-messages"
        )
        self.assertEqual(event_metadata["slack"]["addressed_to_bot"], False)

    def test_event_type_route_keeps_plain_thread_reply_in_thread(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "support-all-messages",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["message.channels"],
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        payload = {
            "type": "event_callback",
            "event_id": "EvPlainThread",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C_SUPPORT",
                "channel_type": "channel",
                "text": "adding thread context",
                "ts": "1712161835.000400",
                "thread_ts": "1712161829.000300",
            },
        }

        response, workflow_client = self._handle_event_with_workflow(payload)

        self.assertEqual(operation_body(response)["ok"], True)
        workflow_request = workflow_client.deliver_event_requests[0]
        event_data = delivered_event_data(workflow_request)
        self.assertEqual(
            event_data["workflowKey"],
            "slack:T123:C_SUPPORT:1712161829.000300",
        )
        self.assertEqual(
            workflow_request.event.id,
            "slack:event:T123:C_SUPPORT:1712161835.000400:U456",
        )
        self.assertEqual(
            event_data["slack"]["reply_thread_ts"], "1712161829.000300"
        )
        verified_ref = provider_module._verify_reply_ref(
            event_data["reply_ref"], "user:gestalt-123"
        )
        self.assertEqual(verified_ref.reply_thread_ts, "1712161829.000300")

    def test_event_type_route_thread_root_filters_channel_messages(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "channel-roots",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["message.channels"],
                                "thread": "root",
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})

        root_cases = [
            ("without_thread_ts", {}),
            ("with_parent_thread_ts", {"thread_ts": "1712161829.000300"}),
        ]
        for name, extra_fields in root_cases:
            with self.subTest(name=name):
                response, workflow_client = self._handle_event_with_workflow(
                    {
                        "type": "event_callback",
                        "event_id": f"EvRoot{name}",
                        "team_id": "T123",
                        "event": {
                            "type": "message",
                            "user": "U456",
                            "channel": "C_SUPPORT",
                            "channel_type": "channel",
                            "text": "please triage this",
                            "ts": "1712161829.000300",
                            **extra_fields,
                        },
                    }
                )

                self.assertEqual(operation_body(response)["ok"], True)
                self.assertEqual(len(workflow_client.deliver_event_requests), 1)
                event_metadata = delivered_event_metadata(
                    workflow_client.deliver_event_requests[0]
                )
                self.assertEqual(
                    event_metadata["slack"]["agent_route_id"], "channel-roots"
                )

        response, workflow_client = self._handle_event_with_workflow(
            {
                "type": "event_callback",
                "event_id": "EvRootIgnoresReply",
                "team_id": "T123",
                "event": {
                    "type": "message",
                    "user": "U456",
                    "channel": "C_SUPPORT",
                    "channel_type": "channel",
                    "text": "thread reply",
                    "ts": "1712161835.000400",
                    "thread_ts": "1712161829.000300",
                },
            }
        )

        self.assertEqual(
            operation_body(response), {"ok": True, "ignored": "no_matching_agent_route"}
        )
        self.assertEqual(workflow_client.deliver_event_requests, [])

    def test_event_type_route_thread_reply_filters_channel_messages(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "channel-replies",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["message.channels"],
                                "thread": "reply",
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})

        response, workflow_client = self._handle_event_with_workflow(
            {
                "type": "event_callback",
                "event_id": "EvReplyIgnoresRoot",
                "team_id": "T123",
                "event": {
                    "type": "message",
                    "user": "U456",
                    "channel": "C_SUPPORT",
                    "channel_type": "channel",
                    "text": "top-level message",
                    "ts": "1712161829.000300",
                },
            }
        )

        self.assertEqual(
            operation_body(response), {"ok": True, "ignored": "no_matching_agent_route"}
        )
        self.assertEqual(workflow_client.deliver_event_requests, [])

        response, workflow_client = self._handle_event_with_workflow(
            {
                "type": "event_callback",
                "event_id": "EvReplyMatches",
                "team_id": "T123",
                "event": {
                    "type": "message",
                    "user": "U456",
                    "channel": "C_SUPPORT",
                    "channel_type": "channel",
                    "text": "thread reply",
                    "ts": "1712161835.000400",
                    "thread_ts": "1712161829.000300",
                },
            }
        )

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        workflow_request = workflow_client.deliver_event_requests[0]
        event_data = delivered_event_data(workflow_request)
        self.assertEqual(
            event_data["workflowKey"],
            "slack:T123:C_SUPPORT:1712161829.000300",
        )
        event_metadata = event_data["metadata"]
        self.assertEqual(event_metadata["slack"]["agent_route_id"], "channel-replies")

    def test_app_mention_routes_respect_thread_filter(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "root-mentions",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["app_mention"],
                                "thread": "root",
                            },
                        },
                        {
                            "id": "any-mentions",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["app_mention"],
                                "thread": "any",
                            },
                        },
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})

        for route_id, event in (
            (
                "root-mentions",
                {
                    "type": "app_mention",
                    "user": "U456",
                    "channel": "C_SUPPORT",
                    "channel_type": "channel",
                    "text": "<@UBOT> top-level question",
                    "ts": "1712161829.000300",
                },
            ),
            (
                "any-mentions",
                {
                    "type": "app_mention",
                    "user": "U456",
                    "channel": "C_SUPPORT",
                    "channel_type": "channel",
                    "text": "<@UBOT> thread question",
                    "ts": "1712161835.000400",
                    "thread_ts": "1712161829.000300",
                },
            ),
        ):
            with self.subTest(route_id=route_id):
                response, workflow_client = self._handle_event_with_workflow(
                    {
                        "type": "event_callback",
                        "event_id": f"Ev{route_id}",
                        "team_id": "T123",
                        "event": event,
                    }
                )

                self.assertEqual(operation_body(response)["ok"], True)
                self.assertEqual(len(workflow_client.deliver_event_requests), 1)
                event_metadata = delivered_event_metadata(
                    workflow_client.deliver_event_requests[0]
                )
                self.assertEqual(event_metadata["slack"]["agent_route_id"], route_id)

    def test_event_type_route_ordering_skips_generic_message_route(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "generic-message-route",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["message"],
                            },
                        },
                        {
                            "id": "explicit-slack-route",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["message.channels"],
                            },
                        },
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        payload = {
            "type": "event_callback",
            "event_id": "EvRouteOrder",
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

        response, workflow_client = self._handle_event_with_workflow(payload)

        self.assertEqual(operation_body(response)["ok"], True)
        event_metadata = delivered_event_metadata(workflow_client.deliver_event_requests[0])
        self.assertEqual(
            event_metadata["slack"]["agent_route_id"], "explicit-slack-route"
        )

    def test_event_type_routes_match_supported_slack_event_literals(
        self,
    ) -> None:
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_cases = [
            (
                "app_mention",
                {
                    "type": "app_mention",
                    "user": "U456",
                    "channel": "C_SUPPORT",
                    "channel_type": "channel",
                    "text": "<@UBOT> please triage this",
                    "ts": "1712161829.000300",
                },
            ),
            (
                "message.channels",
                {
                    "type": "message",
                    "user": "U456",
                    "channel": "C_SUPPORT",
                    "channel_type": "channel",
                    "text": "public channel message",
                    "ts": "1712161829.000300",
                },
            ),
            (
                "message.groups",
                {
                    "type": "message",
                    "user": "U456",
                    "channel": "G_SUPPORT",
                    "channel_type": "group",
                    "text": "private channel message",
                    "ts": "1712161829.000300",
                },
            ),
            (
                "message.im",
                {
                    "type": "message",
                    "user": "U456",
                    "channel": "D_SUPPORT",
                    "channel_type": "im",
                    "text": "direct message",
                    "ts": "1712161829.000300",
                },
            ),
            (
                "message.mpim",
                {
                    "type": "message",
                    "user": "U456",
                    "channel": "GMPIM",
                    "channel_type": "mpim",
                    "text": "group direct message",
                    "ts": "1712161829.000300",
                },
            ),
            (
                "message.app_home",
                {
                    "type": "message",
                    "user": "U456",
                    "channel": "D_HOME",
                    "channel_type": "app_home",
                    "text": "app home message",
                    "ts": "1712161829.000300",
                },
            ),
        ]

        for event_type, event in workflow_cases:
            with self.subTest(event_type=event_type):
                provider_module.configure(
                    "slack",
                    {
                        "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                        "workflow": {"provider": "local"},
                        "agent": {
                            "routes": [
                                {
                                    "id": f"route-{event_type}",
                                    "match": {"eventTypes": [event_type]},
                                }
                            ],
                        },
                    },
                )
                payload = {
                    "type": "event_callback",
                    "event_id": f"Ev{event_type}",
                    "team_id": "T123",
                    "event": event,
                }

                response, workflow_client = self._handle_event_with_workflow(payload)

                self.assertEqual(operation_body(response)["ok"], True)
                self.assertEqual(len(workflow_client.deliver_event_requests), 1)
                event_metadata = delivered_event_metadata(
                    workflow_client.deliver_event_requests[0]
                )
                self.assertEqual(
                    event_metadata["slack"]["agent_route_id"],
                    f"route-{event_type}",
                )
                if event_type == "message.app_home":
                    workflow_request = workflow_client.deliver_event_requests[0]
                    event_data = delivered_event_data(workflow_request)
                    self.assertEqual(event_data["workflowKey"], "slack:T123:D_HOME")
                    self.assertEqual(event_data["slack"]["reply_thread_ts"], "")
                    self.assertEqual(event_data["slack"]["addressed_to_bot"], True)

        assistant_cases = [
            "assistant_thread_started",
            "assistant_thread_context_changed",
        ]
        for event_type in assistant_cases:
            with self.subTest(event_type=event_type):
                provider_module.configure(
                    "slack",
                    {
                        "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                        "workflow": {"provider": "local"},
                        "agent": {
                            "routes": [
                                {
                                    "id": f"route-{event_type}",
                                    "match": {"eventTypes": [event_type]},
                                }
                            ],
                        },
                    },
                )
                payload = {
                    "type": "event_callback",
                    "event_id": f"Ev{event_type}",
                    "team_id": "T123",
                    "event": {
                        "type": event_type,
                        "assistant_thread": {
                            "user_id": "U456",
                            "channel_id": "D_ASSISTANT",
                            "thread_ts": "1712161829.000300",
                            "context": {
                                "channel_id": "C_SUPPORT",
                                "team_id": "T123",
                            },
                        },
                    },
                }

                response, workflow_client = self._handle_event_with_workflow(payload)

                self.assertEqual(operation_body(response)["ok"], True)
                self.assertEqual(len(workflow_client.deliver_event_requests), 1)
                event_data = delivered_event_data(
                    workflow_client.deliver_event_requests[0]
                )
                self.assertEqual(event_data["slack"]["event_type"], event_type)
                self.assertEqual(
                    event_data["metadata"]["slack"]["agent_route_id"],
                    f"route-{event_type}",
                )

    def test_event_type_routes_filter_message_subtypes(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "plain-only",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["message.channels"],
                                "subtypes": [],
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        file_share_payload = {
            "type": "event_callback",
            "event_id": "EvFileShareIgnored",
            "team_id": "T123",
            "event": {
                "type": "message",
                "subtype": "file_share",
                "user": "U456",
                "channel": "C_SUPPORT",
                "channel_type": "channel",
                "text": "shared a file",
                "ts": "1712161829.000300",
                "files": [{"id": "F123", "name": "deploy.txt"}],
            },
        }

        response = provider_module.slack_events_handle(
            file_share_payload,
            gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
        )

        self.assertEqual(
            operation_body(response), {"ok": True, "ignored": "no_matching_agent_route"}
        )

        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "file-shares",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["message.channels"],
                                "subtypes": ["file_share"],
                            },
                        }
                    ],
                },
            },
        )

        response, workflow_client = self._handle_event_with_workflow(
            {
                **file_share_payload,
                "event_id": "EvFileShareMatched",
            }
        )

        self.assertEqual(operation_body(response)["ok"], True)
        event_metadata = delivered_event_metadata(workflow_client.deliver_event_requests[0])
        self.assertEqual(event_metadata["slack"]["agent_route_id"], "file-shares")
        self.assertEqual(event_metadata["slack"]["subtype"], "file_share")

    def test_event_type_route_keeps_ignored_message_events_ignored(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "all-channel-messages",
                            "match": {"eventTypes": ["message.channels"]},
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        ignored_events = [
            {"subtype": "message_changed"},
            {"subtype": "message_deleted"},
            {"subtype": "message_replied"},
        ]

        for index, event_fields in enumerate(ignored_events, start=1):
            with self.subTest(event_fields=event_fields):
                payload = {
                    "type": "event_callback",
                    "event_id": f"EvIgnored{index}",
                    "team_id": "T123",
                    "event": {
                        "type": "message",
                        "user": "U456",
                        "channel": "C_SUPPORT",
                        "channel_type": "channel",
                        "text": "ignore this",
                        "ts": "1712161829.000300",
                        **event_fields,
                    },
                }

                response, workflow_client = self._handle_event_with_workflow(payload)

                self.assertEqual(
                    operation_body(response), {"ok": True, "ignored": "ignored_event"}
                )
                self.assertEqual(workflow_client.deliver_event_requests, [])

    def test_event_type_route_ignores_bot_message_without_bot_match(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "all-channel-messages",
                            "match": {"eventTypes": ["message.channels"]},
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})

        response, workflow_client = self._handle_event_with_workflow(
            {
                "type": "event_callback",
                "event_id": "EvBotIgnored",
                "team_id": "T123",
                "event": {
                    "type": "message",
                    "subtype": "bot_message",
                    "bot_id": "B123",
                    "user": "U_BOT_USER",
                    "channel": "C_SUPPORT",
                    "channel_type": "channel",
                    "text": "bot alert",
                    "ts": "1712161829.000300",
                },
            }
        )

        self.assertEqual(
            operation_body(response), {"ok": True, "ignored": "no_matching_agent_route"}
        )
        self.assertEqual(workflow_client.deliver_event_requests, [])

    def test_event_type_route_can_match_configured_bot_message(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "alert-bot-messages",
                            "match": {
                                "channel": "C_ALERTS",
                                "eventTypes": ["message.channels"],
                                "botIds": ["B123"],
                                "thread": "root",
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})

        response, workflow_client = self._handle_event_with_workflow(
            {
                "type": "event_callback",
                "event_id": "EvBotMatched",
                "team_id": "T123",
                "event": {
                    "type": "message",
                    "subtype": "bot_message",
                    "bot_id": "B123",
                    "user": "U_BOT_USER",
                    "channel": "C_ALERTS",
                    "channel_type": "channel",
                    "text": "",
                    "attachments": [
                        {
                            "fallback": "Datadog alert: high error rate",
                            "fields": [
                                {"title": "Status", "value": "Triggered"},
                            ],
                        }
                    ],
                    "ts": "1712161829.000300",
                },
            }
        )

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        workflow_request = workflow_client.deliver_event_requests[0]
        event_data = delivered_event_data(workflow_request)
        event_payload = event_data
        event_metadata = event_data["metadata"]
        self.assertEqual(
            event_metadata["slack"]["agent_route_id"], "alert-bot-messages"
        )
        self.assertEqual(event_metadata["slack"]["bot_id"], "B123")
        self.assertEqual(event_metadata["slack"]["is_bot_event"], True)
        self.assertIn(
            "Datadog alert: high error rate",
            event_payload["agent_request"]["current_message"]["text"],
        )
        self.assertEqual(
            event_payload["agent_request"]["current_message"]["bot_id"], "B123"
        )
        self.assertEqual(
            event_payload["agent_request"]["current_message"]["is_bot_event"], True
        )

    def test_datadog_bot_agent_route_does_not_require_deliver_route_match(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "events": {
                    "deliver": {
                        "routes": [
                            {
                                "id": "brain-ingest",
                                "match": {"eventTypes": ["message"]},
                            }
                        ]
                    }
                },
                "agent": {
                    "routes": [
                        {
                            "id": "alert-bot-messages",
                            "match": {
                                "channel": "C_ALERTS",
                                "eventTypes": ["message.channels"],
                                "botIds": ["B123"],
                                "thread": "root",
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        payload = {
            "type": "event_callback",
            "event_id": "EvDatadogAlert",
            "team_id": "T123",
            "event": {
                "type": "message",
                "subtype": "bot_message",
                "bot_id": "B123",
                "user": "U_BOT_USER",
                "channel": "C_ALERTS",
                "channel_type": "channel",
                "text": "",
                "attachments": [{"fallback": "Datadog alert: high error rate"}],
                "ts": "1712161829.000300",
            },
        }

        with (
            mock.patch.object(
                provider_module._agent.gestalt,
                "WorkflowDeliverEvent",
                FakeWorkflowDeliverEvent,
            ),
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="service_account:slack-bot")
                ),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        event_metadata = delivered_event_metadata(workflow_client.deliver_event_requests[0])
        self.assertEqual(
            event_metadata["slack"]["agent_route_id"], "alert-bot-messages"
        )
        self.assertEqual(event_metadata["slack"]["bot_id"], "B123")

    def test_event_type_route_rejects_unmatched_bot_id(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "routes": [
                        {
                            "id": "alert-bot-messages",
                            "match": {
                                "channel": "C_ALERTS",
                                "eventTypes": ["message.channels"],
                                "botIds": ["B123"],
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})

        response, workflow_client = self._handle_event_with_workflow(
            {
                "type": "event_callback",
                "event_id": "EvBotRejected",
                "team_id": "T123",
                "event": {
                    "type": "message",
                    "subtype": "bot_message",
                    "bot_id": "B999",
                    "user": "U_BOT_USER",
                    "channel": "C_ALERTS",
                    "channel_type": "channel",
                    "text": "wrong bot",
                    "ts": "1712161829.000300",
                },
            }
        )

        self.assertEqual(
            operation_body(response), {"ok": True, "ignored": "no_matching_agent_route"}
        )
        self.assertEqual(workflow_client.deliver_event_requests, [])

    def test_repeated_slack_events_reuse_session_key_but_keep_event_metadata_on_turns(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        request = gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123"))
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
        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            provider_module.slack_events_handle(first, request)
            provider_module.slack_events_handle(second, request)

        self.assertEqual(len(workflow_client.deliver_event_requests), 2)
        requests = workflow_client.deliver_event_requests
        first_data = delivered_event_data(requests[0])
        second_data = delivered_event_data(requests[1])
        self.assertEqual(
            first_data["workflowKey"],
            second_data["workflowKey"],
        )
        self.assertEqual(
            first_data["workflowKey"], "slack:T123:C789:1712161829.000300"
        )

        self.assertEqual(
            requests[0].event.id,
            "slack:event:T123:C789:1712161829.000300:U456",
        )
        self.assertEqual(
            requests[1].event.id,
            "slack:event:T123:C789:1712161835.000400:U999",
        )
        first_metadata = first_data["metadata"]
        second_metadata = second_data["metadata"]
        self.assertEqual(first_metadata["slack"]["user_id"], "U456")
        self.assertEqual(second_metadata["slack"]["user_id"], "U999")
        self.assertEqual(second_metadata["slack"]["message_ts"], "1712161835.000400")

    def test_configured_routes_ignore_non_matching_channels(self) -> None:
        provider_module.configure(
            "slack",
            {
                "agent": {
                    "routes": [
                        {
                            "id": "triage",
                            "match": {"channels": ["C_SUPPORT"]},
                        }
                    ]
                }
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
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

        with self.assertLogs(provider_module._agent.logger, level="INFO") as logs:
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertEqual(
            operation_body(response), {"ok": True, "ignored": "no_matching_agent_route"}
        )
        log_text = "\n".join(logs.output)
        self.assertIn("ignored Slack event", log_text)
        self.assertIn("ignored_reason=no_matching_agent_route", log_text)
        self.assertIn("slack_channel_id=C_OTHER", log_text)
        self.assertIn("subject_id=user:gestalt-123", log_text)
        self.assertNotIn("hello", log_text)

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

    def test_url_verification_returns_challenge_without_workflow_event(self) -> None:
        payload = {"type": "url_verification", "challenge": "challenge-token"}

        response = provider_module.slack_events_handle(payload, gestalt.Request())

        self.assertEqual(operation_body(response), {"challenge": "challenge-token"})

    def test_deliver_route_delivers_exact_workflow_event(self) -> None:
        provider_module.configure(
            "slack",
            {
                "events": {
                    "deliver": {
                        "routes": [
                            {
                                "id": "deployments",
                                "workflow": {"provider": "local"},
                                "workflowEventType": "deployment.slack_event",
                                "subject": "deployments",
                                "match": {
                                    "eventTypes": ["message"],
                                    "subtypes": [],
                                    "teamIds": ["T123"],
                                    "channelIds": ["C_DEPLOY"],
                                    "channelTypes": ["channel"],
                                    "userIds": ["U456"],
                                },
                            }
                        ]
                    }
                }
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        payload = {
            "type": "event_callback",
            "event_id": "EvDelivery",
            "team_id": "T123",
            "enterprise_id": "E123",
            "api_app_id": "A123",
            "event_context": "EC123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C_DEPLOY",
                "channel_type": "channel",
                "text": "Deploy finished",
                "ts": "1712161829.000300",
                "event_ts": "1712161829.000400",
                "files": [{"id": "F123", "name": "deploy.txt"}],
            },
        }
        with (
            mock.patch.object(
                provider_module._agent.gestalt,
                "WorkflowDeliverEvent",
                FakeWorkflowDeliverEvent,
            ),
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, gestalt.Request())

        self.assertEqual(
            operation_body(response),
            {
                "ok": True,
                "delivered": True,
                "delivered_event_count": 1,
                "workflow_event_ids": ["slack:EvDelivery:route:deployments"],
                "route_ids": ["deployments"],
            },
        )
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        request = workflow_client.deliver_event_requests[0]
        self.assertEqual(request.provider_name, "local")
        event = request.event
        self.assertEqual(event.id, "slack:EvDelivery:route:deployments")
        self.assertEqual(event.type, "deployment.slack_event")
        self.assertEqual(event.source, "slack")
        self.assertEqual(event.subject, "deployments")
        self.assertEqual(event.spec_version, "1.0")
        self.assertEqual(event.datacontenttype, "application/json")
        self.assertEqual(
            sdk_value_to_dict(event.data),
            {
                "routeId": "deployments",
                "slack": {
                    "callback_type": "event_callback",
                    "event_type": "message",
                    "event_id": "EvDelivery",
                    "team_id": "T123",
                    "enterprise_id": "E123",
                    "api_app_id": "A123",
                    "event_context": "EC123",
                    "user_id": "U456",
                    "bot_id": "",
                    "channel_id": "C_DEPLOY",
                    "channel_type": "channel",
                    "subtype": "",
                    "text": "Deploy finished",
                    "message_ts": "1712161829.000300",
                    "event_ts": "1712161829.000400",
                    "thread_ts": "",
                    "is_bot_event": False,
                    "file_ids": ["F123"],
                    "files": [{"id": "F123", "name": "deploy.txt"}],
                },
                "raw": payload,
            },
        )

    def test_deliver_routes_use_route_scoped_event_ids(self) -> None:
        provider_module.configure(
            "slack",
            {
                "events": {
                    "deliver": {
                        "routes": [
                            {
                                "id": "brain-ingest",
                                "workflow": {"provider": "local"},
                                "workflowEventType": "slack.brain",
                                "subject": "brain",
                                "match": {"eventTypes": ["message"]},
                            },
                            {
                                "id": "audit-ingest",
                                "workflow": {"provider": "audit"},
                                "workflowEventType": "slack.audit",
                                "subject": "audit",
                                "match": {"eventTypes": ["message"]},
                            },
                        ]
                    }
                }
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        payload = {
            "type": "event_callback",
            "event_id": "EvDelivery",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C_DEPLOY",
                "channel_type": "channel",
                "text": "Deploy finished",
                "ts": "1712161829.000300",
            },
        }

        with mock.patch.object(
            gestalt.Request,
            "workflows",
            return_value=workflow_client,
            create=True,
        ):
            response = provider_module.slack_events_handle(payload, gestalt.Request())

        self.assertEqual(
            operation_body(response)["workflow_event_ids"],
            [
                "slack:EvDelivery:route:brain-ingest",
                "slack:EvDelivery:route:audit-ingest",
            ],
        )
        self.assertEqual(
            [request.provider_name for request in workflow_client.deliver_event_requests],
            ["local", "audit"],
        )
        self.assertEqual(
            [request.event.subject for request in workflow_client.deliver_event_requests],
            ["brain", "audit"],
        )

    def test_agent_delivery_failure_returns_non_2xx_even_with_deliver_route(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "events": {
                    "deliver": {
                        "routes": [
                            {
                                "id": "brain-ingest",
                                "workflow": {"provider": "local"},
                                "workflowEventType": "slack.event.received",
                                "subject": "route:brain-ingest",
                                "match": {"eventTypes": ["message"]},
                            }
                        ]
                    }
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        workflow_client.deliver_event_error = RuntimeError("deliver failed")
        payload = {
            "type": "event_callback",
            "event_id": "EvDeliveryAndPublish",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C789",
                "channel_type": "im",
                "text": "deliver and publish",
                "ts": "1712161829.000300",
            },
        }
        request = gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123"))

        with (
            mock.patch.object(
                provider_module._agent.gestalt,
                "WorkflowDeliverEvent",
                FakeWorkflowDeliverEvent,
            ),
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertIsInstance(response, gestalt.Response)
        result = cast(gestalt.Response[dict[str, str]], response)
        self.assertEqual(result.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(
            result.body, {"error": "failed to deliver workflow event: deliver failed"}
        )
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)

    def test_deliver_route_ack_uses_request_id_after_deliver_succeeds(self) -> None:
        provider_module.configure(
            "slack",
            {
                "events": {
                    "deliver": {
                        "routes": [
                            {
                                "id": "brain-ingest",
                                "workflow": {"provider": "local"},
                                "workflowEventType": "slack.event.received",
                                "subject": "route:brain-ingest",
                                "match": {"eventTypes": ["message"]},
                            }
                        ]
                    }
                }
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = ExplodingDeliveryResponseWorkflowClient()
        payload = {
            "type": "event_callback",
            "event_id": "EvDeliveryBadResponse",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C789",
                "channel_type": "im",
                "text": "deliver response should not shape ack",
                "ts": "1712161829.000300",
            },
        }

        with (
            mock.patch.object(
                provider_module._agent.gestalt,
                "WorkflowDeliverEvent",
                FakeWorkflowDeliverEvent,
            ),
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, gestalt.Request())

        self.assertEqual(
            operation_body(response),
            {
                "ok": True,
                "delivered": True,
                "delivered_event_count": 1,
                "workflow_event_ids": [
                    "slack:EvDeliveryBadResponse:route:brain-ingest"
                ],
                "route_ids": ["brain-ingest"],
            },
        )
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)

    def test_agent_and_matching_deliver_route_ack_lists_both_events(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "events": {
                    "deliver": {
                        "routes": [
                            {
                                "id": "brain-ingest",
                                "workflow": {"provider": "local"},
                                "workflowEventType": "slack.event.received",
                                "subject": "route:brain-ingest",
                                "match": {"eventTypes": ["message"]},
                            }
                        ]
                    }
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        payload = {
            "type": "event_callback",
            "event_id": "EvBadAck",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C789",
                "channel_type": "im",
                "text": "deliver then bad ack",
                "ts": "1712161829.000300",
            },
        }
        request = gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123"))

        with mock.patch.object(
            gestalt.Request,
            "workflows",
            return_value=workflow_client,
            create=True,
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(len(workflow_client.deliver_event_requests), 2)
        self.assertEqual(
            operation_body(response),
            {
                "ok": True,
                "delivered": True,
                "delivered_event_count": 2,
                "workflow_event_ids": [
                    workflow_client.deliver_event_requests[0].event.id,
                    "slack:EvBadAck:route:brain-ingest",
                ],
                "route_ids": ["default", "brain-ingest"],
            },
        )

    def test_deliver_route_failure_with_agent_dispatch_returns_server_error(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "events": {
                    "deliver": {
                        "routes": [
                            {
                                "id": "brain-ingest",
                                "workflow": {"provider": "local"},
                                "workflowEventType": "slack.event.received",
                                "subject": "route:brain-ingest",
                                "match": {"eventTypes": ["message"]},
                            }
                        ]
                    }
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FailingSecondDeliveryWorkflowClient()
        payload = {
            "type": "event_callback",
            "event_id": "EvDeliveryFailsAfterPublish",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C789",
                "channel_type": "im",
                "text": "deliver fails after publish",
                "ts": "1712161829.000300",
            },
        }

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertIsInstance(response, gestalt.Response)
        result = cast(gestalt.Response[dict[str, str]], response)
        self.assertEqual(result.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(
            result.body, {"error": "failed to deliver workflow event: boom"}
        )
        self.assertEqual(len(workflow_client.deliver_event_requests), 2)

    def test_deliver_only_callback_without_linked_subject_passes_resolution(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "events": {
                    "deliver": {
                        "routes": [
                            {
                                "id": "mentions",
                                "match": {
                                    "eventTypes": ["app_mention"],
                                    "teamIds": ["T123"],
                                },
                            }
                        ]
                    }
                }
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        authorization = FakeAuthorization([])
        payload = {
            "type": "event_callback",
            "event_id": "EvNoLinkedSubject",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> deliver only",
                "ts": "1712161829.000300",
            },
        }

        with mock.patch.object(
            gestalt.Request, "authorization", return_value=authorization, create=True
        ):
            resolved = provider_module.resolve_http_subject(
                gestalt.HTTPSubjectRequest(params=payload),
                gestalt.Request(),
            )

        self.assertIsNone(resolved)
        self.assertEqual(authorization.requests, [])

    def test_deliver_routes_match_bot_include_and_subtype_filters(self) -> None:
        provider_module.configure(
            "slack",
            {
                "events": {
                    "deliver": {
                        "routes": [
                            {
                                "id": "human-channel",
                                "match": {
                                    "eventTypes": ["message"],
                                    "channelIds": ["C_BOT"],
                                },
                            },
                            {
                                "id": "bot-messages",
                                "match": {
                                    "eventTypes": ["message"],
                                    "subtypes": ["bot_message"],
                                    "botIds": ["B123"],
                                    "includeBotEvents": True,
                                },
                            },
                            {
                                "id": "no-subtype",
                                "match": {
                                    "eventTypes": ["message"],
                                    "subtypes": [],
                                    "channelIds": ["C_HUMAN"],
                                },
                            },
                        ]
                    }
                }
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        bot_payload = {
            "type": "event_callback",
            "team_id": "T123",
            "event": {
                "type": "message",
                "subtype": "bot_message",
                "bot_id": "B123",
                "channel": "C_BOT",
                "channel_type": "channel",
                "ts": "1712161829.000300",
            },
        }
        human_payload = {
            "type": "event_callback",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C_HUMAN",
                "channel_type": "channel",
                "text": "human update",
                "ts": "1712161830.000400",
            },
        }
        changed_payload = {
            "type": "event_callback",
            "team_id": "T123",
            "event": {
                "type": "message",
                "subtype": "message_changed",
                "user": "U456",
                "channel": "C_HUMAN",
                "channel_type": "channel",
                "message": {"text": "edited", "ts": "1712161831.000500"},
            },
        }

        with (
            mock.patch.object(
                provider_module._agent.gestalt,
                "WorkflowDeliverEvent",
                FakeWorkflowDeliverEvent,
            ),
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            bot_response = provider_module.slack_events_handle(
                bot_payload, gestalt.Request()
            )
            human_response = provider_module.slack_events_handle(
                human_payload, gestalt.Request()
            )
            changed_response = provider_module.slack_events_handle(
                changed_payload, gestalt.Request()
            )

        self.assertEqual(bot_response["route_ids"], ["bot-messages"])
        self.assertEqual(human_response["route_ids"], ["no-subtype"])
        self.assertEqual(changed_response, {"ok": True, "ignored": "ignored_event"})
        self.assertEqual(len(workflow_client.deliver_event_requests), 2)
        bot_event = workflow_client.deliver_event_requests[0].event
        self.assertEqual(
            bot_event.id,
            "slack:route:bot-messages:team:T123:event:message:subtype:"
            "bot_message:channel:C_BOT:ts:1712161829.000300:thread:-:actor:B123",
        )
        self.assertEqual(bot_event.type, "slack.event.received")
        self.assertEqual(bot_event.source, "slack")
        self.assertEqual(bot_event.subject, "route:bot-messages")
        bot_data = sdk_value_to_dict(bot_event.data)
        self.assertEqual(bot_data["routeId"], "bot-messages")
        self.assertEqual(bot_data["slack"]["bot_id"], "B123")
        self.assertEqual(bot_data["slack"]["subtype"], "bot_message")
        self.assertEqual(bot_data["slack"]["text"], "")
        self.assertTrue(bot_data["slack"]["is_bot_event"])

    def test_deliver_failure_returns_non_2xx(self) -> None:
        provider_module.configure(
            "slack",
            {
                "events": {
                    "deliver": {
                        "routes": [
                            {
                                "id": "all-messages",
                                "match": {"eventTypes": ["message"]},
                            }
                        ]
                    }
                }
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        workflow_client.deliver_event_error = RuntimeError("boom")
        payload = {
            "type": "event_callback",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C789",
                "text": "deliver me",
                "ts": "1712161829.000300",
            },
        }
        with (
            mock.patch.object(
                provider_module._agent.gestalt,
                "WorkflowDeliverEvent",
                FakeWorkflowDeliverEvent,
            ),
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
        ):
            result = provider_module.slack_events_handle(payload, gestalt.Request())

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(
            response.body, {"error": "failed to deliver workflow event: boom"}
        )

    def test_deliver_workflow_client_failure_returns_non_2xx(self) -> None:
        provider_module.configure(
            "slack",
            {
                "events": {
                    "deliver": {
                        "routes": [
                            {
                                "id": "all-messages",
                                "match": {"eventTypes": ["message"]},
                            }
                        ]
                    }
                }
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        payload = {
            "type": "event_callback",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C789",
                "text": "deliver me",
                "ts": "1712161829.000300",
            },
        }
        with (
            mock.patch.object(
                provider_module._agent.gestalt,
                "WorkflowDeliverEvent",
                FakeWorkflowDeliverEvent,
            ),
            mock.patch.object(
                gestalt.Request,
                "workflows",
                side_effect=RuntimeError("workflow client unavailable"),
                create=True,
            ),
        ):
            result = provider_module.slack_events_handle(payload, gestalt.Request())

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(
            response.body,
            {"error": "failed to deliver workflow event: workflow client unavailable"},
        )

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
                    url="https://example.slack.com/archives/C123ABC456/p1712161829000300"
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

    def test_get_thread_context_returns_messages_participants_and_files_contract(
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
                self.assertEqual(parsed.path, "/api/conversations.replies")
                self.assertEqual(
                    query,
                    {
                        "channel": ["C123"],
                        "ts": ["1.0"],
                        "limit": ["25"],
                        "cursor": ["cursor-in"],
                    },
                )
                return FakeHTTPResponse(
                    """
                    {
                      "ok": true,
                      "messages": [
                        {
                          "ts": "1.0",
                          "user": "U1",
                          "text": "parent <@U2>",
                          "files": [
                            {
                              "id": "F1",
                              "name": "notes.txt",
                              "mimetype": "text/plain",
                              "size": 12,
                              "url_private": "https://files.slack.com/files-pri/T-F1/notes.txt"
                            }
                          ]
                        },
                        {"ts": "2.0", "user": "U2", "text": "reply"},
                        {"ts": "3.0", "user": "U3", "bot_id": "B3", "text": "bot"}
                      ],
                      "response_metadata": {"next_cursor": "cursor-1"}
                    }
                    """
                )

            if call_count == 2:
                self.assertEqual(parsed.netloc, "files.slack.com")
                self.assertEqual(parsed.path, "/files-pri/T-F1/notes.txt")
                return FakeHTTPResponse("hello thread")

            if call_count == 3:
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

            if call_count == 4:
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
                    cursor="cursor-in",
                    limit=25,
                    include_user_info=True,
                    include_bots=False,
                    include_files=True,
                    include_file_content=True,
                    max_file_bytes=64,
                ),
                gestalt.Request(token="test-token"),
            )

        data = result["data"]
        self.assertEqual(call_count, 4)
        self.assertEqual(data["messages_returned"], 2)
        self.assertEqual(data["participant_count"], 2)
        self.assertEqual(data["file_count"], 1)
        self.assertEqual(data["next_cursor"], "cursor-1")
        self.assertEqual(data["root_message"]["mentions"], ["U2"])
        self.assertEqual(data["participants"][1]["display_name"], "bob")
        self.assertEqual(data["files"][0]["id"], "F1")
        self.assertEqual(data["files"][0]["content"]["encoding"], "utf-8")
        self.assertEqual(data["files"][0]["content"]["text"], "hello thread")

    def test_get_thread_context_accepts_slack_message_url_contract(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(authorization_header(request), "Bearer test-token")

            parsed = urllib.parse.urlsplit(request.full_url)
            self.assertEqual(parsed.path, "/api/conversations.replies")
            self.assertEqual(
                urllib.parse.parse_qs(parsed.query),
                {
                    "channel": ["C123ABC456"],
                    "ts": ["1712161829.000300"],
                    "limit": ["15"],
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
            result = provider_module.conversations_get_thread_context(
                provider_module.GetThreadContextInput(
                    url="https://example.slack.com/archives/C123ABC456/p1712161829000300"
                ),
                gestalt.Request(token="test-token"),
            )

        data = result["data"]
        self.assertEqual(data["channel"], "C123ABC456")
        self.assertEqual(data["thread_ts"], "1712161829.000300")
        self.assertEqual(data["root_message"]["text"], "hello")

    def test_files_upload_uses_external_upload_contract(self) -> None:
        api_calls: list[str] = []
        upload_calls = 0

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            api_calls.append(request.full_url)
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(authorization_header(request), "Bearer test-token")
            self.assertIn(
                "application/x-www-form-urlencoded",
                request_header(request, "Content-Type"),
            )
            parsed = urllib.parse.urlsplit(request.full_url)

            if parsed.path == "/api/files.getUploadURLExternal":
                self.assertEqual(
                    request_form(request),
                    {
                        "filename": ["report.txt"],
                        "length": [str(len("hello thread".encode("utf-8")))],
                        "alt_txt": ["Quarterly report"],
                        "snippet_type": ["text"],
                    },
                )
                return FakeHTTPResponse(
                    """
                    {
                      "ok": true,
                      "upload_url": "https://files.slack.com/upload/v1/ABC123",
                      "file_id": "F123"
                    }
                    """
                )

            if parsed.path == "/api/files.completeUploadExternal":
                form = request_form(request)
                self.assertEqual(
                    json.loads(form["files"][0]),
                    [{"id": "F123", "title": "Report"}],
                )
                self.assertEqual(form["channel_id"], ["C123"])
                self.assertEqual(form["thread_ts"], ["1.0"])
                self.assertNotIn("initial_comment", form)
                blocks = json.loads(form["blocks"][0])
                self.assertEqual(
                    blocks[0],
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": "Attached"},
                    },
                )
                self.assertEqual(
                    blocks[1],
                    {"type": "section", "text": {"type": "mrkdwn", "text": "Hi"}},
                )
                self.assertEqual(blocks[-1]["type"], "context")
                self.assertEqual(blocks[-1]["elements"][0]["text"], "Sent with Gestalt")
                return FakeHTTPResponse(
                    '{"ok": true, "files": [{"id": "F123", "title": "Report"}]}'
                )

            raise AssertionError(f"unexpected request {request.full_url}")

        def fake_upload_open(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            nonlocal upload_calls
            upload_calls += 1
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(
                request.full_url, "https://files.slack.com/upload/v1/ABC123"
            )
            self.assertIsNone(authorization_header(request))
            self.assertEqual(request_header(request, "Content-Type"), "text/plain")
            self.assertEqual(
                request_header(request, "Content-Length"),
                str(len(b"hello thread")),
            )
            self.assertEqual(request.data, b"hello thread")
            return FakeHTTPResponse("OK - 12")

        with (
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch(
                "internals.client.urllib.request.build_opener",
                return_value=FakeOpener(fake_upload_open),
            ),
        ):
            result = provider_module.files_upload(
                provider_module.UploadFileInput(
                    channel="C123",
                    filename="report.txt",
                    content="hello thread",
                    thread_ts="1.0",
                    title="Report",
                    initial_comment="Attached",
                    content_type="text/plain",
                    alt_txt="Quarterly report",
                    snippet_type="text",
                    blocks=[
                        {
                            "type": "section",
                            "text": {"type": "mrkdwn", "text": "Hi"},
                        }
                    ],
                ),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(upload_calls, 1)
        self.assertEqual(
            [urllib.parse.urlsplit(url).path for url in api_calls],
            [
                "/api/files.getUploadURLExternal",
                "/api/files.completeUploadExternal",
            ],
        )
        self.assertEqual(operation_body(result)["ok"], True)
        self.assertEqual(operation_body(result)["file_id"], "F123")
        self.assertEqual(operation_body(result)["channel"], "C123")
        self.assertEqual(operation_body(result)["thread_ts"], "1.0")

    def test_files_upload_accepts_base64_pdf_contract(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            parsed = urllib.parse.urlsplit(request.full_url)
            if parsed.path == "/api/files.getUploadURLExternal":
                self.assertEqual(
                    request_form(request),
                    {"filename": ["report.pdf"], "length": ["8"]},
                )
                return FakeHTTPResponse(
                    '{"ok": true, "upload_url": "https://files.slack.com/upload/v1/PDF", "file_id": "FPDF"}'
                )
            if parsed.path == "/api/files.completeUploadExternal":
                form = request_form(request)
                blocks = json.loads(form["blocks"][0])
                self.assertNotIn("initial_comment", form)
                self.assertEqual(
                    blocks,
                    [
                        {
                            "type": "context",
                            "elements": [
                                {"type": "mrkdwn", "text": "Sent with Gestalt"}
                            ],
                        }
                    ],
                )
                return FakeHTTPResponse('{"ok": true, "files": [{"id": "FPDF"}]}')
            raise AssertionError(f"unexpected request {request.full_url}")

        def fake_upload_open(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request_header(request, "Content-Type"), "application/pdf")
            self.assertEqual(request.data, b"%PDF-1.4")
            return FakeHTTPResponse("OK - 8")

        with (
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch(
                "internals.client.urllib.request.build_opener",
                return_value=FakeOpener(fake_upload_open),
            ),
        ):
            result = provider_module.files_upload(
                provider_module.UploadFileInput(
                    channel="C123",
                    filename="report.pdf",
                    content_base64="JVBERi0xLjQ=",
                    content_type="application/pdf",
                ),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(operation_body(result)["file_id"], "FPDF")

    def test_files_upload_uses_utf8_byte_length_contract(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            parsed = urllib.parse.urlsplit(request.full_url)
            if parsed.path == "/api/files.getUploadURLExternal":
                self.assertEqual(
                    request_form(request),
                    {"filename": ["unicode.txt"], "length": ["2"]},
                )
                return FakeHTTPResponse(
                    '{"ok": true, "upload_url": "https://files.slack.com/upload/v1/TXT", "file_id": "FTXT"}'
                )
            if parsed.path == "/api/files.completeUploadExternal":
                return FakeHTTPResponse('{"ok": true, "files": [{"id": "FTXT"}]}')
            raise AssertionError(f"unexpected request {request.full_url}")

        def fake_upload_open(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.data, "é".encode("utf-8"))
            return FakeHTTPResponse("OK - 2")

        with (
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch(
                "internals.client.urllib.request.build_opener",
                return_value=FakeOpener(fake_upload_open),
            ),
        ):
            result = provider_module.files_upload(
                provider_module.UploadFileInput(
                    channel="C123", filename="unicode.txt", content="é"
                ),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(operation_body(result)["ok"], True)

    def test_files_upload_validates_request_contract(self) -> None:
        cases = [
            (
                provider_module.UploadFileInput(filename="a.txt", content="hi"),
                "channel is required",
            ),
            (
                provider_module.UploadFileInput(channel="C123", content="hi"),
                "filename is required",
            ),
            (
                provider_module.UploadFileInput(
                    channel="C123",
                    filename="a.txt",
                    content="hi",
                    content_base64="aGk=",
                ),
                "content and content_base64 are mutually exclusive",
            ),
            (
                provider_module.UploadFileInput(channel="C123", filename="a.txt"),
                "content or content_base64 is required",
            ),
            (
                provider_module.UploadFileInput(
                    channel="C123", filename="a.txt", content_base64="not-base64!"
                ),
                "content_base64 must be valid base64",
            ),
            (
                provider_module.UploadFileInput(
                    channel="C123",
                    filename="a.txt",
                    content="hi",
                    blocks=cast(Any, ["bad"]),
                ),
                "blocks must be an array of Slack block objects",
            ),
            (
                provider_module.UploadFileInput(
                    channel="C123",
                    filename="a.txt",
                    content="hi",
                    blocks=[{"type": "divider"} for _ in range(50)],
                ),
                "initial_comment and blocks must leave room for the Gestalt footer",
            ),
        ]

        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            for input, error in cases:
                with self.subTest(error=error):
                    result = provider_module.files_upload(
                        input, gestalt.Request(token="test-token")
                    )
                    self.assertIsInstance(result, gestalt.Response)
                    response = cast(gestalt.Response[dict[str, str]], result)
                    self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
                    self.assertEqual(response.body, {"error": error})

        urlopen.assert_not_called()

    def test_files_upload_rejects_oversize_payload_contract(self) -> None:
        cases = [
            provider_module.UploadFileInput(
                channel="C123", filename="a.txt", content="hello"
            ),
            provider_module.UploadFileInput(
                channel="C123", filename="a.txt", content_base64="aGVsbG8="
            ),
        ]

        with (
            mock.patch("internals.operations.MAX_UPLOAD_BYTES", 4),
            mock.patch("internals.client.urllib.request.urlopen") as urlopen,
        ):
            for input in cases:
                with self.subTest(input=input):
                    result = provider_module.files_upload(
                        input, gestalt.Request(token="test-token")
                    )
                    self.assertIsInstance(result, gestalt.Response)
                    response = cast(gestalt.Response[dict[str, str]], result)
                    self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
                    self.assertEqual(
                        response.body, {"error": "file content exceeds 4 bytes"}
                    )

        urlopen.assert_not_called()

    def test_files_upload_rejects_malformed_init_response_contract(self) -> None:
        responses = [
            (
                '{"ok": true, "file_id": "F123"}',
                "Slack upload URL response missing upload_url",
            ),
            (
                '{"ok": true, "upload_url": "https://files.slack.com/upload/v1/ABC"}',
                "Slack upload URL response missing file_id",
            ),
        ]

        for body, error in responses:
            with self.subTest(error=error):

                def fake_urlopen(
                    request: urllib.request.Request, timeout: float = 30
                ) -> FakeHTTPResponse:
                    self.assertEqual(timeout, 30)
                    self.assertEqual(
                        urllib.parse.urlsplit(request.full_url).path,
                        "/api/files.getUploadURLExternal",
                    )
                    return FakeHTTPResponse(body)

                with (
                    mock.patch(
                        "internals.client.urllib.request.urlopen",
                        side_effect=fake_urlopen,
                    ),
                    mock.patch(
                        "internals.client.urllib.request.build_opener"
                    ) as opener,
                ):
                    result = provider_module.files_upload(
                        provider_module.UploadFileInput(
                            channel="C123", filename="a.txt", content="hi"
                        ),
                        gestalt.Request(token="test-token"),
                    )

                self.assertIsInstance(result, gestalt.Response)
                response = cast(gestalt.Response[dict[str, str]], result)
                self.assertEqual(response.status, HTTPStatus.BAD_GATEWAY)
                self.assertEqual(response.body, {"error": error})
                opener.assert_not_called()

    def test_files_upload_rejects_non_slack_upload_url_contract(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(
                urllib.parse.urlsplit(request.full_url).path,
                "/api/files.getUploadURLExternal",
            )
            return FakeHTTPResponse(
                '{"ok": true, "upload_url": "https://example.com/upload/v1/ABC", "file_id": "F123"}'
            )

        with (
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch("internals.client.urllib.request.build_opener") as opener,
        ):
            result = provider_module.files_upload(
                provider_module.UploadFileInput(
                    channel="C123", filename="a.txt", content="hi"
                ),
                gestalt.Request(token="test-token"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(
            response.body,
            {"error": "slack file upload URL must be a Slack HTTPS upload URL"},
        )
        opener.assert_not_called()

    def test_files_upload_redirect_handler_rejects_redirects_contract(self) -> None:
        handler = client_module._SlackFileUploadRedirectHandler()
        request = urllib.request.Request(
            "https://files.slack.com/upload/v1/ABC", data=b"hi", method="POST"
        )
        cases = [
            (
                "https://example.com/upload/v1/ABC",
                "slack file upload redirected to a non-Slack URL",
            ),
            (
                "https://files.slack.com/upload/v1/DEF",
                "slack file upload redirects are not supported",
            ),
        ]

        for newurl, error in cases:
            with self.subTest(newurl=newurl):
                with self.assertRaises(client_module.SlackClientError) as raised:
                    handler.redirect_request(
                        request, io.BytesIO(), 302, "Found", HTTPMessage(), newurl
                    )
                self.assertEqual(str(raised.exception), error)

    def test_files_upload_propagates_raw_upload_error_contract(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(
                urllib.parse.urlsplit(request.full_url).path,
                "/api/files.getUploadURLExternal",
            )
            return FakeHTTPResponse(
                '{"ok": true, "upload_url": "https://files.slack.com/upload/v1/ABC", "file_id": "F123"}'
            )

        def fake_upload_open(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            raise make_http_error(request.full_url, 403, '{"error": "access_denied"}')

        with (
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch(
                "internals.client.urllib.request.build_opener",
                return_value=FakeOpener(fake_upload_open),
            ),
        ):
            result = provider_module.files_upload(
                provider_module.UploadFileInput(
                    channel="C123", filename="a.txt", content="hi"
                ),
                gestalt.Request(token="test-token"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertEqual(response.body, {"error": "access_denied"})

    def test_events_upload_file_uses_reply_ref_thread_and_bot_token_contract(
        self,
    ) -> None:
        provider_module.configure("slack", {"bot": {"token": "xoxb-bot"}})
        self.addCleanup(provider_module.configure, "slack", {})
        reply_ref = self._signed_reply_ref()

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(authorization_header(request), "Bearer xoxb-bot")
            parsed = urllib.parse.urlsplit(request.full_url)
            if parsed.path == "/api/files.getUploadURLExternal":
                self.assertEqual(
                    request_form(request),
                    {"filename": ["event.pdf"], "length": ["8"]},
                )
                return FakeHTTPResponse(
                    '{"ok": true, "upload_url": "https://files.slack.com/upload/v1/EVENT", "file_id": "FEVENT"}'
                )
            if parsed.path == "/api/files.completeUploadExternal":
                form = request_form(request)
                self.assertEqual(form["channel_id"], ["C789"])
                self.assertEqual(form["thread_ts"], ["1712161829.000300"])
                self.assertEqual(form["initial_comment"], ["Attached"])
                return FakeHTTPResponse('{"ok": true, "files": [{"id": "FEVENT"}]}')
            raise AssertionError(f"unexpected request {request.full_url}")

        def fake_upload_open(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertIsNone(authorization_header(request))
            self.assertEqual(request.data, b"%PDF-1.4")
            return FakeHTTPResponse("OK - 8")

        with (
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch(
                "internals.client.urllib.request.build_opener",
                return_value=FakeOpener(fake_upload_open),
            ),
        ):
            result = provider_module.slack_events_upload_file(
                provider_module.SlackEventUploadFileInput(
                    reply_ref=reply_ref,
                    filename="event.pdf",
                    content_base64="JVBERi0xLjQ=",
                    content_type="application/pdf",
                    initial_comment="Attached",
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertEqual(operation_body(result)["ok"], True)
        self.assertEqual(operation_body(result)["file_id"], "FEVENT")
        self.assertEqual(operation_body(result)["channel"], "C789")

    def test_events_upload_file_preserves_blocks_with_initial_comment_contract(
        self,
    ) -> None:
        provider_module.configure("slack", {"bot": {"token": "xoxb-bot"}})
        self.addCleanup(provider_module.configure, "slack", {})
        reply_ref = self._signed_reply_ref()

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(authorization_header(request), "Bearer xoxb-bot")
            parsed = urllib.parse.urlsplit(request.full_url)
            if parsed.path == "/api/files.getUploadURLExternal":
                return FakeHTTPResponse(
                    '{"ok": true, "upload_url": "https://files.slack.com/upload/v1/EVENT", "file_id": "FEVENT"}'
                )
            if parsed.path == "/api/files.completeUploadExternal":
                form = request_form(request)
                self.assertNotIn("initial_comment", form)
                blocks = json.loads(form["blocks"][0])
                self.assertEqual(
                    blocks,
                    [
                        {
                            "type": "section",
                            "text": {"type": "mrkdwn", "text": "Attached"},
                        },
                        {"type": "divider"},
                    ],
                )
                return FakeHTTPResponse('{"ok": true, "files": [{"id": "FEVENT"}]}')
            raise AssertionError(f"unexpected request {request.full_url}")

        def fake_upload_open(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertIsNone(authorization_header(request))
            self.assertEqual(request.data, b"%PDF-1.4")
            return FakeHTTPResponse("OK - 8")

        with (
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch(
                "internals.client.urllib.request.build_opener",
                return_value=FakeOpener(fake_upload_open),
            ),
        ):
            result = provider_module.slack_events_upload_file(
                provider_module.SlackEventUploadFileInput(
                    reply_ref=reply_ref,
                    filename="event.pdf",
                    content_base64="JVBERi0xLjQ=",
                    content_type="application/pdf",
                    initial_comment="Attached",
                    blocks=[{"type": "divider"}],
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertEqual(operation_body(result)["ok"], True)

    def test_events_upload_file_rejects_foreign_reply_ref_contract(self) -> None:
        provider_module.configure("slack", {"bot": {"token": "xoxb-bot"}})
        self.addCleanup(provider_module.configure, "slack", {})
        reply_ref = self._signed_reply_ref(subject_id="user:other")

        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.slack_events_upload_file(
                provider_module.SlackEventUploadFileInput(
                    reply_ref=reply_ref,
                    filename="event.pdf",
                    content_base64="JVBERi0xLjQ=",
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertEqual(
            response.body, {"error": "reply_ref does not belong to this subject"}
        )
        urlopen.assert_not_called()

    def test_events_upload_file_validates_content_before_slack_contract(self) -> None:
        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.slack_events_upload_file(
                provider_module.SlackEventUploadFileInput(
                    reply_ref="invalid",
                    filename="event.pdf",
                ),
                gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.body, {"error": "content or content_base64 is required"}
        )
        urlopen.assert_not_called()

    def test_events_upload_file_requires_bot_token_contract(self) -> None:
        provider_module.configure("slack", {})
        self.addCleanup(provider_module.configure, "slack", {})

        result = provider_module.slack_events_upload_file(
            provider_module.SlackEventUploadFileInput(
                reply_ref="missing-token",
                filename="event.pdf",
                content_base64="JVBERi0xLjQ=",
            ),
            gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123")),
        )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.PRECONDITION_FAILED)
        self.assertEqual(response.body, {"error": "Slack bot token is not configured"})

    def test_files_get_rejects_non_slack_private_url_contract(self) -> None:
        result = provider_module.files_get(
            provider_module.GetFileInput(
                url_private="https://example.com/files-pri/T-F1/notes.txt"
            ),
            gestalt.Request(token="test-token"),
        )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.body,
            {"error": "url_private must be a Slack HTTPS file URL"},
        )

    def test_files_get_uses_files_info_and_private_download_contract(self) -> None:
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
                self.assertEqual(query, {"file": ["FIMG"]})
                return FakeHTTPResponse(
                    """
                    {
                      "ok": true,
                      "file": {
                        "id": "FIMG",
                        "name": "diagram.png",
                        "mimetype": "image/png",
                        "size": 8,
                        "url_private_download": "https://files.slack.com/files-pri/T-FIMG/diagram.png"
                      }
                    }
                    """
                )

            if call_count == 2:
                self.assertEqual(parsed.netloc, "files.slack.com")
                self.assertEqual(parsed.path, "/files-pri/T-FIMG/diagram.png")
                return FakeHTTPResponse("image-bytes")

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
                provider_module.GetFileInput(file_id="FIMG", max_bytes=64),
                gestalt.Request(token="test-token"),
            )

        data = result["data"]
        self.assertEqual(call_count, 2)
        self.assertEqual(data["file"]["id"], "FIMG")
        self.assertEqual(data["content"]["mime_type"], "image/png")
        self.assertEqual(data["content"]["encoding"], "base64")
        self.assertEqual(data["content"]["kind"], "image")
        self.assertEqual(data["content"]["data"], "aW1hZ2UtYnl0ZXM=")
        self.assertEqual(
            data["content"]["data_uri"],
            "data:image/png;base64,aW1hZ2UtYnl0ZXM=",
        )

    def test_files_get_allows_five_mib_download_request_contract(self) -> None:
        five_mib = 5 * 1024 * 1024
        download_read_sizes: list[int] = []

        class RecordingDownloadResponse:
            def __enter__(self) -> "RecordingDownloadResponse":
                return self

            def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
                return None

            def read(self, size: int = -1) -> bytes:
                download_read_sizes.append(size)
                return b"small attachment"

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse | RecordingDownloadResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(authorization_header(request), "Bearer test-token")
            parsed = urllib.parse.urlsplit(request.full_url)
            query = urllib.parse.parse_qs(parsed.query)

            if parsed.path == "/api/files.info":
                self.assertEqual(query, {"file": ["FIMG"]})
                return FakeHTTPResponse(
                    """
                    {
                      "ok": true,
                      "file": {
                        "id": "FIMG",
                        "name": "diagram.png",
                        "mimetype": "image/png",
                        "url_private_download": "https://files.slack.com/files-pri/T-FIMG/diagram.png"
                      }
                    }
                    """
                )

            self.assertEqual(parsed.netloc, "files.slack.com")
            self.assertEqual(parsed.path, "/files-pri/T-FIMG/diagram.png")
            return RecordingDownloadResponse()

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
                provider_module.GetFileInput(file_id="FIMG", max_bytes=five_mib),
                gestalt.Request(token="test-token"),
            )

        data = result["data"]
        self.assertEqual(download_read_sizes, [five_mib + 1])
        self.assertEqual(data["content"]["bytes_read"], len(b"small attachment"))
        self.assertFalse(data["content"]["truncated"])

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
        self.assertEqual(response.status, HTTPStatus.TOO_MANY_REQUESTS)
        self.assertEqual(response.body, {"error": "rate_limited"})

    def test_get_message_retries_slack_http_rate_limit_with_retry_after(
        self,
    ) -> None:
        calls = 0
        headers = Message()
        headers.add_header("Retry-After", "0")
        rate_limit = urllib.error.HTTPError(
            url="https://slack.com/api/conversations.history",
            code=429,
            msg="ratelimited",
            hdrs=headers,
            fp=io.BytesIO(b'{"ok": false, "error": "ratelimited"}'),
        )

        def fake_urlopen(
            _request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            nonlocal calls
            calls += 1
            self.assertEqual(timeout, 30)
            if calls == 1:
                raise rate_limit
            return FakeHTTPResponse(
                """
                {
                  "ok": true,
                  "messages": [
                    {"ts": "1234567890.123456", "text": "after retry"}
                  ]
                }
                """
            )

        with (
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch("internals.client.time.sleep") as sleep,
        ):
            result = provider_module.conversations_get_message(
                provider_module.GetMessageInput(channel="C123", ts="1234567890.123456"),
                gestalt.Request(token="test-token"),
            )

        self.assertEqual(calls, 2)
        sleep.assert_called_once_with(0.0)
        self.assertEqual(result["data"]["message"]["text"], "after retry")


if __name__ == "__main__":
    unittest.main()
