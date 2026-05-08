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
from email.message import Message
from http import HTTPStatus
from typing import Any, cast
from unittest import mock

import gestalt
import yaml
from google.protobuf import json_format
from google.protobuf import struct_pb2
from gestalt._gen.v1 import agent_pb2 as _agent_pb2
from gestalt._gen.v1 import authorization_pb2 as _authorization_pb2
from gestalt._gen.v1 import workflow_pb2 as _workflow_pb2

import provider as provider_module

agent_pb2: Any = _agent_pb2
authorization_pb2: Any = _authorization_pb2
workflow_pb2: Any = _workflow_pb2
PLUGIN_DIR = pathlib.Path(__file__).resolve().parents[1]
BASE_EVENT_TOOL_REFS = [
    ("slack", "conversations.getThreadContext"),
    ("slack", "conversations.getMessage"),
    ("slack", "files.get"),
    ("slack", "events.setStatus"),
    ("slack", "events.deleteStatus"),
    ("slack", "events.addReaction"),
    ("slack", "events.removeReaction"),
]
ASSISTANT_EVENT_TOOL_REFS = [
    ("slack", "events.setAssistantStatus"),
    ("slack", "events.clearAssistantStatus"),
    ("slack", "events.setThreadTitle"),
    ("slack", "events.setSuggestedPrompts"),
]
WORKFLOW_EVENT_TOOL_REFS = [
    ("slack", "interactions.request"),
]


def new_struct() -> Any:
    return getattr(struct_pb2, "Struct")()


class FakeWorkflowRunStatus:
    @staticmethod
    def Name(status: int) -> str:
        if status == FakeWorkflowPb2.WORKFLOW_RUN_STATUS_PENDING:
            return "WORKFLOW_RUN_STATUS_PENDING"
        return str(status)


class FakeWorkflowSignal:
    def __init__(
        self,
        id: str = "",
        name: str = "",
        idempotency_key: str = "",
        **_kwargs: Any,
    ) -> None:
        self.id = id
        self.name = name
        self.idempotency_key = idempotency_key
        self.payload = new_struct()
        self.metadata = new_struct()


class FakeBoundWorkflowRun:
    def __init__(self, id: str = "", status: int = 0, workflow_key: str = "") -> None:
        self.id = id
        self.status = status
        self.workflow_key = workflow_key


class FakeManagedWorkflowRunSignal:
    def __init__(
        self,
        provider_name: str = "",
        run: Any = None,
        signal: Any = None,
        started_run: bool = False,
        workflow_key: str = "",
    ) -> None:
        self.provider_name = provider_name
        self.run = run
        self.signal = signal
        self.started_run = started_run
        self.workflow_key = workflow_key


class FakeBoundWorkflowAgentTarget:
    def __init__(
        self,
        provider_name: str = "",
        model: str = "",
        prompt: str = "",
        messages: list[Any] | None = None,
        tool_refs: list[Any] | None = None,
        timeout_seconds: int = 0,
        output_delivery: Any = None,
        session_ready_delivery: Any = None,
        **_kwargs: Any,
    ) -> None:
        self.provider_name = provider_name
        self.model = model
        self.prompt = prompt
        self.messages = messages or []
        self.tool_refs = tool_refs or []
        self.timeout_seconds = timeout_seconds
        self.output_delivery = output_delivery
        self.session_ready_delivery = session_ready_delivery
        self.metadata = new_struct()
        self.model_options = new_struct()
        self.provider_options = self.model_options


class FakeBoundWorkflowPluginTarget:
    def __init__(
        self,
        plugin_name: str = "",
        operation: str = "",
        connection: str = "",
        instance: str = "",
        **_kwargs: Any,
    ) -> None:
        self.plugin_name = plugin_name
        self.operation = operation
        self.connection = connection
        self.instance = instance
        self.input = new_struct()


class FakeWorkflowOutputValueSource:
    def __init__(
        self,
        agent_output: str = "",
        signal_payload: str = "",
        signal_metadata: str = "",
        agent_session: str = "",
        literal: Any = None,
        **_kwargs: Any,
    ) -> None:
        self.agent_output = agent_output
        self.signal_payload = signal_payload
        self.signal_metadata = signal_metadata
        self.agent_session = agent_session
        self.literal = literal

    def WhichOneof(self, _name: str) -> str | None:
        if self.agent_output:
            return "agent_output"
        if self.signal_payload:
            return "signal_payload"
        if self.signal_metadata:
            return "signal_metadata"
        if self.agent_session:
            return "agent_session"
        if self.literal is not None:
            return "literal"
        return None


class FakeWorkflowOutputBinding:
    def __init__(
        self,
        input_field: str = "",
        value: Any = None,
        **_kwargs: Any,
    ) -> None:
        self.input_field = input_field
        self.value = value


class FakeWorkflowOutputDelivery:
    def __init__(
        self,
        target: Any = None,
        input_bindings: list[Any] | None = None,
        credential_mode: str = "",
        **_kwargs: Any,
    ) -> None:
        self.target = target
        self.input_bindings = input_bindings or []
        self.credential_mode = credential_mode


class FakeBoundWorkflowTarget:
    def __init__(self, agent: Any = None, **_kwargs: Any) -> None:
        self.agent = agent


class FakeWorkflowManagerSignalOrStartRunRequest:
    def __init__(
        self,
        provider_name: str = "",
        workflow_key: str = "",
        idempotency_key: str = "",
        target: Any = None,
        signal: Any = None,
        **_kwargs: Any,
    ) -> None:
        self.provider_name = provider_name
        self.workflow_key = workflow_key
        self.idempotency_key = idempotency_key
        self.target = target
        self.signal = signal


class FakeWorkflowEvent:
    def __init__(
        self,
        id: str = "",
        source: str = "",
        spec_version: str = "",
        type: str = "",
        subject: str = "",
        datacontenttype: str = "",
        **_kwargs: Any,
    ) -> None:
        self.id = id
        self.source = source
        self.spec_version = spec_version
        self.type = type
        self.subject = subject
        self.datacontenttype = datacontenttype
        self.data = new_struct()


class FakeWorkflowManagerPublishEventRequest:
    def __init__(
        self,
        event: Any = None,
        provider_name: str = "",
        **_kwargs: Any,
    ) -> None:
        self.event = event or FakeWorkflowEvent()
        self.provider_name = provider_name


class FakeWorkflowPb2:
    WORKFLOW_RUN_STATUS_PENDING = 1
    WorkflowRunStatus = FakeWorkflowRunStatus
    WorkflowEvent = FakeWorkflowEvent
    WorkflowSignal = FakeWorkflowSignal
    BoundWorkflowRun = FakeBoundWorkflowRun
    ManagedWorkflowRunSignal = FakeManagedWorkflowRunSignal
    BoundWorkflowAgentTarget = FakeBoundWorkflowAgentTarget
    BoundWorkflowPluginTarget = FakeBoundWorkflowPluginTarget
    BoundWorkflowTarget = FakeBoundWorkflowTarget
    WorkflowOutputBinding = FakeWorkflowOutputBinding
    WorkflowOutputDelivery = FakeWorkflowOutputDelivery
    WorkflowOutputValueSource = FakeWorkflowOutputValueSource
    WorkflowManagerSignalOrStartRunRequest = FakeWorkflowManagerSignalOrStartRunRequest
    WorkflowManagerPublishEventRequest = FakeWorkflowManagerPublishEventRequest


def workflow_pb2_with_signal_or_start_contract() -> Any:
    if hasattr(workflow_pb2, "WorkflowManagerSignalOrStartRunRequest") and hasattr(
        workflow_pb2, "WorkflowOutputDelivery"
    ):
        return workflow_pb2
    return FakeWorkflowPb2


class FakeHTTPResponse:
    def __init__(self, body: str) -> None:
        self._body = body.encode("utf-8")

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


def tool_ref_pairs(refs: Any) -> list[tuple[str, str]]:
    return [
        (str(getattr(ref, "system", "") or ref.plugin), str(ref.operation))
        for ref in refs
    ]


def tool_ref_details(refs: Any) -> list[tuple[str, str, str, str, str, str]]:
    return [
        (
            str(ref.plugin),
            str(ref.operation),
            str(getattr(ref, "connection", "")),
            str(getattr(ref, "instance", "")),
            str(getattr(ref, "title", "")),
            str(getattr(ref, "description", "")),
        )
        for ref in refs
    ]


def output_delivery_bindings(delivery: Any) -> dict[str, tuple[str | None, Any]]:
    out: dict[str, tuple[str | None, Any]] = {}
    for binding in delivery.input_bindings:
        value = binding.value
        kind = value.WhichOneof("kind") if hasattr(value, "WhichOneof") else None
        out[str(binding.input_field)] = (
            kind,
            getattr(value, kind, None) if kind else None,
        )
    return out


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
    channel_id: str = "C_ROUTE",
) -> dict[str, Any]:
    event = provider_module.SlackAgentEvent(
        callback_type="event_callback",
        event_type="app_mention",
        event_id="EvRouteInteraction",
        team_id="T123",
        user_id="U456",
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
        "user": {"id": "U456"},
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


def _manifest_parameter_names(operation: dict[str, Any]) -> list[str]:
    return [parameter["name"] for parameter in operation.get("parameters", [])]


def _manifest_parameter_types(operation: dict[str, Any], name: str) -> list[str]:
    return [
        parameter["type"]
        for parameter in operation.get("parameters", [])
        if parameter["name"] == name
    ]


class FakeAuthorization:
    def __init__(self, subjects: list[Any]) -> None:
        self.subjects = subjects
        self.requests: list[Any] = []

    def search_subjects(self, request: Any) -> Any:
        self.requests.append(request)
        subject_type = str(getattr(request, "subject_type", "") or "").strip()
        subjects = [
            subject
            for subject in self.subjects
            if not subject_type or str(subject.type or "").strip() == subject_type
        ]
        return authorization_pb2.SubjectSearchResponse(subjects=subjects)


class FakeWorkflowManager:
    def __init__(self) -> None:
        self.signal_or_start_requests: list[Any] = []
        self.publish_event_requests: list[Any] = []
        self.signal_or_start_error: Exception | None = None
        self.publish_event_error: Exception | None = None

    def __enter__(self) -> FakeWorkflowManager:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def signal_or_start_run(self, request: Any) -> Any:
        self.signal_or_start_requests.append(request)
        if self.signal_or_start_error is not None:
            raise self.signal_or_start_error
        return workflow_pb2.ManagedWorkflowRunSignal(
            provider_name=request.provider_name or "local",
            run=workflow_pb2.BoundWorkflowRun(
                id="run-123",
                status=workflow_pb2.WORKFLOW_RUN_STATUS_PENDING,
                workflow_key=request.workflow_key,
            ),
            signal=workflow_pb2.WorkflowSignal(
                id="signal-123",
                name=request.signal.name,
                idempotency_key=request.signal.idempotency_key,
            ),
            started_run=True,
            workflow_key=request.workflow_key,
        )

    def publish_event(self, request: Any) -> Any:
        self.publish_event_requests.append(request)
        if self.publish_event_error is not None:
            raise self.publish_event_error
        return request.event


class ExplodingPublishResponseWorkflowManager(FakeWorkflowManager):
    def publish_event(self, request: Any) -> Any:
        self.publish_event_requests.append(request)

        class Response:
            @property
            def id(self) -> str:
                raise RuntimeError("bad publish response")

        return Response()


class DictWorkflowManager(FakeWorkflowManager):
    def signal_or_start_run(self, request: Any) -> Any:
        self.signal_or_start_requests.append(request)
        return {
            "providerName": request.provider_name or "local",
            "run": {
                "id": "run-123",
                "status": workflow_pb2.WORKFLOW_RUN_STATUS_PENDING,
                "workflowKey": request.workflow_key,
            },
            "signal": {"id": "signal-123"},
            "startedRun": True,
            "workflowKey": request.workflow_key,
        }


def agent_options(agent_target: Any) -> Any:
    return getattr(
        agent_target,
        "provider_options",
        getattr(agent_target, "model_options", new_struct()),
    )


def slack_replies_response(
    messages: list[dict[str, Any]] | None = None,
    *,
    has_more: bool = False,
    next_cursor: str = "",
) -> FakeHTTPResponse:
    return FakeHTTPResponse(
        json.dumps(
            {
                "ok": True,
                "messages": messages or [],
                "has_more": has_more,
                "response_metadata": {"next_cursor": next_cursor},
            }
        )
    )


class SlackProviderTests(unittest.TestCase):
    def _handle_event_with_workflow(
        self, payload: dict[str, Any]
    ) -> tuple[Any, FakeWorkflowManager]:
        workflow_manager = FakeWorkflowManager()
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )
        return response, workflow_manager

    def test_agent_module_reexports_model_interfaces(self) -> None:
        import internals.agent as agent_module
        import internals.models as models_module

        for name in (
            "SlackAcknowledgementConfig",
            "SlackAgentConfig",
            "SlackAgentEvent",
            "SlackAgentRoute",
            "SlackAgentRouteMatch",
            "SlackAgentToolRef",
            "SlackAssistantConfig",
            "SlackBotConfig",
            "SlackCallbackType",
            "SlackChannelType",
            "SlackEventType",
            "SlackInteractionRef",
            "SlackReplyRef",
            "SlackThreadContextConfig",
            "SlackWorkflowConfig",
        ):
            self.assertIs(getattr(agent_module, name), getattr(models_module, name))

    def test_agent_tools_reject_non_exact_or_runtime_policy_fields(self) -> None:
        invalid_tools = [
            {"plugin": "*", "operation": ""},
            {"plugin": "linear", "operation": ""},
            {"plugin": "system", "operation": "shell"},
            {"system": "shell", "operation": "run"},
            {"system": "workflow", "operation": ""},
            {"system": "workflow", "operation": "schedules.create", "plugin": "linear"},
            {"plugin": "linear", "operation": "searchIssues", "credentialMode": "none"},
            {"plugin": "linear", "operation": "searchIssues", "runAs": "user"},
            {
                "plugin": "linear",
                "operation": "searchIssues",
                "inputBindings": [],
            },
            {"plugin": "linear", "operation": "searchIssues", "system": True},
        ]

        for tool in invalid_tools:
            with self.subTest(tool=tool):
                with self.assertRaises(ValueError):
                    provider_module.configure(
                        "slack",
                        {
                            "agent": {
                                "provider": "simple",
                                "model": "deep",
                                "tools": [tool],
                            }
                        },
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
                            "provider": "simple",
                            "model": "deep",
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

                response, workflow_manager = self._handle_event_with_workflow(payload)

                self.assertEqual(response["ok"], True)
                self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
                signal_metadata = json_format.MessageToDict(
                    workflow_manager.signal_or_start_requests[0].signal.metadata
                )
                self.assertEqual(
                    signal_metadata["slack"]["agent_route_id"],
                    "all-channel-messages",
                )

    def test_agent_timeout_seconds_maps_to_workflow_target(self) -> None:
        def target_for_config(config: dict[str, Any], *, channel: str = "C789") -> Any:
            provider_module.configure("slack", config)
            self.addCleanup(provider_module.configure, "slack", {})
            workflow_manager = FakeWorkflowManager()
            payload = {
                "type": "event_callback",
                "event_id": "EvTimeout",
                "team_id": "T123",
                "event": {
                    "type": "app_mention",
                    "user": "U456",
                    "channel": channel,
                    "channel_type": "channel",
                    "text": "<@UBOT> hello",
                    "ts": "1712161829.000300",
                },
            }
            workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
            with (
                mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
                mock.patch.object(
                    gestalt.Request,
                    "workflow_manager",
                    return_value=workflow_manager,
                    create=True,
                ),
            ):
                response = provider_module.slack_events_handle(
                    payload,
                    gestalt.Request(
                        subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                    ),
                )
            self.assertEqual(response["ok"], True)
            return workflow_manager.signal_or_start_requests[0].target.agent

        base_config = {
            "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
            "workflow": {"provider": "local"},
            "agent": {"provider": "simple", "model": "deep"},
        }

        with self.subTest("top-level camelCase"):
            target = target_for_config(
                {
                    **base_config,
                    "agent": {
                        **base_config["agent"],
                        "timeoutSeconds": 1800,
                    },
                }
            )
            self.assertEqual(target.timeout_seconds, 1800)

        with self.subTest("top-level snake_case string"):
            target = target_for_config(
                {
                    **base_config,
                    "agent": {
                        **base_config["agent"],
                        "timeout_seconds": "1200",
                    },
                }
            )
            self.assertEqual(target.timeout_seconds, 1200)

        with self.subTest("top-level whole-number float"):
            target = target_for_config(
                {
                    **base_config,
                    "agent": {
                        **base_config["agent"],
                        "timeoutSeconds": 1800.0,
                    },
                }
            )
            self.assertEqual(target.timeout_seconds, 1800)

        with self.subTest("route agent override"):
            target = target_for_config(
                {
                    **base_config,
                    "agent": {
                        **base_config["agent"],
                        "timeoutSeconds": 1800,
                        "routes": [
                            {
                                "id": "short-route",
                                "match": {"channel": "C_ROUTE"},
                                "agent": {"timeoutSeconds": 900},
                            }
                        ],
                    },
                },
                channel="C_ROUTE",
            )
            self.assertEqual(target.timeout_seconds, 900)

        with self.subTest("route-level override"):
            target = target_for_config(
                {
                    **base_config,
                    "agent": {
                        **base_config["agent"],
                        "timeoutSeconds": 1800,
                        "routes": [
                            {
                                "id": "route-level",
                                "match": {"channel": "C_ROUTE"},
                                "timeout_seconds": 600,
                            }
                        ],
                    },
                },
                channel="C_ROUTE",
            )
            self.assertEqual(target.timeout_seconds, 600)

        with self.subTest("omitted timeout"):
            target = target_for_config(base_config)
            self.assertEqual(target.timeout_seconds, 0)

    def test_agent_timeout_seconds_rejects_invalid_values(self) -> None:
        invalid_values = [True, 0, -1, 1.5, "ten minutes", 2_147_483_648]
        for value in invalid_values:
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "timeoutSeconds"):
                    provider_module.configure(
                        "slack",
                        {
                            "agent": {
                                "provider": "simple",
                                "model": "deep",
                                "timeoutSeconds": value,
                            }
                        },
                    )

    def test_route_and_tool_set_tools_reject_runtime_policy_fields(self) -> None:
        invalid_configs = [
            {
                "agent": {
                    "routes": [
                        {
                            "id": "route-tools",
                            "agent": {
                                "tools": [
                                    {
                                        "plugin": "linear",
                                        "operation": "searchIssues",
                                        "runAs": "user",
                                    }
                                ]
                            },
                        }
                    ]
                }
            },
            {
                "agent": {
                    "toolSets": {
                        "unsafe": [
                            {
                                "plugin": "linear",
                                "operation": "searchIssues",
                                "credentialMode": "none",
                            }
                        ]
                    }
                }
            },
        ]

        for config in invalid_configs:
            with self.subTest(config=config):
                with self.assertRaises(ValueError):
                    provider_module.configure("slack", config)

    def test_catalog_and_manifest_expose_native_assistant_contracts(self) -> None:
        catalog = yaml.safe_load((PLUGIN_DIR / "catalog.yaml").read_text())
        manifest = yaml.safe_load((PLUGIN_DIR / "manifest.yaml").read_text())
        catalog_ops = {op["id"]: op for op in catalog["operations"]}
        rest_ops = {
            op["name"]: op for op in manifest["spec"]["surfaces"]["rest"]["operations"]
        }
        http_routes = manifest["spec"]["http"]

        self.assertEqual(
            _catalog_parameter_names(catalog_ops["events.reply"]),
            ["reply_ref", "text"],
        )
        self.assertIn(
            "requires reply_ref and text", catalog_ops["events.reply"]["description"]
        )
        reply_parameters = {
            parameter["name"]: parameter
            for parameter in catalog_ops["events.reply"]["parameters"]
        }
        self.assertIn(
            "current Slack signal", reply_parameters["reply_ref"]["description"]
        )
        self.assertIn(
            "complete Slack message body", reply_parameters["text"]["description"]
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
        self.assertNotIn("assistant.reconcileStuckRequests", catalog_ops)
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

    def test_manifest_models_bot_connection_as_platform_bearer(self) -> None:
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
        self.assertEqual(
            connections["default"]["postConnect"],
            {
                "request": {
                    "method": "POST",
                    "url": "https://slack.com/api/auth.test",
                },
                "success": {"path": "ok", "equals": True},
                "externalIdentity": {
                    "type": "slack_identity",
                    "id": "team:{team_id}:user:{user_id}",
                },
                "metadata": {
                    "slack.team_id": "team_id",
                    "slack.user_id": "user_id",
                },
            },
        )
        bot_connection = connections["bot"]
        self.assertEqual(bot_connection["mode"], "platform")
        self.assertEqual(bot_connection["exposure"], "internal")
        self.assertEqual(bot_connection["auth"], {"type": "bearer"})
        self.assertNotIn("postConnect", bot_connection)
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

        operation = rest_ops["chat.postMessage"]
        self.assertEqual(
            operation["connectionSelector"],
            {
                "parameter": "actor",
                "default": "user",
                "values": {"bot": "bot", "user": "default"},
            },
        )
        self.assertIn("actor", _manifest_parameter_names(operation))

        self.assertEqual(rest_ops["search.messages"]["connection"], "default")
        self.assertNotIn("connectionSelector", rest_ops["search.messages"])
        self.assertIn("im:write", connections["default"]["auth"]["scopes"])
        self.assertIn("mpim:write", connections["default"]["auth"]["scopes"])
        self.assertIn("users:read.email", connections["default"]["auth"]["scopes"])
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

    def test_http_subject_resolves_slack_user_through_managed_external_identity(
        self,
    ) -> None:
        subject = authorization_pb2.Subject(type="subject", id="user:gestalt-123")
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
        self.assertEqual(request.subject_type, "")

    def test_http_subject_dedupes_equivalent_managed_external_identity_subjects(
        self,
    ) -> None:
        canonical = authorization_pb2.Subject(type="subject", id="user:gestalt-123")
        canonical.properties.update({"email": "ada@example.com"})
        legacy = authorization_pb2.Subject(type="user", id="user:gestalt-123")
        authorization = FakeAuthorization([legacy, canonical])
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
            gestalt.Request, "authorization", return_value=authorization
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
        subject = authorization_pb2.Subject(type="subject", id="user:gestalt-123")
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
        self.assertEqual(len(authorization.requests), 1)

    def test_slack_event_handler_signals_workflow_with_exact_tools(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "modelOptions": {"temperature": 0},
                    "tools": [
                        {
                            "plugin": "linear",
                            "operation": "searchIssues",
                            "connection": "default",
                            "instance": "main",
                            "title": "Search Linear issues",
                            "description": "Find Linear issues relevant to the Slack request.",
                        },
                        {"plugin": "statusPage", "operation": "status"},
                        {"system": "workflow", "operation": "definitions.create"},
                        {"system": "workflow", "operation": "schedules.create"},
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
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
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()

        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(
            response,
            {
                "ok": True,
                "workflow_provider": "local",
                "workflow_run_id": "run-123",
                "workflow_key": "slack:T123:C789:1712161829.000300",
                "workflow_signal_id": "signal-123",
                "started_run": True,
                "status": "WORKFLOW_RUN_STATUS_PENDING",
            },
        )
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)

        workflow_request = workflow_manager.signal_or_start_requests[0]
        self.assertEqual(workflow_request.provider_name, "local")
        self.assertEqual(
            workflow_request.workflow_key, "slack:T123:C789:1712161829.000300"
        )
        expected_idempotency_key = "slack:event:T123:C789:1712161829.000300:U456"
        self.assertEqual(workflow_request.idempotency_key, expected_idempotency_key)

        agent_target = workflow_request.target.agent
        self.assertEqual(agent_target.provider_name, "simple")
        self.assertEqual(agent_target.model, "deep")
        self.assertIn("final workflow signal batch", agent_target.prompt)
        self.assertIn("agent_request", agent_target.prompt)
        self.assertIn("current_message", agent_target.prompt)
        self.assertIn("payload.user_prompt", agent_target.prompt)
        self.assertIn("Background thread context", agent_target.prompt)
        self.assertNotIn(
            "Use the payload's user_prompt as the current Slack request",
            agent_target.prompt,
        )
        self.assertEqual(len(agent_target.messages), 1)
        self.assertEqual(
            tool_ref_pairs(agent_target.tool_refs),
            [
                ("linear", "searchIssues"),
                ("statusPage", "status"),
                ("workflow", "definitions.create"),
                ("workflow", "schedules.create"),
                *BASE_EVENT_TOOL_REFS,
                *WORKFLOW_EVENT_TOOL_REFS,
            ],
        )
        self.assertNotIn(("*", ""), tool_ref_pairs(agent_target.tool_refs))
        self.assertEqual(
            tool_ref_details(agent_target.tool_refs)[0],
            (
                "linear",
                "searchIssues",
                "default",
                "main",
                "Search Linear issues",
                "Find Linear issues relevant to the Slack request.",
            ),
        )
        self.assertEqual(
            agent_target.output_delivery.target.plugin_name,
            "slack",
        )
        self.assertEqual(
            agent_target.output_delivery.target.operation,
            "events.reply",
        )
        self.assertEqual(agent_target.output_delivery.credential_mode, "none")
        self.assertEqual(
            output_delivery_bindings(agent_target.output_delivery),
            {
                "text": ("agent_output", "text"),
                "reply_ref": ("signal_payload", "reply_ref"),
            },
        )
        self.assertEqual(
            agent_target.session_ready_delivery.target.plugin_name,
            "slack",
        )
        self.assertEqual(
            agent_target.session_ready_delivery.target.operation,
            "events.replySessionStarted",
        )
        self.assertEqual(agent_target.session_ready_delivery.credential_mode, "none")
        self.assertEqual(
            output_delivery_bindings(agent_target.session_ready_delivery),
            {
                "session_id": ("agent_session", "id"),
                "reply_ref": ("signal_payload", "reply_ref"),
            },
        )
        self.assertNotIn("slack.events.reply", agent_target.messages[0].text)
        self.assertIn("final assistant answer", agent_target.messages[0].text)
        self.assertIn("slack.events.setStatus", agent_target.messages[0].text)
        self.assertIn("slack.interactions.request", agent_target.messages[0].text)
        self.assertNotIn("slack.events.startStream", agent_target.messages[0].text)
        self.assertNotIn("slack.events.appendStream", agent_target.messages[0].text)
        self.assertNotIn("slack.events.stopStream", agent_target.messages[0].text)
        self.assertIn(
            "slack.conversations.getThreadContext", agent_target.messages[0].text
        )
        self.assertIn("slack.files.get", agent_target.messages[0].text)
        self.assertNotIn("slack.chat.postMessage", agent_target.messages[0].text)

        target_metadata = json_format.MessageToDict(agent_target.metadata)
        self.assertEqual(target_metadata["slack"]["team_id"], "T123")
        self.assertEqual(target_metadata["slack"]["channel_id"], "C789")
        self.assertEqual(
            target_metadata["slack"]["root_message_ts"], "1712161829.000300"
        )
        self.assertNotIn("event_id", target_metadata["slack"])
        model_options = json_format.MessageToDict(agent_options(agent_target))
        self.assertEqual(model_options["temperature"], 0)

        signal = workflow_request.signal
        self.assertEqual(signal.name, "slack.event")
        self.assertEqual(signal.idempotency_key, expected_idempotency_key)
        signal_payload = json_format.MessageToDict(signal.payload)
        agent_request = signal_payload["agent_request"]
        self.assertEqual(
            set(agent_request.keys()), {"kind", "user_prompt", "current_message"}
        )
        self.assertEqual(agent_request["kind"], "slack.event")
        self.assertEqual(agent_request["user_prompt"], signal_payload["user_prompt"])
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
        self.assertEqual(signal_payload["slack"]["event_id"], "Ev123")
        self.assertEqual(signal_payload["slack"]["file_ids"], ["F123"])
        self.assertEqual(signal_payload["slack"]["addressed_to_bot"], True)
        self.assertEqual(signal_payload["slack"]["assistant_context_present"], False)
        self.assertEqual(
            signal_payload["slack"]["text"],
            "<@UBOT> summarize deploy status"
            " https://example.slack.com/archives/C123/p1712161800000100",
        )
        self.assertEqual(current_message["text"], signal_payload["slack"]["text"])
        self.assertIn(
            "operation: slack.conversations.getThreadContext",
            signal_payload["user_prompt"],
        )
        self.assertIn("Slack message permalink tools:", signal_payload["user_prompt"])
        self.assertIn(
            "- url: https://example.slack.com/archives/C123/p1712161800000100",
            signal_payload["user_prompt"],
        )
        self.assertIn(
            'input: {"url": "https://example.slack.com/archives/C123/p1712161800000100"}',
            signal_payload["user_prompt"],
        )
        self.assertIn(
            "id=F123 name=diagram.png mimetype=image/png size=12",
            signal_payload["user_prompt"],
        )
        self.assertIn(
            "reply_thread_ts: 1712161829.000300", signal_payload["user_prompt"]
        )
        reply_ref = signal_payload["reply_ref"]
        self.assertIn(f"reply_ref: {reply_ref}", signal_payload["user_prompt"])
        self.assertNotIn("Final reply tool:", signal_payload["user_prompt"])
        self.assertNotIn("operation: slack.events.reply", signal_payload["user_prompt"])
        verified_ref = provider_module._verify_reply_ref(reply_ref, "user:gestalt-123")
        self.assertEqual(verified_ref.team_id, "T123")
        self.assertEqual(verified_ref.channel_id, "C789")
        self.assertEqual(verified_ref.message_ts, "1712161829.000300")
        self.assertEqual(verified_ref.reply_thread_ts, "1712161829.000300")
        self.assertEqual(verified_ref.user_id, "U456")
        self.assertEqual(verified_ref.channel_type, "channel")
        self.assertEqual(verified_ref.subject_id, "user:gestalt-123")

        signal_metadata = json_format.MessageToDict(signal.metadata)
        self.assertEqual(signal_metadata["slack"]["event_id"], "Ev123")
        self.assertEqual(signal_metadata["slack"]["user_id"], "U456")
        self.assertEqual(signal_metadata["slack"]["file_ids"], ["F123"])
        self.assertEqual(signal_metadata["slack"]["addressed_to_bot"], True)

    def test_thread_reply_prefetches_context_in_workflow_signal(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "threadContext": {"maxMessages": 50},
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvThreadReply",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> please summarize",
                "ts": "1712161835.000400",
                "thread_ts": "1712161829.000300",
            },
        }
        calls: list[dict[str, str]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(authorization_header(request), "Bearer xoxb-test-bot")
            parsed = urllib.parse.urlsplit(request.full_url)
            self.assertEqual(parsed.path, "/api/conversations.replies")
            query = dict(urllib.parse.parse_qsl(parsed.query))
            calls.append(query)
            self.assertEqual(query["channel"], "C789")
            self.assertEqual(query["ts"], "1712161829.000300")
            self.assertEqual(query["limit"], "50")
            self.assertNotIn("cursor", query)
            return slack_replies_response(
                [
                    {
                        "type": "message",
                        "user": "U123",
                        "text": "Root request",
                        "ts": "1712161829.000300",
                        "reply_count": 2,
                        "files": [{"id": "F123", "name": "context.txt"}],
                    },
                    {
                        "type": "message",
                        "user": "U456",
                        "text": "Follow up with more details",
                        "ts": "1712161835.000400",
                        "thread_ts": "1712161829.000300",
                    },
                    {
                        "type": "message",
                        "bot_id": "B123",
                        "username": "Deploy Bot",
                        "text": "bot output",
                        "ts": "1712161836.000500",
                        "thread_ts": "1712161829.000300",
                    },
                ]
            )

        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
        workflow_request = workflow_manager.signal_or_start_requests[0]
        signal_payload = json_format.MessageToDict(workflow_request.signal.payload)
        thread_context = signal_payload["slack"]["thread_context"]
        self.assertEqual(thread_context["source"], "bot")
        self.assertEqual(thread_context["channel"], "C789")
        self.assertEqual(thread_context["thread_ts"], "1712161829.000300")
        self.assertEqual(thread_context["messages_returned"], 3)
        self.assertEqual(thread_context["has_more"], False)
        self.assertEqual(thread_context["truncated"], False)
        self.assertEqual(thread_context["messages"][2]["bot_id"], "B123")
        self.assertEqual(thread_context["files"][0]["id"], "F123")
        self.assertNotIn("thread_context_error", signal_payload["slack"])
        self.assertIn("Background thread context:", signal_payload["user_prompt"])
        self.assertNotIn("Prefetched thread context:", signal_payload["user_prompt"])
        self.assertLess(
            signal_payload["user_prompt"].index("Message text:"),
            signal_payload["user_prompt"].index("Background thread context:"),
        )
        self.assertIn('"text": "Root request"', signal_payload["user_prompt"])
        self.assertIn('"bot_id": "B123"', signal_payload["user_prompt"])
        self.assertIn(
            "operation: slack.conversations.getThreadContext",
            signal_payload["user_prompt"],
        )

    def test_thread_context_prefetch_can_be_disabled(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "threadContext": {"enabled": False},
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvThreadNoPrefetch",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> please summarize",
                "ts": "1712161835.000400",
                "thread_ts": "1712161829.000300",
            },
        }
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch("internals.client.urllib.request.urlopen") as urlopen,
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        urlopen.assert_not_called()
        signal_payload = json_format.MessageToDict(
            workflow_manager.signal_or_start_requests[0].signal.payload
        )
        self.assertNotIn("thread_context", signal_payload["slack"])
        self.assertNotIn("thread_context_error", signal_payload["slack"])
        self.assertNotIn("Prefetched thread context:", signal_payload["user_prompt"])
        self.assertNotIn("Background thread context:", signal_payload["user_prompt"])
        self.assertIn(
            "operation: slack.conversations.getThreadContext",
            signal_payload["user_prompt"],
        )

    def test_thread_context_prefetch_error_still_signals_workflow(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvThreadPrefetchError",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> please summarize",
                "ts": "1712161835.000400",
                "thread_ts": "1712161829.000300",
            },
        }

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(authorization_header(request), "Bearer xoxb-test-bot")
            parsed = urllib.parse.urlsplit(request.full_url)
            self.assertEqual(parsed.path, "/api/conversations.replies")
            return FakeHTTPResponse('{"ok": false, "error": "channel_not_found"}')

        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
        signal_payload = json_format.MessageToDict(
            workflow_manager.signal_or_start_requests[0].signal.payload
        )
        error = signal_payload["slack"]["thread_context_error"]
        self.assertEqual(error["source"], "bot")
        self.assertEqual(error["channel"], "C789")
        self.assertEqual(error["thread_ts"], "1712161829.000300")
        self.assertEqual(error["type"], "slack_api")
        self.assertEqual(error["status"], HTTPStatus.BAD_GATEWAY)
        self.assertEqual(error["error"], "channel_not_found")
        self.assertNotIn("thread_context", signal_payload["slack"])
        self.assertIn("Background thread context error:", signal_payload["user_prompt"])
        self.assertNotIn(
            "Prefetched thread context error:", signal_payload["user_prompt"]
        )

    def test_thread_context_prefetch_clamps_oversized_max_messages(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "threadContext": {"maxMessages": 10000},
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvThreadPrefetchClamp",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> please summarize",
                "ts": "1712161835.000400",
                "thread_ts": "1712161829.000300",
            },
        }

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "GET")
            parsed = urllib.parse.urlsplit(request.full_url)
            self.assertEqual(parsed.path, "/api/conversations.replies")
            query = dict(urllib.parse.parse_qsl(parsed.query))
            self.assertEqual(query["limit"], "1000")
            return slack_replies_response(
                [
                    {
                        "type": "message",
                        "user": "U123",
                        "text": "Root request",
                        "ts": "1712161829.000300",
                    }
                ],
                has_more=True,
                next_cursor="next-page",
            )

        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        signal_payload = json_format.MessageToDict(
            workflow_manager.signal_or_start_requests[0].signal.payload
        )
        thread_context = signal_payload["slack"]["thread_context"]
        self.assertEqual(thread_context["messages_returned"], 1)
        self.assertEqual(thread_context["has_more"], True)
        self.assertEqual(thread_context["next_cursor"], "next-page")
        self.assertEqual(thread_context["truncated"], True)

    def test_group_message_with_assistant_context_starts_agent_thread(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
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
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()

        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(response["ok"], True)
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
        workflow_request = workflow_manager.signal_or_start_requests[0]
        self.assertEqual(
            workflow_request.workflow_key,
            "slack:T123:C0AH7JWFYM8:1777853873.601629",
        )
        signal_payload = json_format.MessageToDict(workflow_request.signal.payload)
        self.assertEqual(
            signal_payload["slack"]["reply_thread_ts"], "1777853873.601629"
        )
        self.assertEqual(signal_payload["slack"]["addressed_to_bot"], True)
        self.assertEqual(signal_payload["slack"]["assistant_context_present"], True)
        self.assertEqual(signal_payload["slack"]["bot_user_id"], "U0A8T4M41NY")
        self.assertEqual(
            signal_payload["slack"]["client_msg_id"],
            "163efdd3-cb7d-4348-92fc-e6e2815b2bcb",
        )

    def test_authorized_bot_mention_starts_group_message(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
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
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()

        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(response["ok"], True)
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
        workflow_request = workflow_manager.signal_or_start_requests[0]
        signal_payload = json_format.MessageToDict(workflow_request.signal.payload)
        self.assertEqual(
            signal_payload["slack"]["reply_thread_ts"], "1712161829.000300"
        )
        self.assertEqual(signal_payload["slack"]["addressed_to_bot"], True)
        self.assertEqual(signal_payload["slack"]["assistant_context_present"], False)
        self.assertEqual(signal_payload["slack"]["bot_user_id"], "UBOT")

    def test_app_mention_and_message_event_share_agent_signal_idempotency(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
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
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()

        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            provider_module.slack_events_handle(app_mention, request)
            provider_module.slack_events_handle(message, request)

        self.assertEqual(len(workflow_manager.signal_or_start_requests), 2)
        first, second = workflow_manager.signal_or_start_requests
        expected_idempotency_key = "slack:event:T123:C789:1712161829.000300:U456"
        self.assertEqual(first.workflow_key, "slack:T123:C789:1712161829.000300")
        self.assertEqual(second.workflow_key, first.workflow_key)
        self.assertEqual(first.idempotency_key, expected_idempotency_key)
        self.assertEqual(second.idempotency_key, expected_idempotency_key)
        self.assertEqual(first.signal.idempotency_key, expected_idempotency_key)
        self.assertEqual(second.signal.idempotency_key, expected_idempotency_key)
        first_metadata = json_format.MessageToDict(first.signal.metadata)
        second_metadata = json_format.MessageToDict(second.signal.metadata)
        self.assertEqual(first_metadata["slack"]["event_type"], "app_mention")
        self.assertEqual(second_metadata["slack"]["event_type"], "message")

    def test_slack_event_ack_failure_still_acks_dispatched_workflow(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agentProvider": "simple",
                "agentModel": "deep",
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvBadAckNoPublish",
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
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()

        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                provider_module._agent,
                "_workflow_signal_response_fields",
                side_effect=RuntimeError("bad response"),
            ),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(
            response,
            {
                "ok": True,
                "workflow_dispatched": True,
                "workflow_acknowledgement_failed": True,
            },
        )
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)

    def test_slack_event_signal_failure_without_publish_returns_server_error(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agentProvider": "simple",
                "agentModel": "deep",
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        workflow_manager.signal_or_start_error = RuntimeError("signal failed")
        payload = {
            "type": "event_callback",
            "event_id": "EvSignalFailNoPublish",
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
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()

        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            result = provider_module.slack_events_handle(payload, request)

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(
            response.body,
            {"error": "failed to signal workflow run: signal failed"},
        )
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)

    def test_slack_event_handler_notifies_unlinked_user(self) -> None:
        provider_module.configure("slack", {"bot": {"token": "xoxb-test-bot"}})
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
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(authorization_header(request), "Bearer xoxb-test-bot")
            parsed = urllib.parse.urlsplit(request.full_url)
            body = json.loads(cast(bytes, request.data).decode("utf-8"))
            calls.append((parsed.path, body))
            return FakeHTTPResponse('{"ok": true}')

        request = types.SimpleNamespace(
            subject=gestalt.Subject(id="system:http_binding:slack:events"),
            host=types.SimpleNamespace(public_base_url="https://gestalt.example.test/"),
        )
        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            response = provider_module.slack_events_handle(payload, cast(Any, request))

        self.assertEqual(response, {"ok": True, "unlinked": True})
        self.assertEqual(
            calls,
            [
                (
                    "/api/chat.postMessage",
                    {
                        "channel": "C789",
                        "text": (
                            "Your Slack account is not yet connected at "
                            "https://gestalt.example.test, please connect it "
                            "first before trying again."
                        ),
                        "thread_ts": "1712161829.000300",
                        "unfurl_links": False,
                        "unfurl_media": False,
                    },
                )
            ],
        )

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

        self.assertEqual(response, {"ok": True, "unlinked": True})

    def test_slack_event_handler_allows_configured_bot_route_system_subject(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
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
        workflow_manager = FakeWorkflowManager()
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
        request = gestalt.Request(
            subject=gestalt.Subject(id="system:http_binding:slack:events")
        )

        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch(
                "internals.client.urllib.request.urlopen",
                side_effect=AssertionError("unexpected Slack notification"),
            ),
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(response["ok"], True)
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
        workflow_request = workflow_manager.signal_or_start_requests[0]
        signal_metadata = json_format.MessageToDict(workflow_request.signal.metadata)
        self.assertEqual(
            signal_metadata["slack"]["agent_route_id"], "alert-bot-messages"
        )
        self.assertEqual(signal_metadata["slack"]["bot_id"], "B123")

    def test_slack_event_handler_still_notifies_unlinked_dm_route(self) -> None:
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
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(authorization_header(request), "Bearer xoxb-test-bot")
            parsed = urllib.parse.urlsplit(request.full_url)
            body = json.loads(cast(bytes, request.data).decode("utf-8"))
            calls.append((parsed.path, body))
            return FakeHTTPResponse('{"ok": true}')

        request = types.SimpleNamespace(
            subject=gestalt.Subject(id="system:http_binding:slack:events"),
            host=types.SimpleNamespace(public_base_url="https://gestalt.example.test/"),
        )
        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            response = provider_module.slack_events_handle(payload, cast(Any, request))

        self.assertEqual(response, {"ok": True, "unlinked": True})
        self.assertEqual(
            calls,
            [
                (
                    "/api/chat.postMessage",
                    {
                        "channel": "D789",
                        "text": (
                            "Your Slack account is not yet connected at "
                            "https://gestalt.example.test, please connect it "
                            "first before trying again."
                        ),
                        "thread_ts": "1712161829.000300",
                        "unfurl_links": False,
                        "unfurl_media": False,
                    },
                )
            ],
        )

    def test_slack_event_handler_acks_dict_workflow_response(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agentProvider": "simple",
                "agentModel": "deep",
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = DictWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvDict",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> ack this",
                "ts": "1712161829.000300",
            },
        }
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()

        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(
            response,
            {
                "ok": True,
                "workflow_provider": "local",
                "workflow_run_id": "run-123",
                "workflow_key": "slack:T123:C789:1712161829.000300",
                "workflow_signal_id": "signal-123",
                "started_run": True,
                "status": "WORKFLOW_RUN_STATUS_PENDING",
            },
        )
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)

    def test_slack_event_handler_requires_bot_token_before_signaling_workflow(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "agentProvider": "simple",
                "agentModel": "deep",
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
            gestalt.Request(
                subject=gestalt.Subject(id="user:gestalt-123", kind="user")
            ),
        )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.PRECONDITION_FAILED)
        self.assertEqual(response.body, {"error": "Slack bot token is not configured"})

    def test_slack_event_handler_sets_native_assistant_status_when_configured(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "assistant": {
                    "enabled": True,
                    "iconEmoji": ":hourglass_flowing_sand:",
                    "username": "Example Assistant",
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvAssistantStatus",
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
            return FakeHTTPResponse('{"ok": true}')

        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        self.assertNotIn("assistant_status_error", response)
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
        workflow_request = workflow_manager.signal_or_start_requests[0]
        agent_target = workflow_request.target.agent
        self.assertEqual(
            tool_ref_pairs(agent_target.tool_refs),
            BASE_EVENT_TOOL_REFS + ASSISTANT_EVENT_TOOL_REFS + WORKFLOW_EVENT_TOOL_REFS,
        )
        signal_payload = json_format.MessageToDict(workflow_request.signal.payload)
        self.assertNotIn("Native assistant status tool:", signal_payload["user_prompt"])
        self.assertNotIn(
            "slack.events.setAssistantStatus", agent_target.messages[0].text
        )
        self.assertEqual(
            calls,
            [
                (
                    "/api/assistant.threads.setStatus",
                    {
                        "channel_id": "C789",
                        "thread_ts": "1712161829.000300",
                        "status": "thinking...",
                        "icon_emoji": ":hourglass_flowing_sand:",
                        "username": "Example Assistant",
                    },
                )
            ],
        )

    def test_slack_event_handler_adds_acknowledgement_reaction_after_workflow(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "acknowledgement": {"reaction": ":eyes:"},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        sequence: list[tuple[str, str]] = []

        class RecordingWorkflowManager(FakeWorkflowManager):
            def signal_or_start_run(self, request: Any) -> Any:
                sequence.append(("workflow", "signal"))
                return super().signal_or_start_run(request)

        workflow_manager = RecordingWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvAckReaction",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> deploy?",
                "ts": "1712161829.000300",
            },
        }
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(authorization_header(request), "Bearer xoxb-test-bot")
            parsed = urllib.parse.urlsplit(request.full_url)
            body = json.loads(cast(bytes, request.data).decode("utf-8"))
            calls.append((parsed.path, body))
            sequence.append(("slack", parsed.path))
            return FakeHTTPResponse('{"ok": true}')

        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        self.assertNotIn("acknowledgement_reaction_error", response)
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
        self.assertEqual(
            sequence, [("workflow", "signal"), ("slack", "/api/reactions.add")]
        )
        self.assertEqual(
            calls,
            [
                (
                    "/api/reactions.add",
                    {
                        "channel": "C789",
                        "timestamp": "1712161829.000300",
                        "name": "eyes",
                    },
                )
            ],
        )

    def test_slack_event_handler_treats_existing_acknowledgement_reaction_as_idempotent(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "acknowledgment": {"reaction": "eyes"},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvAckReactionDuplicate",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> deploy?",
                "ts": "1712161829.000300",
            },
        }

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(authorization_header(request), "Bearer xoxb-test-bot")
            parsed = urllib.parse.urlsplit(request.full_url)
            self.assertEqual(parsed.path, "/api/reactions.add")
            return FakeHTTPResponse('{"ok": false, "error": "already_reacted"}')

        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        self.assertNotIn("acknowledgement_reaction_error", response)
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)

    def test_assistant_thread_started_sets_configured_suggested_prompts(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "assistant": {
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
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(authorization_header(request), "Bearer xoxb-test-bot")
            parsed = urllib.parse.urlsplit(request.full_url)
            calls.append(
                (parsed.path, json.loads(cast(bytes, request.data).decode("utf-8")))
            )
            return FakeHTTPResponse('{"ok": true}')

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(
            response,
            {
                "ok": True,
                "event_type": "assistant_thread_started",
                "channel": "D789",
                "thread_ts": "1712161829.000300",
                "suggested_prompts_set": True,
                "suggested_prompt_count": 1,
            },
        )
        self.assertEqual(
            calls,
            [
                (
                    "/api/assistant.threads.setSuggestedPrompts",
                    {
                        "channel_id": "D789",
                        "thread_ts": "1712161829.000300",
                        "prompts": [
                            {
                                "title": "Summarize deploys",
                                "message": "Summarize the latest deploy status",
                            }
                        ],
                        "title": "Try next",
                    },
                )
            ],
        )

    def test_assistant_thread_started_uses_route_specific_suggested_prompts(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
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
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            parsed = urllib.parse.urlsplit(request.full_url)
            calls.append(
                (parsed.path, json.loads(cast(bytes, request.data).decode("utf-8")))
            )
            return FakeHTTPResponse('{"ok": true}')

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        self.assertEqual(response["suggested_prompts_set"], True)
        self.assertEqual(
            calls,
            [
                (
                    "/api/assistant.threads.setSuggestedPrompts",
                    {
                        "channel_id": "D789",
                        "thread_ts": "1712161829.000300",
                        "prompts": [
                            {
                                "title": "Route prompt",
                                "message": "Use the route prompt",
                            }
                        ],
                        "title": "Route prompts",
                    },
                )
            ],
        )

    def test_assistant_thread_started_route_can_disable_inherited_prompts(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
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

        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(
            response,
            {
                "ok": True,
                "event_type": "assistant_thread_started",
                "suggested_prompts_set": False,
            },
        )
        urlopen.assert_not_called()

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
        idempotency_key = "workflow:local:run-123:output:signal-batch-abc"
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
                    reply_ref=reply_ref, text="Here is the answer"
                ),
                cast(
                    Any,
                    type(
                        "RequestWithIdempotencyKey",
                        (),
                        {
                            "subject": gestalt.Subject(
                                id="user:gestalt-123", kind="user"
                            ),
                            "idempotency_key": idempotency_key,
                        },
                    )(),
                ),
            )

        self.assertEqual(
            captured["payload"],
            {
                "channel": "C789",
                "text": "Here is the answer",
                "thread_ts": "1712161829.000300",
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
                reply_ref=reply_ref, text="wrong subject"
            ),
            gestalt.Request(subject=gestalt.Subject(id="user:other", kind="user")),
        )
        self.assertIsInstance(denied, gestalt.Response)
        denied_response = cast(gestalt.Response[dict[str, str]], denied)
        self.assertEqual(denied_response.status, HTTPStatus.FORBIDDEN)
        self.assertEqual(
            denied_response.body,
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
        idempotency_key = "workflow:local:run-123:session-ready:signal-batch-abc"
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
                cast(
                    Any,
                    type(
                        "RequestWithHostAndIdempotencyKey",
                        (),
                        {
                            "subject": gestalt.Subject(
                                id="user:gestalt-123", kind="user"
                            ),
                            "host": types.SimpleNamespace(
                                public_base_url="https://gestalt.example.test/"
                            ),
                            "idempotency_key": idempotency_key,
                        },
                    )(),
                ),
            )

        expected_url = "https://gestalt.example.test/agents?session=agent+session%2F123"
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
            gestalt.Request(
                subject=gestalt.Subject(id="user:gestalt-123", kind="user")
            ),
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

    def test_slack_interaction_request_posts_buttons_and_handler_signals_workflow(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
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
                        {
                            "id": "approve",
                            "label": "Approve",
                            "value": "approved",
                            "style": "primary",
                        }
                    ],
                ),
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
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

        workflow_manager = FakeWorkflowManager()
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
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
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_interactions_handle(
                {"payload": json.dumps(interaction_payload)},
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        self.assertEqual(response["workflow_run_id"], "run-123")
        self.assertEqual(response["workflow_key"], "slack:T123:C789:1712161829.000300")
        self.assertEqual(response["action_id"], "approve")
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
        workflow_request = workflow_manager.signal_or_start_requests[0]
        self.assertEqual(
            workflow_request.workflow_key, "slack:T123:C789:1712161829.000300"
        )
        self.assertEqual(workflow_request.signal.name, "slack.interaction")
        self.assertTrue(
            workflow_request.signal.idempotency_key.startswith("slack:interaction:")
        )
        signal_payload = json_format.MessageToDict(workflow_request.signal.payload)
        self.assertEqual(signal_payload["slack"]["action_id"], "approve")
        self.assertEqual(signal_payload["slack"]["action_value"], "approved")
        self.assertEqual(signal_payload["slack"]["trigger_id"], "1337.abcdef")
        self.assertIn("reply_ref: ", signal_payload["user_prompt"])
        self.assertNotIn("Final reply tool:", signal_payload["user_prompt"])
        self.assertNotIn("operation: slack.events.reply", signal_payload["user_prompt"])
        self.assertNotIn("Native assistant status tool:", signal_payload["user_prompt"])

        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "assistant": {"enabled": True},
            },
        )
        workflow_manager = FakeWorkflowManager()
        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_interactions_handle(
                {"payload": json.dumps(interaction_payload)},
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        workflow_request = workflow_manager.signal_or_start_requests[0]
        agent_target = workflow_request.target.agent
        self.assertIn(
            ("slack", "events.setAssistantStatus"),
            tool_ref_pairs(agent_target.tool_refs),
        )
        self.assertIn(
            ("slack", "events.clearAssistantStatus"),
            tool_ref_pairs(agent_target.tool_refs),
        )
        signal_payload = json_format.MessageToDict(workflow_request.signal.payload)
        self.assertNotIn("Native assistant status tool:", signal_payload["user_prompt"])

    def test_slack_interaction_ack_failure_still_acks_dispatched_workflow(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
        interaction_payload = signed_block_action_payload()

        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                provider_module._agent,
                "_workflow_signal_response_fields",
                side_effect=RuntimeError("bad response"),
            ),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_interactions_handle(
                {"payload": json.dumps(interaction_payload)},
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(
            response,
            {
                "ok": True,
                "workflow_dispatched": True,
                "workflow_acknowledgement_failed": True,
                "action_id": "approve",
            },
        )
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)

    def test_slack_interaction_signal_failure_returns_server_error(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        workflow_manager.signal_or_start_error = RuntimeError("signal failed")
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
        interaction_payload = signed_block_action_payload()

        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            result = provider_module.slack_interactions_handle(
                {"payload": json.dumps(interaction_payload)},
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(
            response.body,
            {"error": "failed to signal workflow run: signal failed"},
        )
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)

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
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )
            updated = provider_module.slack_events_set_status(
                provider_module.SlackEventStatusInput(
                    reply_ref=reply_ref,
                    text="Still working",
                    status_ts="1712161830.000400",
                ),
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )
            added = provider_module.slack_events_add_reaction(
                provider_module.SlackEventReactionInput(
                    reply_ref=reply_ref,
                    name="eyes",
                ),
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )
            removed = provider_module.slack_events_remove_reaction(
                provider_module.SlackEventReactionInput(
                    reply_ref=reply_ref,
                    name=":eyes:",
                    target_ts="1712161830.000400",
                ),
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )
            deleted = provider_module.slack_events_delete_status(
                provider_module.SlackEventDeleteStatusInput(
                    reply_ref=reply_ref,
                    status_ts="1712161830.000400",
                ),
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )
            assistant_status = provider_module.slack_events_set_assistant_status(
                provider_module.SlackEventAssistantStatusInput(
                    reply_ref=reply_ref,
                    status="is checking deployment status",
                    loading_messages=["Reading the thread", "Checking deploys"],
                    icon_emoji=":hourglass_flowing_sand:",
                    username="Example Assistant",
                ),
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )
            cleared_assistant_status = (
                provider_module.slack_events_clear_assistant_status(
                    provider_module.SlackEventReplyRefInput(reply_ref=reply_ref),
                    gestalt.Request(
                        subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                    ),
                )
            )
            title = provider_module.slack_events_set_thread_title(
                provider_module.SlackEventThreadTitleInput(
                    reply_ref=reply_ref,
                    title="Deploy status",
                ),
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
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
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
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
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )
            appended = provider_module.slack_events_append_stream(
                provider_module.SlackEventStreamAppendInput(
                    reply_ref=reply_ref,
                    stream_ts="1712161831.000500",
                    markdown_text="Still checking",
                ),
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
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
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
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

    def test_nested_agent_config_selects_route_by_channel(self) -> None:
        provider_module.configure(
            "supportSlackbot",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "systemPrompt": "Follow the global Slack policy.",
                    "modelOptions": {"temperature": 0},
                    "tools": [
                        {"plugin": "linear", "operation": "searchIssues"},
                    ],
                    "routes": [
                        {
                            "id": "triage",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventType": "message",
                            },
                            "agent": {
                                "prompt": "Triage support requests.",
                                "modelOptions": {"max_output_tokens": 2000},
                                "tools": [
                                    {
                                        "plugin": "statusPage",
                                        "operation": "status",
                                    },
                                ],
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
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
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()

        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(response["ok"], True)
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
        workflow_request = workflow_manager.signal_or_start_requests[0]
        agent_target = workflow_request.target.agent
        self.assertEqual(agent_target.provider_name, "simple")
        self.assertEqual(agent_target.model, "deep")
        self.assertEqual(
            tool_ref_pairs(agent_target.tool_refs),
            [
                ("linear", "searchIssues"),
                ("statusPage", "status"),
                ("supportSlackbot", "conversations.getThreadContext"),
                ("supportSlackbot", "conversations.getMessage"),
                ("supportSlackbot", "files.get"),
                ("supportSlackbot", "events.setStatus"),
                ("supportSlackbot", "events.deleteStatus"),
                ("supportSlackbot", "events.addReaction"),
                ("supportSlackbot", "events.removeReaction"),
                ("supportSlackbot", "interactions.request"),
            ],
        )
        self.assertEqual(
            agent_target.output_delivery.target.plugin_name,
            "supportSlackbot",
        )
        self.assertEqual(
            agent_target.output_delivery.target.operation,
            "events.reply",
        )
        self.assertEqual(agent_target.output_delivery.credential_mode, "none")
        self.assertNotIn("supportSlackbot.events.reply", agent_target.messages[0].text)
        self.assertNotIn(
            "supportSlackbot.events.startStream", agent_target.messages[0].text
        )
        self.assertIn(
            "supportSlackbot.conversations.getThreadContext",
            agent_target.messages[0].text,
        )
        self.assertIn(
            "supportSlackbot.interactions.request",
            agent_target.messages[0].text,
        )
        self.assertNotIn(
            "supportSlackbot.chat.postMessage", agent_target.messages[0].text
        )
        self.assertNotIn(
            "supportSlackbot.events.setAssistantStatus", agent_target.messages[0].text
        )
        self.assertIn("Follow the global Slack policy.", agent_target.messages[0].text)
        self.assertIn("Triage support requests.", agent_target.messages[0].text)

        signal_metadata = json_format.MessageToDict(workflow_request.signal.metadata)
        self.assertEqual(signal_metadata["slack"]["agent_route_id"], "triage")
        self.assertEqual(signal_metadata["slack"]["addressed_to_bot"], True)
        model_options = json_format.MessageToDict(agent_options(agent_target))
        self.assertEqual(model_options["temperature"], 0)
        self.assertEqual(model_options["max_output_tokens"], 2000)

    def test_agent_route_workflow_provider_handles_events_without_global_provider(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "timeoutSeconds": 1800,
                    "routes": [
                        {
                            "id": "route-local",
                            "match": {"channel": "C_ROUTE"},
                            "workflow": {"provider": "route-provider"},
                            "agent": {"timeoutSeconds": 900},
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
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
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()

        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        self.assertEqual(response["workflow_provider"], "route-provider")
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
        self.assertEqual(
            workflow_manager.signal_or_start_requests[0].provider_name,
            "route-provider",
        )
        self.assertEqual(
            workflow_manager.signal_or_start_requests[0].target.agent.timeout_seconds,
            900,
        )

    def test_agent_route_workflow_provider_handles_interactions_without_global_provider(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "routes": [
                        {
                            "id": "route-local",
                            "match": {"channel": "C_ROUTE"},
                            "workflow": {"provider": "route-provider"},
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        interaction_payload = signed_route_block_action_payload("route-local")
        workflow_manager = FakeWorkflowManager()
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()

        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_interactions_handle(
                {"payload": json.dumps(interaction_payload)},
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        self.assertEqual(response["workflow_provider"], "route-provider")
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
        self.assertEqual(
            workflow_manager.signal_or_start_requests[0].provider_name,
            "route-provider",
        )

    def test_signed_interaction_rejects_route_id_that_no_longer_resolves(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
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
                    "provider": "simple",
                    "model": "deep",
                    "routes": [{"id": "new-route", "match": {"channel": "C_ROUTE"}}],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()

        with mock.patch.object(
            gestalt.Request,
            "workflow_manager",
            return_value=workflow_manager,
            create=True,
        ):
            result = provider_module.slack_interactions_handle(
                {"payload": json.dumps(interaction_payload)},
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertEqual(
            response.body,
            {"error": "Slack interaction route is no longer configured"},
        )
        self.assertEqual(workflow_manager.signal_or_start_requests, [])

    def test_agent_route_assistant_overrides_global_assistant_enabled_state(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "assistant": {"enabled": False},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "routes": [
                        {
                            "id": "assistant-route",
                            "match": {"channel": "C_ROUTE"},
                            "agent": {
                                "assistant": {
                                    "enabled": True,
                                    "status": "checking route",
                                }
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvRouteAssistant",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C_ROUTE",
                "channel_type": "channel",
                "text": "<@UBOT> hello",
                "ts": "1712161829.000300",
            },
        }
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            parsed = urllib.parse.urlsplit(request.full_url)
            calls.append(
                (parsed.path, json.loads(cast(bytes, request.data).decode("utf-8")))
            )
            return FakeHTTPResponse('{"ok": true}')

        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        agent_target = workflow_manager.signal_or_start_requests[0].target.agent
        self.assertIn(
            ("slack", "events.setAssistantStatus"),
            tool_ref_pairs(agent_target.tool_refs),
        )
        self.assertEqual(
            calls,
            [
                (
                    "/api/assistant.threads.setStatus",
                    {
                        "channel_id": "C_ROUTE",
                        "thread_ts": "1712161829.000300",
                        "status": "checking route",
                    },
                )
            ],
        )

        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "assistant": {"enabled": True},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "routes": [
                        {
                            "id": "assistant-disabled-route",
                            "match": {"channel": "C_ROUTE"},
                            "agent": {"assistant": {"enabled": False}},
                        }
                    ],
                },
            },
        )
        workflow_manager = FakeWorkflowManager()
        calls.clear()
        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        agent_target = workflow_manager.signal_or_start_requests[0].target.agent
        self.assertNotIn(
            ("slack", "events.setAssistantStatus"),
            tool_ref_pairs(agent_target.tool_refs),
        )
        self.assertEqual(calls, [])

    def test_agent_route_acknowledgement_can_override_or_disable_global_ack(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "acknowledgement": {"reaction": "eyes"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "routes": [
                        {
                            "id": "ack-route",
                            "match": {"channel": "C_ROUTE"},
                            "agent": {
                                "acknowledgement": {
                                    "enabled": True,
                                    "reaction": "rocket",
                                }
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvRouteAck",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C_ROUTE",
                "channel_type": "channel",
                "text": "<@UBOT> hello",
                "ts": "1712161829.000300",
            },
        }
        calls: list[tuple[str, dict[str, Any]]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            parsed = urllib.parse.urlsplit(request.full_url)
            calls.append(
                (parsed.path, json.loads(cast(bytes, request.data).decode("utf-8")))
            )
            return FakeHTTPResponse('{"ok": true}')

        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        self.assertEqual(calls[0][0], "/api/reactions.add")
        self.assertEqual(calls[0][1]["name"], "rocket")
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)

        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "acknowledgement": {"reaction": "eyes"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "routes": [
                        {
                            "id": "ack-disabled-route",
                            "match": {"channel": "C_ROUTE"},
                            "agent": {"acknowledgement": {"enabled": False}},
                        }
                    ],
                },
            },
        )
        workflow_manager = FakeWorkflowManager()
        calls.clear()
        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        self.assertEqual(calls, [])
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)

    def test_agent_route_thread_context_prefetch_overrides_flags(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "threadContext": {"maxMessages": 200},
                    "routes": [
                        {
                            "id": "context-route",
                            "match": {"channel": "C_ROUTE"},
                            "agent": {
                                "threadContext": {
                                    "maxMessages": 25,
                                    "includeUserInfo": True,
                                    "includeBots": False,
                                    "includeFiles": False,
                                    "includeFileContent": True,
                                    "includeImageData": True,
                                    "maxFileBytes": 1024,
                                }
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvRouteThreadContext",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C_ROUTE",
                "channel_type": "channel",
                "text": "<@UBOT> summarize",
                "ts": "1712161835.000400",
                "thread_ts": "1712161829.000300",
            },
        }
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
        thread_context_result = {
            "data": {
                "channel": "C_ROUTE",
                "thread_ts": "1712161829.000300",
                "messages": [],
                "messages_returned": 0,
            }
        }

        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch(
                "internals.agent.get_thread_context",
                return_value=thread_context_result,
            ) as get_context,
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        get_context.assert_called_once_with(
            "xoxb-test-bot",
            channel="C_ROUTE",
            ts="1712161829.000300",
            cursor="",
            limit=25,
            include_user_info=True,
            include_bots=False,
            include_files=False,
            include_file_content=True,
            include_image_data=True,
            max_file_bytes=1024,
        )

    def test_agent_route_thread_context_can_disable_inherited_prefetch(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "threadContext": {"enabled": True, "maxMessages": 25},
                    "routes": [
                        {
                            "id": "context-disabled-route",
                            "match": {"channel": "C_ROUTE"},
                            "agent": {"threadContext": {"enabled": False}},
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvRouteThreadContextDisabled",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C_ROUTE",
                "channel_type": "channel",
                "text": "<@UBOT> summarize",
                "ts": "1712161835.000400",
                "thread_ts": "1712161829.000300",
            },
        }
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()

        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch("internals.agent.get_thread_context") as get_context,
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        get_context.assert_not_called()
        signal_payload = json_format.MessageToDict(
            workflow_manager.signal_or_start_requests[0].signal.payload
        )
        self.assertNotIn("thread_context", signal_payload["slack"])
        self.assertNotIn("thread_context_error", signal_payload["slack"])

    def test_agent_tool_sets_expand_before_tools_and_dedupe_first_ref(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "toolSets": {
                        "shared": [
                            {"plugin": "deployment", "operation": "status"},
                            {"plugin": "github", "operation": "search"},
                        ],
                        "route": [
                            {"plugin": "notion", "operation": "search"},
                            {"plugin": "pagerduty", "operation": "createIncident"},
                        ],
                    },
                    "toolSetRefs": ["shared"],
                    "tools": [
                        {"plugin": "github", "operation": "search"},
                        {"plugin": "notion", "operation": "search"},
                    ],
                    "routes": [
                        {
                            "id": "tool-route",
                            "match": {"channel": "C_ROUTE"},
                            "agent": {
                                "toolSetRefs": ["route"],
                                "tools": [
                                    {
                                        "plugin": "pagerduty",
                                        "operation": "createIncident",
                                    },
                                    {"plugin": "linear", "operation": "searchIssues"},
                                ],
                            },
                        }
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvRouteTools",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C_ROUTE",
                "channel_type": "channel",
                "text": "<@UBOT> route tools",
                "ts": "1712161829.000300",
            },
        }
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()

        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(response["ok"], True)
        agent_target = workflow_manager.signal_or_start_requests[0].target.agent
        self.assertEqual(
            tool_ref_pairs(agent_target.tool_refs),
            [
                ("deployment", "status"),
                ("github", "search"),
                ("notion", "search"),
                ("pagerduty", "createIncident"),
                ("linear", "searchIssues"),
                *BASE_EVENT_TOOL_REFS,
                *WORKFLOW_EVENT_TOOL_REFS,
            ],
        )

        with self.assertRaisesRegex(ValueError, "unknown tool set"):
            provider_module.configure(
                "slack",
                {
                    "agent": {
                        "toolSets": {"shared": []},
                        "routes": [
                            {
                                "id": "missing-tool-set",
                                "agent": {"toolSetRefs": ["missing"]},
                            }
                        ],
                    }
                },
            )

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
            gestalt.Request(
                subject=gestalt.Subject(id="user:gestalt-123", kind="user")
            ),
        )

        self.assertEqual(response, {"ok": True, "ignored": "unsupported_event_type"})

    def test_event_type_route_starts_plain_channel_message_agent(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "routes": [
                        {
                            "id": "support-all-messages",
                            "match": {
                                "channel": "C_SUPPORT",
                                "eventTypes": ["message.channels"],
                                "subtypes": [],
                            },
                            "agent": {"systemPrompt": "Triage support requests."},
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

        response, workflow_manager = self._handle_event_with_workflow(payload)

        self.assertEqual(response["ok"], True)
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
        workflow_request = workflow_manager.signal_or_start_requests[0]
        self.assertEqual(
            workflow_request.workflow_key,
            "slack:T123:C_SUPPORT:1712161829.000300",
        )
        signal_payload = json_format.MessageToDict(workflow_request.signal.payload)
        self.assertEqual(signal_payload["slack"]["addressed_to_bot"], False)
        self.assertEqual(signal_payload["slack"]["subtype"], "")
        self.assertEqual(
            signal_payload["slack"]["reply_thread_ts"], "1712161829.000300"
        )
        self.assertIn(
            "reply_thread_ts: 1712161829.000300", signal_payload["user_prompt"]
        )
        verified_ref = provider_module._verify_reply_ref(
            signal_payload["reply_ref"], "user:gestalt-123"
        )
        self.assertEqual(verified_ref.reply_thread_ts, "1712161829.000300")
        signal_metadata = json_format.MessageToDict(workflow_request.signal.metadata)
        self.assertEqual(
            signal_metadata["slack"]["agent_route_id"], "support-all-messages"
        )
        self.assertEqual(signal_metadata["slack"]["addressed_to_bot"], False)

    def test_event_type_route_keeps_plain_thread_reply_in_thread(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
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

        response, workflow_manager = self._handle_event_with_workflow(payload)

        self.assertEqual(response["ok"], True)
        workflow_request = workflow_manager.signal_or_start_requests[0]
        self.assertEqual(
            workflow_request.workflow_key,
            "slack:T123:C_SUPPORT:1712161829.000300",
        )
        self.assertEqual(
            workflow_request.idempotency_key,
            "slack:event:T123:C_SUPPORT:1712161835.000400:U456",
        )
        signal_payload = json_format.MessageToDict(workflow_request.signal.payload)
        self.assertEqual(
            signal_payload["slack"]["reply_thread_ts"], "1712161829.000300"
        )
        verified_ref = provider_module._verify_reply_ref(
            signal_payload["reply_ref"], "user:gestalt-123"
        )
        self.assertEqual(verified_ref.reply_thread_ts, "1712161829.000300")

    def test_event_type_route_thread_root_filters_channel_messages(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
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
                response, workflow_manager = self._handle_event_with_workflow(
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

                self.assertEqual(response["ok"], True)
                self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
                signal_metadata = json_format.MessageToDict(
                    workflow_manager.signal_or_start_requests[0].signal.metadata
                )
                self.assertEqual(
                    signal_metadata["slack"]["agent_route_id"], "channel-roots"
                )

        response, workflow_manager = self._handle_event_with_workflow(
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

        self.assertEqual(response, {"ok": True, "ignored": "no_matching_agent_route"})
        self.assertEqual(workflow_manager.signal_or_start_requests, [])

    def test_event_type_route_thread_reply_filters_channel_messages(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
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

        response, workflow_manager = self._handle_event_with_workflow(
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

        self.assertEqual(response, {"ok": True, "ignored": "no_matching_agent_route"})
        self.assertEqual(workflow_manager.signal_or_start_requests, [])

        response, workflow_manager = self._handle_event_with_workflow(
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

        self.assertEqual(response["ok"], True)
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
        workflow_request = workflow_manager.signal_or_start_requests[0]
        self.assertEqual(
            workflow_request.workflow_key,
            "slack:T123:C_SUPPORT:1712161829.000300",
        )
        signal_metadata = json_format.MessageToDict(workflow_request.signal.metadata)
        self.assertEqual(signal_metadata["slack"]["agent_route_id"], "channel-replies")

    def test_app_mention_routes_respect_thread_filter(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
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
                response, workflow_manager = self._handle_event_with_workflow(
                    {
                        "type": "event_callback",
                        "event_id": f"Ev{route_id}",
                        "team_id": "T123",
                        "event": event,
                    }
                )

                self.assertEqual(response["ok"], True)
                self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
                signal_metadata = json_format.MessageToDict(
                    workflow_manager.signal_or_start_requests[0].signal.metadata
                )
                self.assertEqual(signal_metadata["slack"]["agent_route_id"], route_id)

    def test_event_type_route_ordering_skips_legacy_message_route(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "routes": [
                        {
                            "id": "legacy-message-route",
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

        response, workflow_manager = self._handle_event_with_workflow(payload)

        self.assertEqual(response["ok"], True)
        signal_metadata = json_format.MessageToDict(
            workflow_manager.signal_or_start_requests[0].signal.metadata
        )
        self.assertEqual(
            signal_metadata["slack"]["agent_route_id"], "explicit-slack-route"
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
                            "provider": "simple",
                            "model": "deep",
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

                response, workflow_manager = self._handle_event_with_workflow(payload)

                self.assertEqual(response["ok"], True)
                self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
                signal_metadata = json_format.MessageToDict(
                    workflow_manager.signal_or_start_requests[0].signal.metadata
                )
                self.assertEqual(
                    signal_metadata["slack"]["agent_route_id"],
                    f"route-{event_type}",
                )
                if event_type == "message.app_home":
                    workflow_request = workflow_manager.signal_or_start_requests[0]
                    self.assertEqual(workflow_request.workflow_key, "slack:T123:D_HOME")
                    signal_payload = json_format.MessageToDict(
                        workflow_request.signal.payload
                    )
                    self.assertEqual(signal_payload["slack"]["reply_thread_ts"], "")
                    self.assertEqual(signal_payload["slack"]["addressed_to_bot"], True)

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

                response = provider_module.slack_events_handle(
                    payload,
                    gestalt.Request(
                        subject=gestalt.Subject(
                            id="user:gestalt-123",
                            kind="user",
                        )
                    ),
                )

                self.assertEqual(response["ok"], True)
                self.assertEqual(response["event_type"], event_type)

    def test_event_type_routes_filter_message_subtypes(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
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
            gestalt.Request(
                subject=gestalt.Subject(id="user:gestalt-123", kind="user")
            ),
        )

        self.assertEqual(response, {"ok": True, "ignored": "no_matching_agent_route"})

        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
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

        response, workflow_manager = self._handle_event_with_workflow(
            {
                **file_share_payload,
                "event_id": "EvFileShareMatched",
            }
        )

        self.assertEqual(response["ok"], True)
        signal_metadata = json_format.MessageToDict(
            workflow_manager.signal_or_start_requests[0].signal.metadata
        )
        self.assertEqual(signal_metadata["slack"]["agent_route_id"], "file-shares")
        self.assertEqual(signal_metadata["slack"]["subtype"], "file_share")

    def test_event_type_route_keeps_ignored_message_events_ignored(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
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

                response, workflow_manager = self._handle_event_with_workflow(payload)

                self.assertEqual(response, {"ok": True, "ignored": "ignored_event"})
                self.assertEqual(workflow_manager.signal_or_start_requests, [])

    def test_event_type_route_ignores_bot_message_without_bot_match(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
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

        response, workflow_manager = self._handle_event_with_workflow(
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

        self.assertEqual(response, {"ok": True, "ignored": "no_matching_agent_route"})
        self.assertEqual(workflow_manager.signal_or_start_requests, [])

    def test_event_type_route_can_match_configured_bot_message(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
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

        response, workflow_manager = self._handle_event_with_workflow(
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

        self.assertEqual(response["ok"], True)
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)
        workflow_request = workflow_manager.signal_or_start_requests[0]
        signal_payload = json_format.MessageToDict(workflow_request.signal.payload)
        signal_metadata = json_format.MessageToDict(workflow_request.signal.metadata)
        self.assertEqual(
            signal_metadata["slack"]["agent_route_id"], "alert-bot-messages"
        )
        self.assertEqual(signal_metadata["slack"]["bot_id"], "B123")
        self.assertEqual(signal_metadata["slack"]["is_bot_event"], True)
        self.assertIn(
            "Datadog alert: high error rate",
            signal_payload["agent_request"]["current_message"]["text"],
        )
        self.assertEqual(
            signal_payload["agent_request"]["current_message"]["bot_id"], "B123"
        )
        self.assertEqual(
            signal_payload["agent_request"]["current_message"]["is_bot_event"], True
        )

    def test_event_type_route_rejects_unmatched_bot_id(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
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

        response, workflow_manager = self._handle_event_with_workflow(
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

        self.assertEqual(response, {"ok": True, "ignored": "no_matching_agent_route"})
        self.assertEqual(workflow_manager.signal_or_start_requests, [])

    def test_repeated_slack_events_reuse_session_key_but_keep_event_metadata_on_turns(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "threadContext": {"enabled": False},
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
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

        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()
        with (
            mock.patch(f"{__name__}.workflow_pb2", workflow_pb2_contract),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            provider_module.slack_events_handle(first, request)
            provider_module.slack_events_handle(second, request)

        self.assertEqual(len(workflow_manager.signal_or_start_requests), 2)
        requests = workflow_manager.signal_or_start_requests
        self.assertEqual(
            requests[0].workflow_key,
            requests[1].workflow_key,
        )
        self.assertEqual(requests[0].workflow_key, "slack:T123:C789:1712161829.000300")
        for workflow_request in requests:
            target_metadata = json_format.MessageToDict(
                workflow_request.target.agent.metadata
            )
            self.assertEqual(
                target_metadata["slack"]["root_message_ts"], "1712161829.000300"
            )
            self.assertNotIn("event_id", target_metadata["slack"])
            self.assertNotIn("user_id", target_metadata["slack"])

        self.assertEqual(
            requests[0].idempotency_key,
            "slack:event:T123:C789:1712161829.000300:U456",
        )
        self.assertEqual(
            requests[1].idempotency_key,
            "slack:event:T123:C789:1712161835.000400:U999",
        )
        first_metadata = json_format.MessageToDict(requests[0].signal.metadata)
        second_metadata = json_format.MessageToDict(requests[1].signal.metadata)
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
                            "agent": {"systemPrompt": "Triage support requests."},
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

        response = provider_module.slack_events_handle(
            payload,
            gestalt.Request(
                subject=gestalt.Subject(id="user:gestalt-123", kind="user")
            ),
        )

        self.assertEqual(response, {"ok": True, "ignored": "no_matching_agent_route"})

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

    def test_url_verification_returns_challenge_without_workflow_run(self) -> None:
        payload = {"type": "url_verification", "challenge": "challenge-token"}

        response = provider_module.slack_events_handle(payload, gestalt.Request())

        self.assertEqual(response, {"challenge": "challenge-token"})

    def test_publish_route_publishes_exact_workflow_event(self) -> None:
        provider_module.configure(
            "slack",
            {
                "events": {
                    "publish": {
                        "routes": [
                            {
                                "id": "deployments",
                                "workflowProvider": "local",
                                "workflowEventType": "deployment.slack_event",
                                "source": "slack/events",
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
        workflow_manager = FakeWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvPublish",
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
                "WorkflowManagerPublishEventRequest",
                FakeWorkflowManagerPublishEventRequest,
            ),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, gestalt.Request())

        self.assertEqual(
            response,
            {
                "ok": True,
                "published": True,
                "published_event_count": 1,
                "workflow_event_ids": ["slack:EvPublish"],
                "route_ids": ["deployments"],
            },
        )
        self.assertEqual(len(workflow_manager.publish_event_requests), 1)
        request = workflow_manager.publish_event_requests[0]
        self.assertEqual(request.provider_name, "local")
        event = request.event
        self.assertEqual(event.id, "slack:EvPublish")
        self.assertEqual(event.type, "deployment.slack_event")
        self.assertEqual(event.source, "slack/events")
        self.assertEqual(event.subject, "deployments")
        self.assertEqual(event.spec_version, "1.0")
        self.assertEqual(event.datacontenttype, "application/json")
        self.assertEqual(
            json_format.MessageToDict(event.data),
            {
                "routeId": "deployments",
                "slack": {
                    "callback_type": "event_callback",
                    "event_type": "message",
                    "event_id": "EvPublish",
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

    def test_publish_route_ack_survives_agent_signal_failure(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "events": {
                    "publish": {
                        "routes": [
                            {
                                "id": "brain-ingest",
                                "workflowProvider": "local",
                                "workflowEventType": "slack.event.received",
                                "source": "slack",
                                "subject": "route:brain-ingest",
                                "match": {"eventTypes": ["message"]},
                            }
                        ]
                    }
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        workflow_manager.signal_or_start_error = RuntimeError("signal failed")
        payload = {
            "type": "event_callback",
            "event_id": "EvPublishAndSignal",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C789",
                "channel_type": "im",
                "text": "publish and signal",
                "ts": "1712161829.000300",
            },
        }
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )

        with (
            mock.patch.object(
                provider_module._agent.gestalt,
                "WorkflowManagerPublishEventRequest",
                FakeWorkflowManagerPublishEventRequest,
            ),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(
            response,
            {
                "ok": True,
                "published": True,
                "published_event_count": 1,
                "workflow_event_ids": ["slack:EvPublishAndSignal"],
                "route_ids": ["brain-ingest"],
            },
        )
        self.assertEqual(len(workflow_manager.publish_event_requests), 1)
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)

    def test_publish_route_ack_uses_request_id_after_publish_succeeds(self) -> None:
        provider_module.configure(
            "slack",
            {
                "events": {
                    "publish": {
                        "routes": [
                            {
                                "id": "brain-ingest",
                                "workflowProvider": "local",
                                "workflowEventType": "slack.event.received",
                                "source": "slack",
                                "subject": "route:brain-ingest",
                                "match": {"eventTypes": ["message"]},
                            }
                        ]
                    }
                }
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = ExplodingPublishResponseWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvPublishBadResponse",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C789",
                "channel_type": "im",
                "text": "publish response should not shape ack",
                "ts": "1712161829.000300",
            },
        }

        with (
            mock.patch.object(
                provider_module._agent.gestalt,
                "WorkflowManagerPublishEventRequest",
                FakeWorkflowManagerPublishEventRequest,
            ),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, gestalt.Request())

        self.assertEqual(
            response,
            {
                "ok": True,
                "published": True,
                "published_event_count": 1,
                "workflow_event_ids": ["slack:EvPublishBadResponse"],
                "route_ids": ["brain-ingest"],
            },
        )
        self.assertEqual(len(workflow_manager.publish_event_requests), 1)

    def test_publish_route_ack_survives_workflow_ack_failure(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "events": {
                    "publish": {
                        "routes": [
                            {
                                "id": "brain-ingest",
                                "workflowProvider": "local",
                                "workflowEventType": "slack.event.received",
                                "source": "slack",
                                "subject": "route:brain-ingest",
                                "match": {"eventTypes": ["message"]},
                            }
                        ]
                    }
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_manager = FakeWorkflowManager()
        payload = {
            "type": "event_callback",
            "event_id": "EvBadAck",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C789",
                "channel_type": "im",
                "text": "publish then bad ack",
                "ts": "1712161829.000300",
            },
        }
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )

        with (
            mock.patch.object(
                provider_module._agent.gestalt,
                "WorkflowManagerPublishEventRequest",
                FakeWorkflowManagerPublishEventRequest,
            ),
            mock.patch.object(
                provider_module._agent,
                "_workflow_signal_response_fields",
                side_effect=RuntimeError("bad response"),
            ),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            response = provider_module.slack_events_handle(payload, request)

        self.assertEqual(
            response,
            {
                "ok": True,
                "published": True,
                "published_event_count": 1,
                "workflow_event_ids": ["slack:EvBadAck"],
                "route_ids": ["brain-ingest"],
            },
        )
        self.assertEqual(len(workflow_manager.publish_event_requests), 1)
        self.assertEqual(len(workflow_manager.signal_or_start_requests), 1)

    def test_publish_only_callback_without_linked_subject_passes_resolution(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "events": {
                    "publish": {
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
                "text": "<@UBOT> publish only",
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

        self.assertIsNone(resolved)
        self.assertEqual(len(authorization.requests), 1)

    def test_publish_routes_match_bot_include_and_subtype_filters(self) -> None:
        provider_module.configure(
            "slack",
            {
                "events": {
                    "publish": {
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
        workflow_manager = FakeWorkflowManager()
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
                "WorkflowManagerPublishEventRequest",
                FakeWorkflowManagerPublishEventRequest,
            ),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
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
        self.assertEqual(len(workflow_manager.publish_event_requests), 2)
        bot_event = workflow_manager.publish_event_requests[0].event
        self.assertEqual(
            bot_event.id,
            "slack:route:bot-messages:team:T123:event:message:subtype:"
            "bot_message:channel:C_BOT:ts:1712161829.000300:thread:-:actor:B123",
        )
        self.assertEqual(bot_event.type, "slack.event.received")
        self.assertEqual(bot_event.source, "slack")
        self.assertEqual(bot_event.subject, "route:bot-messages")
        bot_data = json_format.MessageToDict(bot_event.data)
        self.assertEqual(bot_data["routeId"], "bot-messages")
        self.assertEqual(bot_data["slack"]["bot_id"], "B123")
        self.assertEqual(bot_data["slack"]["subtype"], "bot_message")
        self.assertEqual(bot_data["slack"]["text"], "")
        self.assertTrue(bot_data["slack"]["is_bot_event"])

    def test_publish_failure_returns_non_2xx(self) -> None:
        provider_module.configure(
            "slack",
            {
                "events": {
                    "publish": {
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
        workflow_manager = FakeWorkflowManager()
        workflow_manager.publish_event_error = RuntimeError("boom")
        payload = {
            "type": "event_callback",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C789",
                "text": "publish me",
                "ts": "1712161829.000300",
            },
        }
        with (
            mock.patch.object(
                provider_module._agent.gestalt,
                "WorkflowManagerPublishEventRequest",
                FakeWorkflowManagerPublishEventRequest,
            ),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            result = provider_module.slack_events_handle(payload, gestalt.Request())

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(
            response.body, {"error": "failed to publish workflow event: boom"}
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
