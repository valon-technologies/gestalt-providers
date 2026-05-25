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
from typing import Any, cast
from unittest import mock

import gestalt
import yaml

from internals.agent_links import agent_session_url
import provider as provider_module

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
WORKFLOW_EVENT_TOOL_REFS: list[tuple[str, str]] = []


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


def new_struct() -> dict[str, Any]:
    return {}


def authorization_subject(
    *, type: str, id: str, properties: dict[str, Any] | None = None
) -> types.SimpleNamespace:
    return types.SimpleNamespace(type=type, id=id, properties=properties or {})


class FakeWorkflowPublishEvent:
    def __init__(
        self,
        event: Any = None,
        provider_name: str = "",
        **_kwargs: Any,
    ) -> None:
        self.event = event
        self.provider_name = provider_name


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


def request_json(request: urllib.request.Request) -> dict[str, Any]:
    return json.loads(cast(bytes, request.data).decode("utf-8"))


def tool_ref_pairs(refs: Any) -> list[tuple[str, str]]:
    return [
        (str(getattr(ref, "system", "") or ref.app), str(ref.operation)) for ref in refs
    ]


def tool_ref_details(refs: Any) -> list[tuple[str, str, str, str, str, str]]:
    return [
        (
            str(ref.app),
            str(ref.operation),
            str(getattr(ref, "connection", "")),
            str(getattr(ref, "instance", "")),
            str(getattr(ref, "title", "")),
            str(getattr(ref, "description", "")),
        )
        for ref in refs
    ]


def workflow_text(value: Any) -> str:
    template = getattr(value, "template", None)
    if template is not None:
        return str(template)
    return str(value)


class WorkflowMessageView:
    def __init__(self, message: Any) -> None:
        self.role = getattr(message, "role", "")
        self.text = workflow_text(getattr(message, "text", ""))


class WorkflowAppDeliveryView:
    def __init__(self, app_step: Any) -> None:
        app = getattr(app_step, "app", None)
        self.target = types.SimpleNamespace(
            app_name=getattr(app, "name", ""),
            operation=getattr(app, "operation", ""),
        )
        self.credential_mode = getattr(app, "credential_mode", "")
        input_value = getattr(app, "input", None)
        fields = getattr(input_value, "object", None)
        if fields is None and isinstance(input_value, dict):
            fields = input_value
        self.input_bindings = [
            types.SimpleNamespace(input_field=key, value=value)
            for key, value in (fields or {}).items()
        ]


class WorkflowWhenView:
    def __init__(self, when: Any) -> None:
        self.equals = getattr(when, "equals", None)
        value = getattr(when, "value", None)
        step_output = getattr(value, "step_output", None)
        self.step_id = getattr(when, "step_id", "") or getattr(
            step_output, "step_id", ""
        )
        output_path = getattr(when, "output_path", "") or getattr(
            step_output, "path", ""
        )
        output_path = str(output_path)
        if output_path.startswith("agent.structuredOutput."):
            output_path = output_path.replace(
                "agent.structuredOutput.", "structured_output.", 1
            )
        self.output_path = output_path


def workflow_when_view(when: Any) -> WorkflowWhenView | None:
    if when is None:
        return None
    return WorkflowWhenView(when)


class WorkflowAgentView:
    def __init__(
        self,
        step: Any,
        agent: Any,
        all_steps: list[Any],
        *,
        include_steps: bool = False,
        compute_deliveries: bool = True,
    ) -> None:
        self.provider_name = getattr(agent, "provider_name", "") or getattr(
            agent, "provider", ""
        )
        self.id = getattr(step, "id", "")
        self.model = getattr(agent, "model", "")
        self.prompt = workflow_text(getattr(agent, "prompt", ""))
        self.messages = [
            WorkflowMessageView(message)
            for message in (getattr(agent, "messages", None) or [])
        ]
        self.tool_refs = (
            getattr(agent, "tool_refs", None) or getattr(agent, "tools", None) or []
        )
        self.response_schema: Any = getattr(agent, "response_schema", None)
        self.when: Any = workflow_when_view(getattr(step, "when", None))
        self.timeout_seconds = getattr(step, "timeout_seconds", 0) or getattr(
            agent, "timeout_seconds", 0
        )
        self.metadata = (
            getattr(step, "metadata", None)
            or getattr(agent, "metadata", None)
            or new_struct()
        )
        self.model_options = (
            getattr(agent, "model_options", None)
            or getattr(agent, "provider_options", None)
            or new_struct()
        )
        self.provider_options = self.model_options
        self.reply_delivery: Any = (
            workflow_app_delivery_for_agent(
                all_steps, getattr(step, "id", ""), "events.reply"
            )
            if compute_deliveries
            else None
        )
        self.session_ready_reply_delivery: Any = (
            workflow_app_delivery_for_agent(
                all_steps, getattr(step, "id", ""), "events.replySessionStarted"
            )
            if compute_deliveries
            else None
        )
        self.steps = workflow_agent_steps(all_steps) if include_steps else []


def workflow_target_agent(target: Any) -> WorkflowAgentView:
    steps = list(getattr(target, "steps", None) or [])
    agent_steps = [step for step in steps if getattr(step, "agent", None) is not None]
    if len(agent_steps) > 1:
        return WorkflowAgentView(
            types.SimpleNamespace(id="", timeout_seconds=0, metadata=None),
            types.SimpleNamespace(prompt="", messages=[], tools=[]),
            steps,
            include_steps=True,
            compute_deliveries=False,
        )
    for step in agent_steps:
        return WorkflowAgentView(
            step, getattr(step, "agent"), steps, include_steps=True
        )
    raise AssertionError("workflow target has no agent step")


def workflow_agent_steps(steps: list[Any]) -> list[WorkflowAgentView]:
    out: list[WorkflowAgentView] = []
    for step in steps:
        agent = getattr(step, "agent", None)
        if agent is not None:
            out.append(WorkflowAgentView(step, agent, steps))
    return out


def workflow_app_delivery_for_agent(
    steps: list[Any],
    agent_step_id: str,
    operation: str,
) -> WorkflowAppDeliveryView | None:
    for step in steps:
        app = getattr(step, "app", None)
        if app is None or getattr(app, "operation", "") != operation:
            continue
        if workflow_app_step_uses_agent_output(step, agent_step_id):
            return WorkflowAppDeliveryView(step)
    return None


def workflow_app_step_uses_agent_output(step: Any, agent_step_id: str) -> bool:
    app = getattr(step, "app", None)
    input_value = getattr(app, "input", None)
    fields = getattr(input_value, "object", None)
    if fields is None and isinstance(input_value, dict):
        fields = input_value
    for value in (fields or {}).values():
        step_output = getattr(value, "step_output", None)
        if getattr(step_output, "step_id", "") == agent_step_id:
            return True
    return False


def workflow_binding_value(value: Any) -> tuple[str | None, Any]:
    kind = value.WhichOneof("kind") if hasattr(value, "WhichOneof") else None
    if kind is not None:
        return kind, getattr(value, kind, None)
    step_output = getattr(value, "step_output", None)
    if step_output is not None:
        path = str(getattr(step_output, "path", ""))
        if path == "agent.text":
            return "agent_output", "text"
        if path == "agent.sessionId":
            return "agent_session", "id"
        structured_prefix = "agent.structuredOutput."
        if path.startswith(structured_prefix):
            return "agent_output", path.removeprefix(structured_prefix)
        return "step_output", {
            "step_id": getattr(step_output, "step_id", ""),
            "path": path,
        }
    for candidate in (
        "agent_output",
        "signal_payload",
        "signal_metadata",
        "agent_session",
        "literal",
    ):
        candidate_value = getattr(value, candidate, None)
        if candidate_value not in (None, "") and not (
            candidate == "literal" and type(candidate_value) is object
        ):
            return candidate, candidate_value
    return None, None


def app_delivery_bindings(delivery: Any) -> dict[str, tuple[str | None, Any]]:
    out: dict[str, tuple[str | None, Any]] = {}
    for binding in delivery.input_bindings:
        out[str(binding.input_field)] = workflow_binding_value(binding.value)
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
        self.subjects = [_native_subject(subject) for subject in subjects]
        self.requests: list[Any] = []

    def search_subjects(self, request: Any) -> Any:
        self.requests.append(request)
        if isinstance(request, dict):
            subject_type = str(request.get("subject_type", "") or "").strip()
        else:
            subject_type = str(getattr(request, "subject_type", "") or "").strip()
        subjects = [
            subject
            for subject in self.subjects
            if not subject_type or str(subject.type or "").strip() == subject_type
        ]
        return types.SimpleNamespace(subjects=subjects)


def _native_subject(subject: Any) -> Any:
    properties = getattr(subject, "properties", None)
    if properties is None:
        native_properties = None
    elif isinstance(properties, dict):
        native_properties = properties
    else:
        native_properties = sdk_value_to_dict(properties)
    return types.SimpleNamespace(
        type=str(getattr(subject, "type", "") or ""),
        id=str(getattr(subject, "id", "") or ""),
        properties=native_properties,
    )


class FakeWorkflowClient:
    def __init__(self) -> None:
        self.signal_or_start_requests: list[Any] = []
        self.publish_event_requests: list[Any] = []
        self.signal_or_start_error: Exception | None = None
        self.publish_event_error: Exception | None = None

    def __enter__(self) -> FakeWorkflowClient:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def signal_or_start_run(self, request: Any) -> Any:
        self.signal_or_start_requests.append(request)
        if self.signal_or_start_error is not None:
            raise self.signal_or_start_error
        signal = request.signal or gestalt.WorkflowSignal()
        return gestalt.WorkflowRunSignal(
            provider_name=request.provider_name or "local",
            run=gestalt.BoundWorkflowRun(
                id="run-123",
                status=gestalt.WORKFLOW_RUN_STATUS_PENDING,
                workflow_key=request.workflow_key,
            ),
            signal=gestalt.WorkflowSignal(
                id="signal-123",
                name=signal.name,
                payload=signal.payload,
                metadata=signal.metadata,
                idempotency_key=signal.idempotency_key,
            ),
            started_run=True,
            workflow_key=request.workflow_key,
        )

    def publish_event(self, request: Any) -> Any:
        self.publish_event_requests.append(request)
        if self.publish_event_error is not None:
            raise self.publish_event_error
        return request.event


class ExplodingPublishResponseWorkflowClient(FakeWorkflowClient):
    def publish_event(self, request: Any) -> Any:
        self.publish_event_requests.append(request)

        class Response:
            @property
            def id(self) -> str:
                raise RuntimeError("bad publish response")

        return Response()


class DictWorkflowClient(FakeWorkflowClient):
    def signal_or_start_run(self, request: Any) -> Any:
        self.signal_or_start_requests.append(request)
        return {
            "providerName": request.provider_name or "local",
            "run": {
                "id": "run-123",
                "status": 1,
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

    def _slack_bot_service_account_request(
        self, token: str = "xoxb-resolved-bot", agent_slack_user_id: str = ""
    ) -> gestalt.Request:
        kwargs: dict[str, Any] = {}
        if agent_slack_user_id:
            kwargs["agent_external_identity"] = gestalt.ExternalIdentity(
                type="slack_identity",
                id=f"team:T123:user:{agent_slack_user_id}",
            )
        return gestalt.Request(
            token=token,
            credential=gestalt.Credential(mode="user", connection="bot"),
            subject=gestalt.Subject(
                id="service_account:slack-bot", kind="service_account"
            ),
            **kwargs,
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
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )
        return response, workflow_client

    def test_agent_module_reexports_model_interfaces(self) -> None:
        import internals.agent as agent_module
        import internals.models as models_module

        for name in (
            "SlackAcknowledgementConfig",
            "SlackAgentConfig",
            "SlackAgentEvent",
            "SlackAgentRoute",
            "SlackAgentRouteMatch",
            "SlackAgentStep",
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
            {"app": "*", "operation": ""},
            {"app": "linear", "operation": ""},
            {"app": "system", "operation": "shell"},
            {"system": "shell", "operation": "run"},
            {"system": "workflow", "operation": ""},
            {"system": "workflow", "operation": "schedules.create", "app": "linear"},
            {"app": "linear", "operation": "searchIssues", "credentialMode": "none"},
            {"app": "linear", "operation": "searchIssues", "runAs": "user"},
            {"app": "linear", "operation": "searchIssues", "runAs": {}},
            {
                "app": "linear",
                "operation": "searchIssues",
                "runAs": {
                    "subject": {"id": "service_account:linear"},
                    "externalIdentity": {"type": "linear_workspace"},
                },
            },
            {
                "system": "workflow",
                "operation": "schedules.create",
                "runAs": {
                    "subject": {"id": "service_account:workflow-tools"},
                },
            },
            {
                "app": "linear",
                "operation": "searchIssues",
                "inputBindings": [],
            },
            {"app": "linear", "operation": "searchIssues", "system": True},
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

    def test_agent_routes_require_service_account_run_as_subjects(self) -> None:
        invalid_run_as = [
            {"subject": {"id": "system:http_binding:slack:events"}},
            {"subject": {"id": "user:gestalt-123"}},
            {"subject": {"id": "service_account:"}},
            {"subject": {"id": "automation-without-kind"}},
            {
                "subject": {
                    "id": "automation-with-kind",
                    "kind": "service_account",
                }
            },
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

                response, workflow_client = self._handle_event_with_workflow(payload)

                self.assertEqual(operation_body(response)["ok"], True)
                self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
                signal_metadata = sdk_value_to_dict(
                    workflow_client.signal_or_start_requests[0].signal.metadata
                )
                self.assertEqual(
                    signal_metadata["slack"]["agent_route_id"],
                    "all-channel-messages",
                )

    def test_agent_timeout_seconds_maps_to_workflow_target(self) -> None:
        def target_for_config(config: dict[str, Any], *, channel: str = "C789") -> Any:
            provider_module.configure("slack", config)
            self.addCleanup(provider_module.configure, "slack", {})
            workflow_client = FakeWorkflowClient()
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
                    gestalt.Request(
                        subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                    ),
                )
            self.assertEqual(operation_body(response)["ok"], True)
            return workflow_target_agent(
                workflow_client.signal_or_start_requests[0].target
            )

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

    def test_route_agent_steps_map_to_workflow_target_steps(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "toolSets": {
                        "triage": [{"app": "linear", "operation": "searchIssues"}],
                    },
                    "routes": [
                        {
                            "id": "stepped",
                            "match": {"channel": "C_STEPS"},
                            "agent": {
                                "tools": [{"app": "jira", "operation": "search"}],
                                "steps": [
                                    {
                                        "id": "collect",
                                        "prompt": "Collect Slack context.",
                                        "messages": [
                                            {
                                                "role": "system",
                                                "text": "Use the event signal.",
                                            },
                                            {
                                                "role": "user",
                                                "text": "Summarize the request.",
                                            },
                                        ],
                                        "toolSetRefs": ["triage"],
                                        "tools": [
                                            {
                                                "app": "github",
                                                "operation": "pulls/list",
                                            }
                                        ],
                                        "responseSchema": {
                                            "type": "object",
                                            "properties": {
                                                "summary": {"type": "string"}
                                            },
                                        },
                                        "modelOptions": {"temperature": 0},
                                        "metadata": {"phase": "collect"},
                                        "timeoutSeconds": 120,
                                    },
                                    {
                                        "id": "reply",
                                        "prompt": "Write the Slack reply.",
                                        "when": {
                                            "stepId": "collect",
                                            "outputPath": "structured_output.actionable",
                                            "equals": True,
                                        },
                                        "slackReply": {
                                            "agentOutput": "answer.markdown"
                                        },
                                    },
                                ],
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
            "event_id": "EvRouteSteps",
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
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        agent_target = workflow_target_agent(
            workflow_client.signal_or_start_requests[0].target
        )
        self.assertEqual(agent_target.prompt, "")
        self.assertEqual(list(agent_target.tool_refs or []), [])
        self.assertIsNone(agent_target.reply_delivery)
        self.assertEqual(len(agent_target.steps), 2)
        collect = agent_target.steps[0]
        self.assertEqual(collect.id, "collect")
        self.assertEqual(collect.prompt, "Collect Slack context.")
        self.assertEqual(
            [message.role for message in collect.messages], ["system", "system", "user"]
        )
        collect_tool_pairs = tool_ref_pairs(collect.tool_refs)
        self.assertEqual(
            collect_tool_pairs[:3],
            [("jira", "search"), ("linear", "searchIssues"), ("github", "pulls/list")],
        )
        self.assertEqual(collect_tool_pairs[3:], BASE_EVENT_TOOL_REFS)
        self.assertEqual(collect.response_schema["type"], "object")
        self.assertEqual(collect.model_options["temperature"], 0)
        self.assertEqual(collect.metadata["phase"], "collect")
        self.assertEqual(collect.timeout_seconds, 120)
        self.assertIsNone(collect.when)

        reply = agent_target.steps[1]
        self.assertEqual(reply.id, "reply")
        self.assertEqual(reply.when.step_id, "collect")
        self.assertEqual(reply.when.output_path, "structured_output.actionable")
        self.assertEqual(reply.when.equals, True)
        self.assertEqual(
            app_delivery_bindings(reply.reply_delivery),
            {
                "text": ("agent_output", "answer.markdown"),
                "reply_ref": ("signal_payload", "reply_ref"),
            },
        )
        self.assertEqual(reply.reply_delivery.target.app_name, "slack")
        self.assertEqual(reply.reply_delivery.target.operation, "events.reply")

    def test_route_agent_steps_reject_invalid_ids_and_when_references(self) -> None:
        invalid_steps = [
            ([{"prompt": "missing id"}], ".id is required"),
            (
                [
                    {"id": "diagnosis", "prompt": "first"},
                    {"id": "diagnosis", "prompt": "second"},
                ],
                "duplicates another step",
            ),
            (
                [
                    {
                        "id": "pr_fix",
                        "prompt": "fix",
                        "when": {
                            "stepId": "diagnosis",
                            "outputPath": "structured_output.actionable",
                            "equals": True,
                        },
                    }
                ],
                "must reference an earlier step",
            ),
            ([{"id": "empty"}], "prompt or messages is required"),
        ]
        for steps, message in invalid_steps:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError, message):
                    provider_module.configure(
                        "slack",
                        {
                            "agent": {
                                "provider": "simple",
                                "model": "deep",
                                "routes": [
                                    {
                                        "id": "stepped",
                                        "match": {"channel": "C_STEPS"},
                                        "agent": {"steps": steps},
                                    }
                                ],
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
                                        "app": "linear",
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
                                "app": "linear",
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
        self.assertNotIn("assistant.reconcileStuckRequests", catalog_ops)
        self.assertNotIn("chat.postMessage", rest_ops)
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
        self.assertNotIn("mode", bot_connection)
        self.assertNotIn("exposure", bot_connection)
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

        self.assertNotIn("chat.postMessage", rest_ops)

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

        _, captured = self._capture_chat_post_message(
            provider_module.ChatPostMessageInput(
                channel="C123",
                text="hello from gestalt",
            ),
            self._slack_bot_service_account_request(agent_slack_user_id="U456"),
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
                subject=gestalt.Subject(id="user:gestalt-123", kind="user"),
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
                credential=gestalt.Credential(mode="user"),
                subject=gestalt.Subject(
                    id="service_account:slack-bot-2", kind="service_account"
                ),
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
                subject=gestalt.Subject(
                    id="service_account:slack-bot-2", kind="service_account"
                ),
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

    def test_http_subject_resolves_slack_user_through_managed_external_identity(
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
        resource = request["resource"]
        action = request["action"]
        self.assertEqual(resource["type"], "external_identity")
        self.assertEqual(
            resource["id"],
            provider_module.external_identity_resource_id(
                "slack_identity",
                "team:T123:user:U456",
            ),
        )
        self.assertEqual(action["name"], "assume")
        self.assertNotIn("subject_type", request)

    def test_http_subject_dedupes_equivalent_managed_external_identity_subjects(
        self,
    ) -> None:
        canonical = authorization_subject(type="subject", id="user:gestalt-123")
        canonical.properties.update({"email": "ada@example.com"})
        legacy = authorization_subject(type="user", id="user:gestalt-123")
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

    def test_http_subject_uses_matching_route_run_as_before_slack_identity_lookup(
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
            gestalt.Request, "authorization", return_value=authorization
        ):
            resolved = provider_module.resolve_http_subject(
                gestalt.HTTPSubjectRequest(params=payload),
                gestalt.Request(),
            )

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved.id, "service_account:slack-bot")
        self.assertEqual(resolved.kind, "service_account")
        self.assertEqual(resolved.display_name, "Platform Slack Bot")
        self.assertEqual(resolved.auth_source, "slack_agent_route_run_as")
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
            gestalt.Request, "authorization", return_value=authorization
        ):
            resolved = provider_module.resolve_http_subject(
                gestalt.HTTPSubjectRequest(params=payload),
                gestalt.Request(),
            )

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved.id, "service_account:eng-background-agent")
        self.assertEqual(resolved.kind, "service_account")
        self.assertEqual(resolved.display_name, "Engineering Background Agent")
        self.assertEqual(resolved.auth_source, "slack_agent_route_run_as")
        self.assertEqual(authorization.requests, [])

    def test_addressed_channel_message_skips_unaddressed_run_as_route(self) -> None:
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
        self.assertEqual(workflow_client.signal_or_start_requests, [])

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
        self.assertEqual(resolved.auth_source, "authorization")
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
                    gestalt.Request, "authorization", return_value=authorization
                ):
                    resolved = provider_module.resolve_http_subject(
                        gestalt.HTTPSubjectRequest(params=payload),
                        gestalt.Request(),
                    )

                self.assertIsNone(resolved)
                self.assertEqual(authorization.requests, [])

    def test_http_subject_uses_signed_interaction_route_run_as_before_slack_identity_lookup(
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
            gestalt.Request, "authorization", return_value=authorization
        ):
            resolved = provider_module.resolve_http_subject(
                gestalt.HTTPSubjectRequest(params=payload),
                gestalt.Request(),
            )

        self.assertIsNotNone(resolved)
        assert resolved is not None
        self.assertEqual(resolved.id, "service_account:slack-bot")
        self.assertEqual(resolved.kind, "service_account")
        self.assertEqual(resolved.display_name, "Platform Slack Bot")
        self.assertEqual(resolved.auth_source, "slack_agent_route_run_as")
        self.assertEqual(authorization.requests, [])

    def test_http_subject_uses_slack_identity_for_user_signed_interaction_on_run_as_route(
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
        self.assertEqual(resolved.auth_source, "authorization")
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
                            "app": "linear",
                            "operation": "searchIssues",
                            "connection": "default",
                            "instance": "main",
                            "title": "Search Linear issues",
                            "description": "Find Linear issues relevant to the Slack request.",
                        },
                        {"app": "statusPage", "operation": "status"},
                        {"system": "workflow", "operation": "definitions.create"},
                        {"system": "workflow", "operation": "schedules.create"},
                    ],
                },
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
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )

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
                "workflow_provider": "local",
                "workflow_run_id": "run-123",
                "workflow_key": "slack:T123:C789:1712161829.000300",
                "workflow_signal_id": "signal-123",
                "started_run": True,
                "status": "WORKFLOW_RUN_STATUS_PENDING",
            },
        )
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)

        workflow_request = workflow_client.signal_or_start_requests[0]
        self.assertEqual(workflow_request.provider_name, "local")
        self.assertEqual(
            workflow_request.workflow_key, "slack:T123:C789:1712161829.000300"
        )
        expected_idempotency_key = "slack:event:T123:C789:1712161829.000300:U456"
        self.assertEqual(workflow_request.idempotency_key, expected_idempotency_key)

        agent_target = workflow_target_agent(workflow_request.target)
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
            agent_target.reply_delivery.target.app_name,
            "slack",
        )
        self.assertEqual(
            agent_target.reply_delivery.target.operation,
            "events.reply",
        )
        self.assertEqual(agent_target.reply_delivery.credential_mode, "none")
        self.assertEqual(
            app_delivery_bindings(agent_target.reply_delivery),
            {
                "text": ("agent_output", "text"),
                "reply_ref": ("signal_payload", "reply_ref"),
            },
        )
        self.assertEqual(
            agent_target.session_ready_reply_delivery.target.app_name,
            "slack",
        )
        self.assertEqual(
            agent_target.session_ready_reply_delivery.target.operation,
            "events.replySessionStarted",
        )
        self.assertEqual(
            agent_target.session_ready_reply_delivery.credential_mode, "none"
        )
        self.assertEqual(
            app_delivery_bindings(agent_target.session_ready_reply_delivery),
            {
                "session_id": ("agent_session", "id"),
                "reply_ref": ("signal_payload", "reply_ref"),
            },
        )
        self.assertNotIn("slack.events.reply", agent_target.messages[0].text)
        self.assertIn("final assistant answer", agent_target.messages[0].text)
        self.assertIn("slack.events.setStatus", agent_target.messages[0].text)
        self.assertNotIn("slack.interactions.request", agent_target.messages[0].text)
        self.assertNotIn("slack.events.startStream", agent_target.messages[0].text)
        self.assertNotIn("slack.events.appendStream", agent_target.messages[0].text)
        self.assertNotIn("slack.events.stopStream", agent_target.messages[0].text)
        self.assertIn(
            "slack.conversations.getThreadContext", agent_target.messages[0].text
        )
        self.assertIn("slack.files.get", agent_target.messages[0].text)
        self.assertNotIn("slack.chat.postMessage", agent_target.messages[0].text)

        target_metadata = sdk_value_to_dict(agent_target.metadata)
        self.assertEqual(target_metadata["slack"]["team_id"], "T123")
        self.assertEqual(target_metadata["slack"]["channel_id"], "C789")
        self.assertEqual(
            target_metadata["slack"]["root_message_ts"], "1712161829.000300"
        )
        self.assertNotIn("event_id", target_metadata["slack"])
        model_options = sdk_value_to_dict(agent_options(agent_target))
        self.assertEqual(model_options["temperature"], 0)

        signal = workflow_request.signal
        self.assertEqual(signal.name, "slack.event")
        self.assertEqual(signal.idempotency_key, expected_idempotency_key)
        signal_payload = sdk_value_to_dict(signal.payload)
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

        signal_metadata = sdk_value_to_dict(signal.metadata)
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
        workflow_client = FakeWorkflowClient()
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

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
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

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        workflow_request = workflow_client.signal_or_start_requests[0]
        signal_payload = sdk_value_to_dict(workflow_request.signal.payload)
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
        workflow_client = FakeWorkflowClient()
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
        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
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

        self.assertEqual(operation_body(response)["ok"], True)
        urlopen.assert_not_called()
        signal_payload = sdk_value_to_dict(
            workflow_client.signal_or_start_requests[0].signal.payload
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
        workflow_client = FakeWorkflowClient()
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

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
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

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        signal_payload = sdk_value_to_dict(
            workflow_client.signal_or_start_requests[0].signal.payload
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
        workflow_client = FakeWorkflowClient()
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

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
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

        self.assertEqual(operation_body(response)["ok"], True)
        signal_payload = sdk_value_to_dict(
            workflow_client.signal_or_start_requests[0].signal.payload
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
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )

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
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        workflow_request = workflow_client.signal_or_start_requests[0]
        self.assertEqual(
            workflow_request.workflow_key,
            "slack:T123:C0AH7JWFYM8:1777853873.601629",
        )
        signal_payload = sdk_value_to_dict(workflow_request.signal.payload)
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
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )

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
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        workflow_request = workflow_client.signal_or_start_requests[0]
        signal_payload = sdk_value_to_dict(workflow_request.signal.payload)
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
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )

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

        self.assertEqual(len(workflow_client.signal_or_start_requests), 2)
        first, second = workflow_client.signal_or_start_requests
        expected_idempotency_key = "slack:event:T123:C789:1712161829.000300:U456"
        self.assertEqual(first.workflow_key, "slack:T123:C789:1712161829.000300")
        self.assertEqual(second.workflow_key, first.workflow_key)
        self.assertEqual(first.idempotency_key, expected_idempotency_key)
        self.assertEqual(second.idempotency_key, expected_idempotency_key)
        self.assertEqual(first.signal.idempotency_key, expected_idempotency_key)
        self.assertEqual(second.signal.idempotency_key, expected_idempotency_key)
        first_metadata = sdk_value_to_dict(first.signal.metadata)
        second_metadata = sdk_value_to_dict(second.signal.metadata)
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
        workflow_client = FakeWorkflowClient()
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

        with (
            mock.patch.object(
                provider_module._agent,
                "_workflow_signal_response_fields",
                side_effect=RuntimeError("bad response"),
            ),
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
                "workflow_dispatched": True,
                "workflow_acknowledgement_failed": True,
            },
        )
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)

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
        workflow_client = FakeWorkflowClient()
        workflow_client.signal_or_start_error = RuntimeError("signal failed")
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
            {"error": "failed to signal workflow run: signal failed"},
        )
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        raw_idempotency_key = "slack:event:T123:C789:1712161829.000300:U456"
        expected_hash = hashlib.sha256(raw_idempotency_key.encode()).hexdigest()
        log_text = "\n".join(logs.output)
        self.assertIn("attempting Slack event workflow signal", log_text)
        self.assertIn("failed to signal Slack event workflow", log_text)
        self.assertIn(f"idempotency_key_sha256={expected_hash}", log_text)
        self.assertIn("error_type=RuntimeError", log_text)
        self.assertIn("error=signal failed", log_text)
        self.assertNotIn(raw_idempotency_key, log_text)
        self.assertNotIn("<@UBOT> summarize deploy status", log_text)

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

        self.assertEqual(operation_body(response), {"ok": True, "unlinked": True})
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
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        workflow_request = workflow_client.signal_or_start_requests[0]
        signal_metadata = sdk_value_to_dict(workflow_request.signal.metadata)
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

        self.assertEqual(operation_body(response), {"ok": True, "unlinked": True})
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
        workflow_client = DictWorkflowClient()
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
                "workflow_provider": "local",
                "workflow_run_id": "run-123",
                "workflow_key": "slack:T123:C789:1712161829.000300",
                "workflow_signal_id": "signal-123",
                "started_run": True,
                "status": "WORKFLOW_RUN_STATUS_PENDING",
            },
        )
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)

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
        workflow_client = FakeWorkflowClient()
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

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
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

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertNotIn("assistant_status_error", operation_body(response))
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        workflow_request = workflow_client.signal_or_start_requests[0]
        agent_target = workflow_target_agent(workflow_request.target)
        self.assertEqual(
            tool_ref_pairs(agent_target.tool_refs),
            BASE_EVENT_TOOL_REFS + ASSISTANT_EVENT_TOOL_REFS + WORKFLOW_EVENT_TOOL_REFS,
        )
        signal_payload = sdk_value_to_dict(workflow_request.signal.payload)
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

        class RecordingWorkflowClient(FakeWorkflowClient):
            def signal_or_start_run(self, request: Any) -> Any:
                sequence.append(("workflow", "signal"))
                return super().signal_or_start_run(request)

        workflow_client = RecordingWorkflowClient()
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

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
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

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertNotIn("acknowledgement_reaction_error", operation_body(response))
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
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
        workflow_client = FakeWorkflowClient()
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

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
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

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertNotIn("acknowledgement_reaction_error", operation_body(response))
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)

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
            operation_body(response),
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

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(operation_body(response)["suggested_prompts_set"], True)
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
            operation_body(response),
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
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
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
                        provider_module.SlackInteractionActionInput(
                            action_id="approve",
                            label="Approve",
                            value="approved",
                            style="primary",
                        )
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
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(operation_body(response)["workflow_run_id"], "run-123")
        self.assertEqual(
            operation_body(response)["workflow_key"],
            "slack:T123:C789:1712161829.000300",
        )
        self.assertEqual(operation_body(response)["action_id"], "approve")
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        workflow_request = workflow_client.signal_or_start_requests[0]
        self.assertEqual(
            workflow_request.workflow_key, "slack:T123:C789:1712161829.000300"
        )
        self.assertEqual(workflow_request.signal.name, "slack.interaction")
        self.assertTrue(
            workflow_request.signal.idempotency_key.startswith("slack:interaction:")
        )
        signal_payload = sdk_value_to_dict(workflow_request.signal.payload)
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
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        workflow_request = workflow_client.signal_or_start_requests[0]
        agent_target = workflow_target_agent(workflow_request.target)
        self.assertIn(
            ("slack", "events.setAssistantStatus"),
            tool_ref_pairs(agent_target.tool_refs),
        )
        self.assertIn(
            ("slack", "events.clearAssistantStatus"),
            tool_ref_pairs(agent_target.tool_refs),
        )
        signal_payload = sdk_value_to_dict(workflow_request.signal.payload)
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
        workflow_client = FakeWorkflowClient()
        interaction_payload = signed_block_action_payload()

        with (
            mock.patch.object(
                provider_module._agent,
                "_workflow_signal_response_fields",
                side_effect=RuntimeError("bad response"),
            ),
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
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(
            operation_body(response),
            {
                "ok": True,
                "workflow_dispatched": True,
                "workflow_acknowledgement_failed": True,
                "action_id": "approve",
            },
        )
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)

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
        workflow_client = FakeWorkflowClient()
        workflow_client.signal_or_start_error = RuntimeError("signal failed")
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
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        workflow_request = workflow_client.signal_or_start_requests[0]
        raw_idempotency_key = workflow_request.idempotency_key
        expected_hash = hashlib.sha256(raw_idempotency_key.encode()).hexdigest()
        raw_interaction_ref = interaction_payload["actions"][0]["value"]
        log_text = "\n".join(logs.output)
        self.assertIn("attempting Slack interaction workflow signal", log_text)
        self.assertIn("failed to signal Slack interaction workflow", log_text)
        self.assertIn(f"idempotency_key_sha256={expected_hash}", log_text)
        self.assertIn("error_type=RuntimeError", log_text)
        self.assertIn("error=signal failed", log_text)
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
                "agent": {"provider": "simple", "model": "deep"},
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
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(
            response.body,
            {"error": "failed to signal workflow run: workflow client unavailable"},
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
                        {"app": "linear", "operation": "searchIssues"},
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
                                        "app": "statusPage",
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
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )

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
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        workflow_request = workflow_client.signal_or_start_requests[0]
        agent_target = workflow_target_agent(workflow_request.target)
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
            ],
        )
        self.assertEqual(
            agent_target.reply_delivery.target.app_name,
            "supportSlackbot",
        )
        self.assertEqual(
            agent_target.reply_delivery.target.operation,
            "events.reply",
        )
        self.assertEqual(agent_target.reply_delivery.credential_mode, "none")
        self.assertNotIn("supportSlackbot.events.reply", agent_target.messages[0].text)
        self.assertNotIn(
            "supportSlackbot.events.startStream", agent_target.messages[0].text
        )
        self.assertIn(
            "supportSlackbot.conversations.getThreadContext",
            agent_target.messages[0].text,
        )
        self.assertNotIn(
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

        signal_metadata = sdk_value_to_dict(workflow_request.signal.metadata)
        self.assertEqual(signal_metadata["slack"]["agent_route_id"], "triage")
        self.assertEqual(signal_metadata["slack"]["addressed_to_bot"], True)
        model_options = sdk_value_to_dict(agent_options(agent_target))
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
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(
            operation_body(response)["workflow_provider"], "route-provider"
        )
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        self.assertEqual(
            workflow_client.signal_or_start_requests[0].provider_name,
            "route-provider",
        )
        self.assertEqual(
            workflow_target_agent(
                workflow_client.signal_or_start_requests[0].target
            ).timeout_seconds,
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
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(
            operation_body(response)["workflow_provider"], "route-provider"
        )
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        self.assertEqual(
            workflow_client.signal_or_start_requests[0].provider_name,
            "route-provider",
        )

    def test_user_scoped_interaction_rejects_different_slack_user(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
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
            gestalt.Request(
                subject=gestalt.Subject(id="user:gestalt-123", kind="user")
            ),
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
                    "provider": "simple",
                    "model": "deep",
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
                    subject=gestalt.Subject(
                        id="service_account:slack-bot", kind="service_account"
                    )
                ),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        signal_payload = sdk_value_to_dict(
            workflow_client.signal_or_start_requests[0].signal.payload
        )
        self.assertEqual(signal_payload["slack"]["user_id"], "U_HUMAN")

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
        workflow_client = FakeWorkflowClient()

        with mock.patch.object(
            gestalt.Request,
            "workflows",
            return_value=workflow_client,
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
        self.assertEqual(workflow_client.signal_or_start_requests, [])

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
        workflow_client = FakeWorkflowClient()
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

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
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

        self.assertEqual(operation_body(response)["ok"], True)
        agent_target = workflow_target_agent(
            workflow_client.signal_or_start_requests[0].target
        )
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
        workflow_client = FakeWorkflowClient()
        calls.clear()
        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
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

        self.assertEqual(operation_body(response)["ok"], True)
        agent_target = workflow_target_agent(
            workflow_client.signal_or_start_requests[0].target
        )
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
        workflow_client = FakeWorkflowClient()
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

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
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

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(calls[0][0], "/api/reactions.add")
        self.assertEqual(calls[0][1]["name"], "rocket")
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)

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
        workflow_client = FakeWorkflowClient()
        calls.clear()
        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
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

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(calls, [])
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)

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
        workflow_client = FakeWorkflowClient()
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
        thread_context_result = {
            "data": {
                "channel": "C_ROUTE",
                "thread_ts": "1712161829.000300",
                "messages": [],
                "messages_returned": 0,
            }
        }

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
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

        self.assertEqual(operation_body(response)["ok"], True)
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
        workflow_client = FakeWorkflowClient()
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

        with (
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
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

        self.assertEqual(operation_body(response)["ok"], True)
        get_context.assert_not_called()
        signal_payload = sdk_value_to_dict(
            workflow_client.signal_or_start_requests[0].signal.payload
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
                            {"app": "deployment", "operation": "status"},
                            {"app": "github", "operation": "search"},
                        ],
                        "route": [
                            {"app": "notion", "operation": "search"},
                            {"app": "pagerduty", "operation": "createIncident"},
                        ],
                    },
                    "toolSetRefs": ["shared"],
                    "tools": [
                        {"app": "github", "operation": "search"},
                        {"app": "notion", "operation": "search"},
                    ],
                    "routes": [
                        {
                            "id": "tool-route",
                            "match": {"channel": "C_ROUTE"},
                            "agent": {
                                "toolSetRefs": ["route"],
                                "tools": [
                                    {
                                        "app": "pagerduty",
                                        "operation": "createIncident",
                                    },
                                    {
                                        "app": "linear",
                                        "operation": "searchIssues",
                                        "runAs": {
                                            "subject": {
                                                "id": "service_account:slack-linear",
                                            },
                                        },
                                    },
                                ],
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
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        agent_target = workflow_target_agent(
            workflow_client.signal_or_start_requests[0].target
        )
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
        linear_ref = next(
            ref
            for ref in agent_target.tool_refs
            if ref.app == "linear" and ref.operation == "searchIssues"
        )
        self.assertEqual(
            _agent_subject_id(linear_ref.run_as),
            "service_account:slack-linear",
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

    def test_agent_can_explicitly_expose_interaction_request_tool(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "tools": [
                        {"app": "slack", "operation": "interactions.request"},
                    ],
                },
            },
        )
        self.addCleanup(provider_module.configure, "slack", {})
        workflow_client = FakeWorkflowClient()
        payload = {
            "type": "event_callback",
            "event_id": "EvExplicitInteractions",
            "team_id": "T123",
            "event": {
                "type": "app_mention",
                "user": "U456",
                "channel": "C789",
                "channel_type": "channel",
                "text": "<@UBOT> ask me to choose",
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
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        agent_target = workflow_target_agent(
            workflow_client.signal_or_start_requests[0].target
        )
        self.assertEqual(
            tool_ref_pairs(agent_target.tool_refs),
            [
                ("slack", "interactions.request"),
                *BASE_EVENT_TOOL_REFS,
            ],
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

        response, workflow_client = self._handle_event_with_workflow(payload)

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        workflow_request = workflow_client.signal_or_start_requests[0]
        self.assertEqual(
            workflow_request.workflow_key,
            "slack:T123:C_SUPPORT:1712161829.000300",
        )
        signal_payload = sdk_value_to_dict(workflow_request.signal.payload)
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
        signal_metadata = sdk_value_to_dict(workflow_request.signal.metadata)
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

        response, workflow_client = self._handle_event_with_workflow(payload)

        self.assertEqual(operation_body(response)["ok"], True)
        workflow_request = workflow_client.signal_or_start_requests[0]
        self.assertEqual(
            workflow_request.workflow_key,
            "slack:T123:C_SUPPORT:1712161829.000300",
        )
        self.assertEqual(
            workflow_request.idempotency_key,
            "slack:event:T123:C_SUPPORT:1712161835.000400:U456",
        )
        signal_payload = sdk_value_to_dict(workflow_request.signal.payload)
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
                self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
                signal_metadata = sdk_value_to_dict(
                    workflow_client.signal_or_start_requests[0].signal.metadata
                )
                self.assertEqual(
                    signal_metadata["slack"]["agent_route_id"], "channel-roots"
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
        self.assertEqual(workflow_client.signal_or_start_requests, [])

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
        self.assertEqual(workflow_client.signal_or_start_requests, [])

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
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        workflow_request = workflow_client.signal_or_start_requests[0]
        self.assertEqual(
            workflow_request.workflow_key,
            "slack:T123:C_SUPPORT:1712161829.000300",
        )
        signal_metadata = sdk_value_to_dict(workflow_request.signal.metadata)
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
                response, workflow_client = self._handle_event_with_workflow(
                    {
                        "type": "event_callback",
                        "event_id": f"Ev{route_id}",
                        "team_id": "T123",
                        "event": event,
                    }
                )

                self.assertEqual(operation_body(response)["ok"], True)
                self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
                signal_metadata = sdk_value_to_dict(
                    workflow_client.signal_or_start_requests[0].signal.metadata
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

        response, workflow_client = self._handle_event_with_workflow(payload)

        self.assertEqual(operation_body(response)["ok"], True)
        signal_metadata = sdk_value_to_dict(
            workflow_client.signal_or_start_requests[0].signal.metadata
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

                response, workflow_client = self._handle_event_with_workflow(payload)

                self.assertEqual(operation_body(response)["ok"], True)
                self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
                signal_metadata = sdk_value_to_dict(
                    workflow_client.signal_or_start_requests[0].signal.metadata
                )
                self.assertEqual(
                    signal_metadata["slack"]["agent_route_id"],
                    f"route-{event_type}",
                )
                if event_type == "message.app_home":
                    workflow_request = workflow_client.signal_or_start_requests[0]
                    self.assertEqual(workflow_request.workflow_key, "slack:T123:D_HOME")
                    signal_payload = sdk_value_to_dict(workflow_request.signal.payload)
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

                self.assertEqual(operation_body(response)["ok"], True)
                self.assertEqual(operation_body(response)["event_type"], event_type)

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

        self.assertEqual(
            operation_body(response), {"ok": True, "ignored": "no_matching_agent_route"}
        )

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

        response, workflow_client = self._handle_event_with_workflow(
            {
                **file_share_payload,
                "event_id": "EvFileShareMatched",
            }
        )

        self.assertEqual(operation_body(response)["ok"], True)
        signal_metadata = sdk_value_to_dict(
            workflow_client.signal_or_start_requests[0].signal.metadata
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

                response, workflow_client = self._handle_event_with_workflow(payload)

                self.assertEqual(
                    operation_body(response), {"ok": True, "ignored": "ignored_event"}
                )
                self.assertEqual(workflow_client.signal_or_start_requests, [])

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
        self.assertEqual(workflow_client.signal_or_start_requests, [])

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
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        workflow_request = workflow_client.signal_or_start_requests[0]
        signal_payload = sdk_value_to_dict(workflow_request.signal.payload)
        signal_metadata = sdk_value_to_dict(workflow_request.signal.metadata)
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

    def test_datadog_bot_agent_route_does_not_require_publish_route_match(
        self,
    ) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot", "userId": "UBOT"},
                "workflow": {"provider": "local"},
                "events": {
                    "publish": {
                        "routes": [
                            {
                                "id": "brain-ingest",
                                "match": {"eventTypes": ["message"]},
                            }
                        ]
                    }
                },
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
                "WorkflowPublishEvent",
                FakeWorkflowPublishEvent,
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
                    subject=gestalt.Subject(
                        id="service_account:slack-bot", kind="service_account"
                    )
                ),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        self.assertEqual(workflow_client.publish_event_requests, [])
        signal_metadata = sdk_value_to_dict(
            workflow_client.signal_or_start_requests[0].signal.metadata
        )
        self.assertEqual(
            signal_metadata["slack"]["agent_route_id"], "alert-bot-messages"
        )
        self.assertEqual(signal_metadata["slack"]["bot_id"], "B123")

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
        self.assertEqual(workflow_client.signal_or_start_requests, [])

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
        workflow_client = FakeWorkflowClient()
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

        self.assertEqual(len(workflow_client.signal_or_start_requests), 2)
        requests = workflow_client.signal_or_start_requests
        self.assertEqual(
            requests[0].workflow_key,
            requests[1].workflow_key,
        )
        self.assertEqual(requests[0].workflow_key, "slack:T123:C789:1712161829.000300")
        for workflow_request in requests:
            target_metadata = sdk_value_to_dict(
                workflow_target_agent(workflow_request.target).metadata
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
        first_metadata = sdk_value_to_dict(requests[0].signal.metadata)
        second_metadata = sdk_value_to_dict(requests[1].signal.metadata)
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

        with self.assertLogs(provider_module._agent.logger, level="INFO") as logs:
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
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

    def test_url_verification_returns_challenge_without_workflow_run(self) -> None:
        payload = {"type": "url_verification", "challenge": "challenge-token"}

        response = provider_module.slack_events_handle(payload, gestalt.Request())

        self.assertEqual(operation_body(response), {"challenge": "challenge-token"})

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
        workflow_client = FakeWorkflowClient()
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
                "WorkflowPublishEvent",
                FakeWorkflowPublishEvent,
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
                "published": True,
                "published_event_count": 1,
                "workflow_event_ids": ["slack:EvPublish"],
                "route_ids": ["deployments"],
            },
        )
        self.assertEqual(len(workflow_client.publish_event_requests), 1)
        request = workflow_client.publish_event_requests[0]
        self.assertEqual(request.provider_name, "local")
        event = request.event
        self.assertEqual(event.id, "slack:EvPublish")
        self.assertEqual(event.type, "deployment.slack_event")
        self.assertEqual(event.source, "slack/events")
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

    def test_agent_signal_failure_returns_non_2xx_even_with_publish_route(self) -> None:
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
        workflow_client = FakeWorkflowClient()
        workflow_client.signal_or_start_error = RuntimeError("signal failed")
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
                "WorkflowPublishEvent",
                FakeWorkflowPublishEvent,
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
            result.body, {"error": "failed to signal workflow run: signal failed"}
        )
        self.assertEqual(workflow_client.publish_event_requests, [])
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)

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
        workflow_client = ExplodingPublishResponseWorkflowClient()
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
                "WorkflowPublishEvent",
                FakeWorkflowPublishEvent,
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
                "published": True,
                "published_event_count": 1,
                "workflow_event_ids": ["slack:EvPublishBadResponse"],
                "route_ids": ["brain-ingest"],
            },
        )
        self.assertEqual(len(workflow_client.publish_event_requests), 1)

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
                "WorkflowPublishEvent",
                FakeWorkflowPublishEvent,
            ),
            mock.patch.object(
                provider_module._agent,
                "_workflow_signal_response_fields",
                side_effect=RuntimeError("bad response"),
            ),
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
                "workflow_dispatched": True,
                "workflow_acknowledgement_failed": True,
            },
        )
        self.assertEqual(len(workflow_client.publish_event_requests), 1)
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)

    def test_publish_failure_after_agent_handoff_is_logged_not_returned(
        self,
    ) -> None:
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
        workflow_client = FakeWorkflowClient()
        workflow_client.publish_event_error = RuntimeError("boom")
        payload = {
            "type": "event_callback",
            "event_id": "EvPublishFailsAfterSignal",
            "team_id": "T123",
            "event": {
                "type": "message",
                "user": "U456",
                "channel": "C789",
                "channel_type": "im",
                "text": "publish fails after signal",
                "ts": "1712161829.000300",
            },
        }

        with (
            mock.patch.object(
                provider_module._agent.gestalt,
                "WorkflowPublishEvent",
                FakeWorkflowPublishEvent,
            ),
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflow_client,
                create=True,
            ),
            mock.patch.object(provider_module._agent.logger, "warning") as warning,
        ):
            response = provider_module.slack_events_handle(
                payload,
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(operation_body(response)["ok"], True)
        self.assertEqual(operation_body(response)["workflow_key"], "slack:T123:C789")
        self.assertNotIn("published_event_count", operation_body(response))
        self.assertEqual(len(workflow_client.signal_or_start_requests), 1)
        self.assertEqual(len(workflow_client.publish_event_requests), 1)
        warning.assert_called_once()
        self.assertIn(
            "ignored Slack workflow event publish failure after agent handoff",
            warning.call_args.args[0],
        )
        self.assertIn(
            "slack_event_id=EvPublishFailsAfterSignal", warning.call_args.args[1]
        )
        self.assertIn("workflow_key=slack:T123:C789", warning.call_args.args[1])

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
                "WorkflowPublishEvent",
                FakeWorkflowPublishEvent,
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
        self.assertEqual(len(workflow_client.publish_event_requests), 2)
        bot_event = workflow_client.publish_event_requests[0].event
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
        workflow_client = FakeWorkflowClient()
        workflow_client.publish_event_error = RuntimeError("boom")
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
                "WorkflowPublishEvent",
                FakeWorkflowPublishEvent,
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
            response.body, {"error": "failed to publish workflow event: boom"}
        )

    def test_publish_workflow_client_failure_returns_non_2xx(self) -> None:
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
                "WorkflowPublishEvent",
                FakeWorkflowPublishEvent,
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
            {"error": "failed to publish workflow event: workflow client unavailable"},
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


def _agent_subject_id(subject: Any) -> str:
    return cast(str, getattr(subject, "id", ""))


if __name__ == "__main__":
    unittest.main()
