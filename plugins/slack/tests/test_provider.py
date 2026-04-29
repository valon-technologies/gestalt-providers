from __future__ import annotations

import io
import json
import pathlib
import unittest
import urllib.error
import urllib.parse
import urllib.request
from email.message import Message
from http import HTTPStatus
from typing import Any, cast
from unittest import mock

import gestalt
import yaml
from google.protobuf import json_format
from google.protobuf import struct_pb2
from gestalt.gen.v1 import agent_pb2 as _agent_pb2
from gestalt.gen.v1 import authorization_pb2 as _authorization_pb2
from gestalt.gen.v1 import workflow_pb2 as _workflow_pb2

import provider as provider_module

agent_pb2: Any = _agent_pb2
authorization_pb2: Any = _authorization_pb2
workflow_pb2: Any = _workflow_pb2
PLUGIN_DIR = pathlib.Path(__file__).resolve().parents[1]
BASE_EVENT_TOOL_REFS = [
    ("*", ""),
    ("slack", "events.reply"),
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
    ("slack", "events.startStream"),
    ("slack", "events.appendStream"),
    ("slack", "events.stopStream"),
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
        tool_source: int = 0,
        tool_refs: list[Any] | None = None,
        **_kwargs: Any,
    ) -> None:
        self.provider_name = provider_name
        self.model = model
        self.prompt = prompt
        self.messages = messages or []
        self.tool_source = tool_source
        self.tool_refs = tool_refs or []
        self.metadata = new_struct()
        self.provider_options = new_struct()


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


class FakeWorkflowPb2:
    WORKFLOW_RUN_STATUS_PENDING = 1
    WorkflowRunStatus = FakeWorkflowRunStatus
    WorkflowSignal = FakeWorkflowSignal
    BoundWorkflowRun = FakeBoundWorkflowRun
    ManagedWorkflowRunSignal = FakeManagedWorkflowRunSignal
    BoundWorkflowAgentTarget = FakeBoundWorkflowAgentTarget
    BoundWorkflowTarget = FakeBoundWorkflowTarget
    WorkflowManagerSignalOrStartRunRequest = (
        FakeWorkflowManagerSignalOrStartRunRequest
    )


def workflow_pb2_with_signal_or_start_contract() -> Any:
    if hasattr(workflow_pb2, "WorkflowManagerSignalOrStartRunRequest"):
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
    return [(str(ref.plugin), str(ref.operation)) for ref in refs]


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

    def __enter__(self) -> FakeWorkflowManager:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def signal_or_start_run(self, request: Any) -> Any:
        self.signal_or_start_requests.append(request)
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


class SlackProviderTests(unittest.TestCase):
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
        self.assertIn("requires reply_ref and text", catalog_ops["events.reply"]["description"])
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
            _catalog_parameter_names(catalog_ops["events.clearAssistantStatus"]),
            ["reply_ref"],
        )
        self.assertEqual(
            _catalog_parameter_names(catalog_ops["interactions.request"]),
            ["reply_ref", "text", "actions", "expires_in_seconds"],
        )
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
        bot_connection = connections["bot"]
        self.assertEqual(bot_connection["mode"], "platform")
        self.assertEqual(bot_connection["auth"], {"type": "bearer"})
        self.assertNotIn("instance" + "Selector", json.dumps(manifest))

        selector_operations = (
            "conversations.list",
            "chat.postMessage",
            "conversations.history",
            "conversations.replies",
        )
        for operation_name in selector_operations:
            operation = rest_ops[operation_name]
            self.assertEqual(
                operation["connectionSelector"],
                {
                    "parameter": "actor",
                    "default": "bot",
                    "values": {"bot": "bot", "user": "default"},
                },
            )
            self.assertIn("actor", _manifest_parameter_names(operation))

        self.assertEqual(rest_ops["search.messages"]["connection"], "default")
        self.assertNotIn("connectionSelector", rest_ops["search.messages"])
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

    def test_agent_tool_source_native_search_handles_new_and_legacy_sdks(self) -> None:
        class NativeAgentPB:
            AGENT_TOOL_SOURCE_MODE_NATIVE_SEARCH = 7

        class LegacyAgentPB:
            AGENT_TOOL_SOURCE_MODE_EXPLICIT = 1

        with mock.patch("internals.agent.agent_pb2", NativeAgentPB):
            self.assertEqual(provider_module._agent_tool_source_native_search(), 7)
        with mock.patch("internals.agent.agent_pb2", LegacyAgentPB):
            self.assertEqual(provider_module._agent_tool_source_native_search(), 1)

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

        with mock.patch(
            "internals.agent.urllib.request.urlopen", side_effect=fake_urlopen
        ):
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
        with mock.patch("internals.agent.urllib.request.urlopen") as urlopen:
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

        with mock.patch(
            "internals.agent.urllib.request.urlopen", side_effect=fake_urlopen
        ):
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

    def test_slack_event_handler_signals_workflow_with_user_tool_search(self) -> None:
        provider_module.configure(
            "slack",
            {
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
                "agentProvider": "simple",
                "agentModel": "deep",
                "agentProviderOptions": {"temperature": 0},
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
                "text": "<@UBOT> summarize deploy status",
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
            mock.patch.object(
                provider_module._agent, "workflow_pb2", workflow_pb2_contract
            ),
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
        self.assertEqual(workflow_request.idempotency_key, "slack:event:Ev123")

        agent_target = workflow_request.target.agent
        self.assertEqual(agent_target.provider_name, "simple")
        self.assertEqual(agent_target.model, "deep")
        self.assertIn("final workflow signal batch", agent_target.prompt)
        self.assertEqual(len(agent_target.messages), 1)
        self.assertEqual(
            agent_target.tool_source, provider_module._agent_tool_source_native_search()
        )
        self.assertEqual(
            tool_ref_pairs(agent_target.tool_refs),
            BASE_EVENT_TOOL_REFS + WORKFLOW_EVENT_TOOL_REFS,
        )
        self.assertIn("slack.events.reply", agent_target.messages[0].text)
        self.assertIn("text set to the complete", agent_target.messages[0].text)
        self.assertIn("runtime supplies it", agent_target.messages[0].text)
        self.assertIn("slack.events.setStatus", agent_target.messages[0].text)
        self.assertIn("slack.interactions.request", agent_target.messages[0].text)
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
        provider_options = json_format.MessageToDict(agent_target.provider_options)
        self.assertEqual(provider_options["temperature"], 0)

        signal = workflow_request.signal
        self.assertEqual(signal.name, "slack.event")
        self.assertEqual(signal.idempotency_key, "slack:event:Ev123")
        signal_payload = json_format.MessageToDict(signal.payload)
        self.assertEqual(signal_payload["slack"]["event_id"], "Ev123")
        self.assertEqual(signal_payload["slack"]["file_ids"], ["F123"])
        self.assertEqual(
            signal_payload["slack"]["text"], "<@UBOT> summarize deploy status"
        )
        self.assertIn(
            "operation: slack.conversations.getThreadContext",
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
        self.assertIn("Final reply tool:", signal_payload["user_prompt"])
        self.assertIn("operation: slack.events.reply", signal_payload["user_prompt"])
        self.assertIn(
            "text: <complete Slack message body to post>",
            signal_payload["user_prompt"],
        )
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
            gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123", kind="user")),
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
                    "username": "Valon Assistant",
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
            mock.patch.object(
                provider_module._agent, "workflow_pb2", workflow_pb2_contract
            ),
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
                        "username": "Valon Assistant",
                    },
                )
            ],
        )

    def test_slack_event_handler_adds_acknowledgement_reaction_before_workflow(
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
            mock.patch.object(
                provider_module._agent, "workflow_pb2", workflow_pb2_contract
            ),
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
            sequence, [("slack", "/api/reactions.add"), ("workflow", "signal")]
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
            mock.patch.object(
                provider_module._agent, "workflow_pb2", workflow_pb2_contract
            ),
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
                gestalt.Request(
                    subject=gestalt.Subject(id="user:gestalt-123", kind="user")
                ),
            )

        self.assertEqual(
            captured["payload"],
            {
                "channel": "C789",
                "text": "Here is the answer",
                "thread_ts": "1712161829.000300",
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
            mock.patch.object(
                provider_module._agent, "workflow_pb2", workflow_pb2_contract
            ),
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
        self.assertIn("Final reply tool:", signal_payload["user_prompt"])
        self.assertIn("operation: slack.events.reply", signal_payload["user_prompt"])
        self.assertIn(
            "text: <complete Slack message body to post>",
            signal_payload["user_prompt"],
        )
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
            mock.patch.object(
                provider_module._agent, "workflow_pb2", workflow_pb2_contract
            ),
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
                    username="Valon Assistant",
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
                    username="Valon Assistant",
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
                        "username": "Valon Assistant",
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
                        "username": "Valon Assistant",
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
                "bot": {"token": "xoxb-test-bot"},
                "workflow": {"provider": "local"},
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
                "text": "please triage this",
                "ts": "1712161829.000300",
            },
        }
        request = gestalt.Request(
            subject=gestalt.Subject(id="user:gestalt-123", kind="user")
        )
        workflow_pb2_contract = workflow_pb2_with_signal_or_start_contract()

        with (
            mock.patch.object(
                provider_module._agent, "workflow_pb2", workflow_pb2_contract
            ),
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
                ("*", ""),
                ("supportSlackbot", "events.reply"),
                ("supportSlackbot", "events.setStatus"),
                ("supportSlackbot", "events.deleteStatus"),
                ("supportSlackbot", "events.addReaction"),
                ("supportSlackbot", "events.removeReaction"),
                ("supportSlackbot", "interactions.request"),
            ],
        )
        self.assertIn("supportSlackbot.events.reply", agent_target.messages[0].text)
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
        self.assertIn(
            "Follow the global Slack policy.", agent_target.messages[0].text
        )
        self.assertIn("Triage support requests.", agent_target.messages[0].text)

        signal_metadata = json_format.MessageToDict(workflow_request.signal.metadata)
        self.assertEqual(signal_metadata["slack"]["agent_route_id"], "triage")
        provider_options = json_format.MessageToDict(agent_target.provider_options)
        self.assertEqual(provider_options["temperature"], 0)
        self.assertEqual(provider_options["max_output_tokens"], 2000)

    def test_repeated_slack_events_reuse_session_key_but_keep_event_metadata_on_turns(
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
            mock.patch.object(
                provider_module._agent, "workflow_pb2", workflow_pb2_contract
            ),
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

        self.assertEqual(requests[0].idempotency_key, "slack:event:EvFirst")
        self.assertEqual(requests[1].idempotency_key, "slack:event:EvSecond")
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
            gestalt.Request(subject=gestalt.Subject(id="user:gestalt-123", kind="user")),
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


if __name__ == "__main__":
    unittest.main()
