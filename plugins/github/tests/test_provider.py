from __future__ import annotations

import io
import json
import pathlib
import unittest
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from email.message import Message
from http import HTTPStatus
from typing import Any, cast
from unittest import mock

import gestalt
from google.protobuf import json_format
from gestalt._gen.v1 import agent_pb2 as _agent_pb2
from gestalt._gen.v1 import workflow_pb2 as _workflow_pb2
import yaml

import internals.client as client_module
import internals.operations as operations_module
from internals.config import GitHubBotIdentity
from internals.errors import GitHubAPIError
import provider as provider_module

agent_pb2: Any = _agent_pb2
workflow_pb2: Any = _workflow_pb2


class FakeHTTPResponse:
    def __init__(self, body: Any = None) -> None:
        self._body = json.dumps(body or {}).encode("utf-8")

    def __enter__(self) -> FakeHTTPResponse:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


class FakeWorkflowManager:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.requests: list[Any] = []

    def __enter__(self) -> FakeWorkflowManager:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def signal_or_start_run(self, request: Any) -> Any:
        self.requests.append(request)
        if self.fail:
            raise RuntimeError("workflow manager unavailable")
        run = workflow_pb2.BoundWorkflowRun(
            id="workflow-run-123",
            status=workflow_pb2.WORKFLOW_RUN_STATUS_RUNNING,
            target=request.target,
            workflow_key=request.workflow_key,
        )
        signal = workflow_pb2.WorkflowSignal(id="signal-123")
        signal.CopyFrom(request.signal)
        signal.id = "signal-123"
        return workflow_pb2.ManagedWorkflowRunSignal(
            provider_name=request.provider_name,
            run=run,
            signal=signal,
            started_run=True,
            workflow_key=request.workflow_key,
        )


def request_json(request: urllib.request.Request) -> dict[str, Any]:
    data = request.data
    if data is None:
        return {}
    data = cast(bytes, data)
    return cast(dict[str, Any], json.loads(data.decode("utf-8")))


def request_path(request: urllib.request.Request) -> str:
    return urllib.parse.urlparse(request.full_url).path


def auth_header(request: urllib.request.Request) -> str:
    return request.get_header("Authorization") or dict(request.header_items()).get(
        "Authorization", ""
    )


def http_error(
    url: str, status: int, body: str = '{"message":"Not Found"}'
) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        url=url,
        code=status,
        msg="error",
        hdrs=Message(),
        fp=io.BytesIO(body.encode("utf-8")),
    )


def github_request(
    installation_id: int = 99, repo: str = "acme/widgets"
) -> gestalt.Request:
    return gestalt.Request(
        subject=gestalt.Subject(
            id=f"service_account:github_app_installation:{installation_id}:repo:{repo}",
            kind="service_account",
            display_name=f"GitHub App installation {installation_id}",
            auth_source="github_app_webhook",
        )
    )


class RecordingGitHubClient(client_module.GitHubAPIClient):
    def __init__(self) -> None:
        self.tokens: list[tuple[int, tuple[str, ...], dict[str, str]]] = []
        self.requests: list[tuple[str, str, str | None, dict[str, Any]]] = []
        self.commit_message = ""
        self.tree_entries: list[dict[str, Any]] = []

    def installation_token(
        self,
        installation_id: int,
        *,
        repositories: Sequence[str] | None = None,
        permissions: Mapping[str, str] | None = None,
    ) -> str:
        repositories_tuple = tuple(repositories or ())
        permissions_dict = dict(permissions or {})
        self.tokens.append((installation_id, repositories_tuple, permissions_dict))
        permissions_key = ",".join(
            f"{key}:{value}" for key, value in sorted(permissions_dict.items())
        )
        return f"token:{permissions_key}"

    def github_json(
        self,
        method: str,
        path: str,
        token: str | None,
        payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        body = dict(payload or {})
        self.requests.append((method, path, token, body))
        if path == "/repos/acme/widgets/git/commits/base-commit":
            return {"tree": {"sha": "base-tree"}}
        if path == "/repos/acme/widgets/git/trees":
            tree = body.get("tree")
            self.tree_entries = tree if isinstance(tree, list) else []
            return {"sha": "new-tree"}
        if path == "/repos/acme/widgets/git/commits":
            self.commit_message = str(body.get("message", ""))
            return {"sha": "new-commit"}
        if path == "/repos/acme/widgets/git/refs":
            return {}
        raise AssertionError(f"unexpected GitHub request {method} {path}")

    def github_json_value(
        self,
        method: str,
        path: str,
        token: str | None,
        payload: Mapping[str, Any] | None = None,
    ) -> Any:
        return self.github_json(method, path, token, payload)

    def repository_default_branch(self, token: str, owner: str, repo: str) -> str:
        return "main"

    def get_branch_ref(
        self, token: str, owner: str, repo: str, branch: str
    ) -> dict[str, Any] | None:
        if branch == "feature":
            return None
        if branch == "main":
            return {"object": {"sha": "base-commit"}}
        raise AssertionError(f"unexpected branch ref lookup {branch}")

    def require_branch_ref(
        self, token: str, owner: str, repo: str, branch: str, field_name: str
    ) -> dict[str, Any]:
        ref = self.get_branch_ref(token, owner, repo, branch)
        if ref is None:
            raise ValueError(f"{field_name} branch {branch!r} was not found")
        return ref

    def object_sha(self, ref: Mapping[str, Any], name: str) -> str:
        obj = ref.get("object")
        if not isinstance(obj, dict) or not isinstance(obj.get("sha"), str):
            raise AssertionError(f"missing {name} sha")
        return obj["sha"]

    def bot_identity_or_none(self) -> GitHubBotIdentity | None:
        return GitHubBotIdentity(
            name="Example App Bot",
            login="example-app[bot]",
            user_id="12345678",
            email="12345678+example-app[bot]@users.noreply.github.com",
        )

    def commit_url(self, owner: str, repo: str, sha: str) -> str:
        return f"https://github.example/{owner}/{repo}/commit/{sha}"


class GitHubProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "modelOptions": {"temperature": 0},
                },
            },
        )
        self.addCleanup(provider_module.configure, "github", {})

    def test_manifest_declares_github_app_webhook_contract(self) -> None:
        manifest_path = pathlib.Path(__file__).resolve().parents[1] / "manifest.yaml"
        manifest = yaml.safe_load(manifest_path.read_text())

        spec = manifest["spec"]
        webhook = spec["http"]["event"]
        security = spec["securitySchemes"]["github_app"]

        self.assertEqual(webhook["path"], "/event")
        self.assertEqual(webhook["method"], "POST")
        self.assertEqual(webhook["credentialMode"], "none")
        self.assertEqual(webhook["security"], "github_app")
        self.assertEqual(webhook["target"], provider_module.GITHUB_EVENT_OPERATION)
        self.assertNotIn("ack", webhook)
        self.assertEqual(security["type"], "hmac")
        self.assertEqual(security["secret"]["env"], "GITHUB_WEBHOOK_SECRET")
        self.assertEqual(security["signatureHeader"], "X-Hub-Signature-256")
        self.assertEqual(security["signaturePrefix"], "sha256=")
        self.assertEqual(security["payloadTemplate"], "{raw_body}")

    def test_post_connect_maps_default_connection_to_external_identity(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(request_path(request), "/user")
            self.assertEqual(auth_header(request), "Bearer user-token")
            return FakeHTTPResponse({"id": 12345678, "login": "octocat"})

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            metadata = provider_module.post_connect(
                gestalt.ConnectedToken(
                    access_token="user-token",
                    connection="default",
                    subject_id="subject-1",
                )
            )

        self.assertEqual(
            metadata,
            {
                "gestalt.external_identity.type": "github_identity",
                "gestalt.external_identity.id": "user:12345678",
                "github.user_id": "12345678",
                "github.login": "octocat",
            },
        )

    def test_post_connect_skips_non_default_connection(self) -> None:
        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            metadata = provider_module.post_connect(
                gestalt.ConnectedToken(
                    access_token="token",
                    connection="bot",
                    subject_id="subject-1",
                )
            )

        self.assertEqual(metadata, {})
        urlopen.assert_not_called()

    def test_bot_identity_is_derived_from_github_app(self) -> None:
        calls: list[tuple[str, str]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            path = request_path(request)
            calls.append((path, auth_header(request)))
            if path == "/app":
                self.assertEqual(auth_header(request), "Bearer app-jwt")
                return FakeHTTPResponse(
                    {"name": "Example App Bot", "slug": "example-app"}
                )
            if path == "/users/example-app%5Bbot%5D":
                self.assertEqual(auth_header(request), "")
                return FakeHTTPResponse({"id": 12345678, "login": "example-app[bot]"})
            self.fail(f"unexpected request {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            identity = client_module.bot_identity()
            cached_identity = client_module.bot_identity()

        self.assertEqual(identity, cached_identity)
        self.assertEqual(identity.name, "Example App Bot")
        self.assertEqual(identity.login, "example-app[bot]")
        self.assertEqual(identity.user_id, "12345678")
        self.assertEqual(
            identity.email, "12345678+example-app[bot]@users.noreply.github.com"
        )
        self.assertEqual(
            calls, [("/app", "Bearer app-jwt"), ("/users/example-app%5Bbot%5D", "")]
        )

    def test_commit_message_skips_bot_identity_when_bot_coauthor_disabled(self) -> None:
        with mock.patch(
            "internals.client.bot_identity",
            side_effect=AssertionError("unexpected lookup"),
        ):
            message = operations_module.commit_message_with_coauthors(
                "Update README",
                coauthors=[
                    operations_module.GitHubCoAuthor(
                        name="Ada", email="ada@example.com"
                    )
                ],
                include_bot=False,
            )

        self.assertEqual(
            message, "Update README\n\nCo-authored-by: Ada <ada@example.com>"
        )

    def test_invalid_coauthors_are_rejected_before_github_calls(self) -> None:
        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.bot_commit_files(
                provider_module.CommitFilesInput(
                    owner="acme",
                    repo="widgets",
                    message="Update README",
                    files=[
                        provider_module.FileChangeInput(
                            path="README.md", content="hello"
                        )
                    ],
                    branch="feature",
                    base_branch="main",
                    installation_id=99,
                    coauthors=[provider_module.CoAuthorInput(name="", email="")],
                ),
                github_request(),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertIn("coauthor name and email are required", response.body["error"])
        urlopen.assert_not_called()

    def test_commit_files_uses_typed_github_client_interface(self) -> None:
        recording_client = RecordingGitHubClient()
        client: client_module.GitHubAPIClient = recording_client

        commit = operations_module.commit_files(
            operations_module.GitHubCommitRequest(
                owner="acme",
                repo="widgets",
                message="Update README",
                files=(
                    operations_module.GitHubFileChange(
                        path="/README.md", content="hello"
                    ),
                ),
                branch="feature",
                base_branch="main",
                installation_id=99,
                coauthors=(
                    operations_module.GitHubCoAuthor(
                        name="Ada", email="ada@example.com"
                    ),
                ),
            ),
            subject=github_request().subject,
            pull_request_permissions=True,
            client=client,
        )

        self.assertEqual(commit.commit_sha, "new-commit")
        self.assertEqual(
            commit.commit_url,
            "https://github.example/acme/widgets/commit/new-commit",
        )
        self.assertEqual(commit.files_changed, 1)
        self.assertEqual(
            recording_client.tokens,
            [
                (
                    99,
                    ("widgets",),
                    {"contents": "write", "pull_requests": "write"},
                )
            ],
        )
        self.assertEqual(
            [request[1] for request in recording_client.requests],
            [
                "/repos/acme/widgets/git/commits/base-commit",
                "/repos/acme/widgets/git/trees",
                "/repos/acme/widgets/git/commits",
                "/repos/acme/widgets/git/refs",
            ],
        )
        self.assertEqual(
            recording_client.tree_entries,
            [
                {
                    "path": "README.md",
                    "mode": "100644",
                    "type": "blob",
                    "content": "hello",
                }
            ],
        )
        self.assertIn(
            "Co-authored-by: Ada <ada@example.com>",
            recording_client.commit_message,
        )
        self.assertIn(
            "Co-authored-by: Example App Bot "
            "<12345678+example-app[bot]@users.noreply.github.com>",
            recording_client.commit_message,
        )
        normalized = operations_module.normalize_file_changes(
            (
                operations_module.GitHubFileChange(
                    path="/docs/guide.md", content="guide"
                ),
            )
        )
        self.assertIsInstance(normalized, tuple)
        self.assertEqual(normalized[0].path, "docs/guide.md")

    def test_bot_identity_retries_user_lookup_after_partial_derivation(self) -> None:
        calls: list[str] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            path = request_path(request)
            calls.append(path)
            if path == "/app":
                return FakeHTTPResponse(
                    {"name": "Example App Bot", "slug": "example-app"}
                )
            if path == "/users/example-app%5Bbot%5D" and calls.count(path) == 1:
                raise http_error(request.full_url, HTTPStatus.FORBIDDEN)
            if path == "/users/example-app%5Bbot%5D":
                return FakeHTTPResponse({"id": 12345678, "login": "example-app[bot]"})
            self.fail(f"unexpected request {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            partial_identity = client_module.bot_identity()
            full_identity = client_module.bot_identity()

        self.assertEqual(partial_identity.login, "example-app[bot]")
        self.assertEqual(partial_identity.email, "")
        self.assertEqual(
            full_identity.email,
            "12345678+example-app[bot]@users.noreply.github.com",
        )
        self.assertEqual(
            calls,
            [
                "/app",
                "/users/example-app%5Bbot%5D",
                "/app",
                "/users/example-app%5Bbot%5D",
            ],
        )

    def test_resolve_http_subject_maps_installation_to_service_account(self) -> None:
        subject = provider_module.resolve_http_subject(
            gestalt.HTTPSubjectRequest(
                params={
                    "installation": {"id": 99.0},
                    "repository": {"full_name": "acme/widgets"},
                }
            )
        )

        self.assertIsNotNone(subject)
        assert subject is not None
        self.assertEqual(
            subject.id, "service_account:github_app_installation:99:repo:acme/widgets"
        )
        self.assertEqual(subject.kind, "service_account")
        self.assertEqual(subject.auth_source, "github_app_webhook")
        self.assertIn("acme/widgets", subject.display_name)

    def test_webhook_handler_signals_workflow_run(self) -> None:
        workflow_manager = FakeWorkflowManager()
        payload = {
            "action": "opened",
            "installation": {"id": 99.0, "app_id": 12345},
            "repository": {
                "id": 1,
                "full_name": "acme/widgets",
                "name": "widgets",
                "owner": {"login": "acme"},
                "default_branch": "main",
            },
            "pull_request": {
                "number": 7.0,
                "title": "Add widget workflow",
                "state": "open",
                "html_url": "https://github.com/acme/widgets/pull/7",
                "head": {"ref": "feature", "sha": "abc123"},
                "base": {"ref": "main", "sha": "def456"},
            },
            "headers": {
                "X-GitHub-Delivery": "delivery-123",
                "X-Hub-Signature-256": "sha256=secret",
            },
            "sender": {"id": 10, "login": "octocat", "type": "User"},
        }

        with (
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch.object(
                gestalt.Request,
                "agent_manager",
                side_effect=AssertionError("agent manager should not be called"),
            ),
        ):
            result = provider_module.github_events_handle(payload, gestalt.Request())

        self.assertEqual(result["ok"], True)
        self.assertEqual(result["dispatch"], "workflow")
        self.assertEqual(result["workflow_provider"], "local")
        self.assertEqual(result["workflow_run_id"], "workflow-run-123")
        self.assertEqual(result["workflow_key"], "github:99:acme/widgets:7")
        self.assertEqual(result["workflow_signal_id"], "signal-123")
        self.assertEqual(result["workflow_started_run"], True)
        self.assertEqual(len(workflow_manager.requests), 1)

        request = workflow_manager.requests[0]
        self.assertEqual(request.provider_name, "local")
        self.assertEqual(request.workflow_key, "github:99:acme/widgets:7")
        self.assertTrue(
            request.idempotency_key.startswith(
                "github:event:acme/widgets:pull_request:opened:"
            )
        )

        agent = request.target.agent
        self.assertEqual(agent.provider_name, "simple")
        self.assertEqual(agent.model, "deep")
        self.assertEqual(
            [tool.plugin for tool in agent.tool_refs],
            ["github", "github", "github"],
        )
        self.assertEqual(
            [tool.operation for tool in agent.tool_refs],
            [
                provider_module.BOT_COMMIT_FILES_OPERATION,
                provider_module.BOT_OPEN_PULL_REQUEST_OPERATION,
                provider_module.BOT_CREATE_PULL_REQUEST_OPERATION,
            ],
        )
        metadata = json_format.MessageToDict(agent.metadata)
        self.assertEqual(metadata["github"]["installation_id"], 99)
        self.assertEqual(metadata["github"]["repository"], "acme/widgets")
        self.assertEqual(metadata["github"]["number"], 7)

        self.assertEqual(request.signal.name, "github.app.webhook")
        data = json_format.MessageToDict(request.signal.payload)
        self.assertEqual(data["delivery_id"], "delivery-123")
        self.assertEqual(data["github_event"], "pull_request")
        self.assertEqual(data["github_action"], "opened")
        self.assertEqual(data["installation"]["id"], 99)
        self.assertEqual(data["repository"]["full_name"], "acme/widgets")
        self.assertEqual(data["sender"]["login"], "octocat")
        self.assertEqual(data["summary"]["head_ref"], "feature")
        self.assertEqual(data["summary"]["base_ref"], "main")
        self.assertEqual(data["payload_omitted"], True)
        self.assertIn("payload_sha256", data)
        self.assertNotIn("payload", data)
        self.assertNotIn("_gestalt_payload_preview_json", json.dumps(data))
        agent_request = data["agent_request"]
        self.assertEqual(agent_request["subject"]["repository"], "acme/widgets")
        self.assertEqual(agent_request["subject"]["number"], 7)
        self.assertEqual(
            agent_request["subject"]["html_url"],
            "https://github.com/acme/widgets/pull/7",
        )
        self.assertEqual(agent_request["pull_request"]["head_ref"], "feature")
        self.assertEqual(agent_request["pull_request"]["base_ref"], "main")
        self.assertEqual(agent_request["pull_request"]["title"], "Add widget workflow")
        self.assertIn("GitHub App webhook", agent_request["user_prompt"])
        self.assertIn("head_ref: feature", agent_request["user_prompt"])

    def test_ci_webhooks_use_event_specific_workflow_keys(self) -> None:
        base = {
            "action": "completed",
            "installation": {"id": 99},
            "repository": {
                "full_name": "acme/widgets",
                "name": "widgets",
                "owner": {"login": "acme"},
            },
            "sender": {"id": 10, "login": "octocat", "type": "User"},
        }
        cases: list[tuple[str, dict[str, Any], str, int | None, list[int]]] = [
            (
                "check_run",
                {"id": 123.0, "pull_requests": [{"number": 7.0}]},
                "github:99:acme/widgets:check_run:123",
                7,
                [7],
            ),
            (
                "check_suite",
                {"id": 456, "pull_requests": [{"number": 7}, {"number": 8}]},
                "github:99:acme/widgets:check_suite:456",
                None,
                [7, 8],
            ),
            (
                "workflow_run",
                {"pull_requests": [{"number": 9}]},
                "github:99:acme/widgets:workflow_run:delivery-workflow_run",
                9,
                [9],
            ),
        ]

        for event_type, event_object, expected_key, number, pr_numbers in cases:
            with self.subTest(event_type=event_type):
                request = self._workflow_signal_request(
                    {
                        **base,
                        event_type: event_object,
                        "headers": {"X-GitHub-Delivery": f"delivery-{event_type}"},
                    }
                )
                self.assertEqual(request.workflow_key, expected_key)
                data = cast(
                    dict[str, Any],
                    json_format.MessageToDict(request.signal.payload),
                )
                event_id_key = f"{event_type}_id"
                if "id" in event_object:
                    self.assertEqual(data["summary"][event_id_key], event_object["id"])
                else:
                    self.assertNotIn(event_id_key, data["summary"])
                self.assertEqual(data["summary"]["pull_request_numbers"], pr_numbers)
                if number is None:
                    self.assertNotIn("number", data["summary"])
                else:
                    self.assertEqual(data["summary"]["number"], number)
                self.assertIn(
                    event_type,
                    json_format.MessageToDict(request.target.agent.metadata)["github"][
                        "session_ref"
                    ],
                )
        digest_fallback = self._workflow_signal_request(
            {
                **base,
                "check_run": {"pull_requests": []},
            }
        )
        self.assertTrue(
            digest_fallback.workflow_key.startswith(
                "github:99:acme/widgets:check_run:payload:"
            )
        )
        self.assertNotEqual(digest_fallback.workflow_key, "github:99:acme/widgets")

    def test_explicit_policy_dispatches_failed_check_run_to_comment_tools(
        self,
    ) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "fallback"},
                "webhookPolicies": [
                    {
                        "id": "failed-ci-comment",
                        "match": {
                            "events": ["check_run"],
                            "actions": ["completed"],
                            "statuses": ["completed"],
                            "conclusions": ["failure"],
                            "repositories": ["acme/widgets"],
                            "branches": ["main"],
                            "checkNames": ["Build Gestalt"],
                        },
                        "agent": {
                            "provider": "simple",
                            "model": "deep",
                            "systemPrompt": "Investigate failed CI.",
                        },
                        "action": {"mode": "comment"},
                    }
                ],
            },
        )

        request = self._workflow_signal_request(
            {
                "action": "completed",
                "installation": {"id": 99},
                "repository": {
                    "full_name": "acme/widgets",
                    "name": "widgets",
                    "owner": {"login": "acme"},
                },
                "check_run": {
                    "id": 123,
                    "name": "Build Gestalt",
                    "status": "completed",
                    "conclusion": "failure",
                    "html_url": "https://github.com/acme/widgets/runs/123",
                    "details_url": "https://ci.example/runs/123",
                    "head_sha": "abc123",
                    "head_branch": "main",
                    "pull_requests": [{"number": 7}],
                },
                "headers": {
                    "X-GitHub-Event": "check_run",
                    "X-GitHub-Delivery": "delivery-check-run",
                },
                "sender": {"login": "octocat"},
            }
        )

        self.assertEqual(
            request.workflow_key,
            "github:99:acme/widgets:check_run:123:policy:failed-ci-comment",
        )
        self.assertIn(
            ":policy:failed-ci-comment:",
            request.idempotency_key,
        )
        agent = request.target.agent
        self.assertEqual(agent.model, "deep")
        self.assertEqual(
            [tool.operation for tool in agent.tool_refs],
            [
                provider_module.BOT_GET_CHECK_RUN_OPERATION,
                provider_module.BOT_LIST_CHECK_RUN_ANNOTATIONS_OPERATION,
                provider_module.BOT_GET_WORKFLOW_RUN_OPERATION,
                provider_module.BOT_LIST_WORKFLOW_RUN_JOBS_OPERATION,
                provider_module.BOT_CREATE_ISSUE_COMMENT_OPERATION,
            ],
        )
        metadata = json_format.MessageToDict(agent.metadata)
        self.assertEqual(metadata["github"]["policy"]["id"], "failed-ci-comment")
        self.assertEqual(metadata["github"]["policy"]["mode"], "comment")

        data = cast(
            dict[str, Any],
            json_format.MessageToDict(request.signal.payload),
        )
        self.assertEqual(data["webhook_policy"]["id"], "failed-ci-comment")
        self.assertEqual(data["check_run"]["name"], "Build Gestalt")
        self.assertEqual(data["check_run"]["conclusion"], "failure")
        self.assertEqual(data["check_run"]["pull_request_numbers"], [7])
        self.assertEqual(data["agent_request"]["policy"]["mode"], "comment")
        self.assertIn(
            "policy_id: failed-ci-comment", data["agent_request"]["user_prompt"]
        )

    def test_explicit_policy_webhook_events_allowlist_semantics(self) -> None:
        push_payload = {
            "ref": "refs/heads/feature",
            "after": "1" * 40,
            "commits": [],
            "installation": {"id": 99},
            "repository": {"full_name": "acme/widgets"},
            "headers": {"X-GitHub-Event": "push"},
            "sender": {"login": "octocat"},
        }
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {"id": "push-observe", "match": {"events": ["push"]}}
                ],
            },
        )
        request = self._workflow_signal_request(push_payload)
        self.assertEqual(
            request.workflow_key,
            "github:99:acme/widgets:policy:push-observe",
        )

        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "webhookEvents": ["pull_request"],
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {"id": "push-observe", "match": {"events": ["push"]}}
                ],
            },
        )
        with mock.patch.object(
            gestalt.Request,
            "workflow_manager",
            side_effect=AssertionError("workflow manager should not be called"),
            create=True,
        ):
            result = provider_module.github_events_handle(
                push_payload, gestalt.Request()
            )

        self.assertEqual(result, {"ok": True, "ignored": "unsupported_event_type:push"})

    def test_explicit_policy_no_match_is_ignored(self) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {
                        "id": "failed-ci",
                        "match": {
                            "events": ["check_run"],
                            "conclusions": ["failure"],
                        },
                    }
                ],
            },
        )
        with mock.patch.object(
            gestalt.Request,
            "workflow_manager",
            side_effect=AssertionError("workflow manager should not be called"),
            create=True,
        ):
            result = provider_module.github_events_handle(
                {
                    "action": "completed",
                    "installation": {"id": 99},
                    "repository": {"full_name": "acme/widgets"},
                    "check_run": {
                        "id": 123,
                        "status": "completed",
                        "conclusion": "success",
                    },
                    "headers": {"X-GitHub-Event": "check_run"},
                    "sender": {"login": "octocat"},
                },
                gestalt.Request(),
            )

        self.assertEqual(result, {"ok": True, "ignored": "policy_not_matched"})

    def test_policy_validation_and_allowed_operation_order(self) -> None:
        for config, expected in (
            (
                {"webhookPolicies": [{"id": "bad id"}]},
                "id must match",
            ),
            (
                {
                    "webhookPolicies": [
                        {"id": "duplicate"},
                        {"id": "duplicate"},
                    ]
                },
                "duplicate webhook policy id",
            ),
            (
                {
                    "webhookPolicies": [
                        {
                            "id": "unknown-op",
                            "action": {"allowedOperations": ["bot.nope"]},
                        }
                    ]
                },
                "unknown operation",
            ),
        ):
            with self.subTest(expected=expected):
                with self.assertRaisesRegex(ValueError, expected):
                    provider_module.configure(
                        "github",
                        {
                            "appId": "12345",
                            "appPrivateKey": "unused-in-tests",
                            "workflow": {"provider": "local"},
                            **config,
                        },
                    )

        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {
                        "id": "empty-tools",
                        "match": {"actions": ["opened"]},
                        "action": {"allowedOperations": []},
                    },
                    {
                        "id": "ordered-tools",
                        "action": {
                            "allowedOperations": [
                                provider_module.BOT_CREATE_PULL_REQUEST_OPERATION,
                                provider_module.BOT_GET_CHECK_RUN_OPERATION,
                                provider_module.BOT_GET_CHECK_RUN_OPERATION,
                            ]
                        },
                    },
                ],
            },
        )
        empty = self._workflow_signal_request(
            {
                "action": "opened",
                "installation": {"id": 99},
                "repository": {"full_name": "acme/widgets"},
                "pull_request": {"number": 7},
                "headers": {"X-GitHub-Event": "pull_request"},
                "sender": {"login": "octocat"},
            }
        )
        self.assertEqual([tool.operation for tool in empty.target.agent.tool_refs], [])

        ordered = self._workflow_signal_request(
            {
                "action": "closed",
                "installation": {"id": 99},
                "repository": {"full_name": "acme/widgets"},
                "pull_request": {"number": 7},
                "headers": {"X-GitHub-Event": "pull_request"},
                "sender": {"login": "octocat"},
            }
        )
        self.assertEqual(
            [tool.operation for tool in ordered.target.agent.tool_refs],
            [
                provider_module.BOT_GET_CHECK_RUN_OPERATION,
                provider_module.BOT_CREATE_PULL_REQUEST_OPERATION,
            ],
        )

    def _workflow_signal_request(self, payload: dict[str, Any]) -> Any:
        workflow_manager = FakeWorkflowManager()
        with (
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch.object(
                gestalt.Request,
                "agent_manager",
                side_effect=AssertionError("agent manager should not be called"),
            ),
        ):
            result = provider_module.github_events_handle(payload, gestalt.Request())
        self.assertEqual(result["ok"], True)
        self.assertEqual(result["dispatch"], "workflow")
        self.assertEqual(len(workflow_manager.requests), 1)
        return workflow_manager.requests[0]

    def _workflow_signal_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        request = self._workflow_signal_request(payload)
        return cast(
            dict[str, Any],
            json_format.MessageToDict(request.signal.payload),
        )

    def test_webhook_handler_compacts_issue_comment_and_review_context(self) -> None:
        long_body = "please update this workflow\n" + ("x" * 10000)
        base = {
            "installation": {"id": 99},
            "repository": {
                "full_name": "acme/widgets",
                "name": "widgets",
                "owner": {"login": "acme"},
            },
            "sender": {"id": 10, "login": "octocat", "type": "User"},
        }

        issue_comment_request = self._workflow_signal_request(
            {
                **base,
                "action": "created",
                "issue": {
                    "number": 7,
                    "title": "Broken widget",
                    "state": "open",
                    "html_url": "https://github.com/acme/widgets/issues/7",
                },
                "comment": {
                    "id": 111,
                    "html_url": "https://github.com/acme/widgets/issues/7#issuecomment-111",
                    "body": long_body,
                    "user": {"login": "octocat"},
                },
            }
        )
        issue_comment = cast(
            dict[str, Any],
            json_format.MessageToDict(issue_comment_request.signal.payload),
        )
        self.assertEqual(issue_comment["github_event"], "issue_comment")
        self.assertNotIn("payload", issue_comment)
        self.assertNotIn("_gestalt_payload_preview_json", json.dumps(issue_comment))
        self.assertNotIn("comment_body", issue_comment["summary"])
        self.assertNotIn(
            "please update this workflow", json.dumps(issue_comment["summary"])
        )
        issue_metadata = json_format.MessageToDict(
            issue_comment_request.signal.metadata
        )
        self.assertNotIn("comment_body", issue_metadata["github"])
        self.assertNotIn("please update this workflow", json.dumps(issue_metadata))
        comment = issue_comment["agent_request"]["comment"]
        self.assertEqual(comment["id"], 111)
        self.assertLess(len(comment["body"]), 5000)
        self.assertTrue(comment["body"].endswith("...<truncated>"))
        self.assertIn(
            "please update this workflow", issue_comment["agent_request"]["user_prompt"]
        )

        review_request = self._workflow_signal_request(
            {
                **base,
                "action": "submitted",
                "pull_request": {
                    "number": 8,
                    "title": "Refactor widgets",
                    "state": "open",
                    "html_url": "https://github.com/acme/widgets/pull/8",
                    "head": {"ref": "feature"},
                    "base": {"ref": "main"},
                },
                "review": {
                    "id": 222,
                    "state": "commented",
                    "html_url": "https://github.com/acme/widgets/pull/8#pullrequestreview-222",
                    "body": long_body,
                    "user": {"login": "reviewer"},
                },
            }
        )
        review = cast(
            dict[str, Any],
            json_format.MessageToDict(review_request.signal.payload),
        )
        self.assertEqual(review["github_event"], "pull_request_review")
        self.assertNotIn("review_body", review["summary"])
        self.assertNotIn("please update this workflow", json.dumps(review["summary"]))
        review_metadata = json_format.MessageToDict(review_request.signal.metadata)
        self.assertNotIn("review_body", review_metadata["github"])
        self.assertNotIn("please update this workflow", json.dumps(review_metadata))
        self.assertLess(len(review["agent_request"]["review"]["body"]), 5000)
        self.assertIn("review:", review["agent_request"]["user_prompt"])

        review_comment = self._workflow_signal_payload(
            {
                **base,
                "action": "created",
                "pull_request": {
                    "number": 9,
                    "title": "Fix widget docs",
                    "state": "open",
                    "html_url": "https://github.com/acme/widgets/pull/9",
                    "head": {"ref": "docs"},
                    "base": {"ref": "main"},
                },
                "comment": {
                    "id": 333,
                    "html_url": "https://github.com/acme/widgets/pull/9#discussion_r333",
                    "body": long_body,
                    "user": {"login": "reviewer"},
                },
            }
        )
        self.assertEqual(review_comment["github_event"], "pull_request_review_comment")
        self.assertLess(len(review_comment["agent_request"]["comment"]["body"]), 5000)
        self.assertIn("comment:", review_comment["agent_request"]["user_prompt"])

    def test_webhook_handler_compacts_ref_event_when_configured(self) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "webhookEvents": ["push"],
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
            },
        )
        payload = {
            "ref": "refs/heads/feature",
            "before": "0" * 40,
            "after": "1" * 40,
            "base_ref": "refs/heads/main",
            "compare": "https://github.com/acme/widgets/compare/0...1",
            "created": False,
            "deleted": False,
            "forced": True,
            "head_commit": {
                "id": "1" * 40,
                "message": "Update widgets",
                "url": "https://github.com/acme/widgets/commit/1111",
            },
            "commits": [{"id": "raw-commit-that-should-not-be-copied"}],
            "installation": {"id": 99},
            "repository": {"full_name": "acme/widgets"},
            "sender": {"login": "octocat"},
        }

        data = self._workflow_signal_payload(payload)

        self.assertEqual(data["github_event"], "push")
        self.assertNotIn("payload", data)
        agent_request = data["agent_request"]
        self.assertEqual(agent_request["ref"], "refs/heads/feature")
        self.assertEqual(agent_request["before"], "0" * 40)
        self.assertEqual(agent_request["after"], "1" * 40)
        self.assertEqual(agent_request["base_ref"], "refs/heads/main")
        self.assertEqual(agent_request["forced"], True)
        self.assertEqual(agent_request["head_commit"]["id"], "1" * 40)
        self.assertNotIn("commits", agent_request)
        self.assertIn("ref: refs/heads/feature", agent_request["user_prompt"])

    def test_webhook_handler_fails_retryable_without_workflow_manager(
        self,
    ) -> None:
        payload = {
            "action": "opened",
            "installation": {"id": 99.0},
            "repository": {"full_name": "acme/widgets"},
            "pull_request": {"number": 7.0},
            "sender": {"login": "octocat"},
        }

        with self.assertLogs(provider_module.logger, level="ERROR") as logs:
            result = provider_module.github_events_handle(payload, gestalt.Request())

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.SERVICE_UNAVAILABLE)
        self.assertIn("failed to dispatch workflow run", response.body["error"])
        self.assertIn("failed to dispatch GitHub webhook workflow", logs.output[0])

    def test_webhook_handler_workflow_dispatch_failure_is_retryable(self) -> None:
        payload = {
            "action": "opened",
            "installation": {"id": 99.0},
            "repository": {"full_name": "acme/widgets"},
            "pull_request": {"number": 7.0},
            "sender": {"login": "octocat"},
        }

        with mock.patch.object(
            gestalt.Request,
            "workflow_manager",
            return_value=FakeWorkflowManager(fail=True),
            create=True,
        ):
            with self.assertLogs(provider_module.logger, level="ERROR") as logs:
                result = provider_module.github_events_handle(
                    payload, gestalt.Request()
                )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.SERVICE_UNAVAILABLE)
        self.assertIn("workflow manager unavailable", response.body["error"])
        self.assertIn("failed to dispatch GitHub webhook workflow", logs.output[0])

    def test_commit_files_creates_branch_commit_and_bot_coauthor(self) -> None:
        calls: list[tuple[str, str, dict[str, Any], str]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            calls.append((method, path, body, auth_header(request)))

            if path == "/app":
                self.assertEqual(method, "GET")
                return FakeHTTPResponse(
                    {"name": "Example App Bot", "slug": "example-app"}
                )
            if path == "/users/example-app%5Bbot%5D":
                self.assertEqual(method, "GET")
                self.assertEqual(auth_header(request), "")
                return FakeHTTPResponse({"id": 12345678, "login": "example-app[bot]"})
            if path == "/app/installations/99/access_tokens":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer app-jwt")
                self.assertEqual(body["permissions"], {"contents": "write"})
                return FakeHTTPResponse({"token": "installation-token"})
            if path == "/repos/acme/widgets/git/ref/heads/feature":
                raise http_error(request.full_url, HTTPStatus.NOT_FOUND)
            if path == "/repos/acme/widgets/git/ref/heads/main":
                return FakeHTTPResponse({"object": {"sha": "base-commit"}})
            if path == "/repos/acme/widgets/git/commits/base-commit":
                return FakeHTTPResponse({"tree": {"sha": "base-tree"}})
            if path == "/repos/acme/widgets/git/trees":
                self.assertEqual(
                    body["tree"],
                    [
                        {
                            "path": "README.md",
                            "mode": "100644",
                            "type": "blob",
                            "content": "hello",
                        }
                    ],
                )
                return FakeHTTPResponse({"sha": "new-tree"})
            if path == "/repos/acme/widgets/git/commits":
                self.assertEqual(body["tree"], "new-tree")
                self.assertEqual(body["parents"], ["base-commit"])
                self.assertIn("Co-authored-by: Ada <ada@example.com>", body["message"])
                self.assertIn(
                    "Co-authored-by: Example App Bot <12345678+example-app[bot]@users.noreply.github.com>",
                    body["message"],
                )
                return FakeHTTPResponse({"sha": "new-commit"})
            if path == "/repos/acme/widgets/git/refs":
                self.assertEqual(
                    body, {"ref": "refs/heads/feature", "sha": "new-commit"}
                )
                return FakeHTTPResponse({})
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_commit_files(
                provider_module.CommitFilesInput(
                    owner="acme",
                    repo="widgets",
                    message="Update README",
                    files=[
                        provider_module.FileChangeInput(
                            path="README.md", content="hello"
                        )
                    ],
                    branch="feature",
                    base_branch="main",
                    installation_id=99,
                    coauthors=[
                        provider_module.CoAuthorInput(
                            name="Ada", email="ada@example.com"
                        )
                    ],
                ),
                github_request(),
            )

        self.assertIsInstance(result, dict)
        data = cast(dict[str, Any], result)["data"]["commit"]
        self.assertEqual(data["sha"], "new-commit")
        self.assertEqual(data["branch"], "feature")
        self.assertEqual(data["base_branch"], "main")
        self.assertTrue(data["branch_created"])
        self.assertEqual(calls[-1][1], "/repos/acme/widgets/git/refs")

    def test_create_pull_request_commits_files_then_opens_pr(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)

            if path == "/app":
                return FakeHTTPResponse(
                    {"name": "Example App Bot", "slug": "example-app"}
                )
            if path == "/users/example-app%5Bbot%5D":
                self.assertEqual(auth_header(request), "")
                return FakeHTTPResponse({"id": 12345678, "login": "example-app[bot]"})
            if path == "/app/installations/99/access_tokens":
                permissions = body["permissions"]
                if permissions == {"contents": "write", "pull_requests": "write"}:
                    return FakeHTTPResponse({"token": "write-token"})
                if permissions == {"pull_requests": "write"}:
                    return FakeHTTPResponse({"token": "pr-token"})
            if path == "/repos/acme/widgets/git/ref/heads/feature":
                raise http_error(request.full_url, HTTPStatus.NOT_FOUND)
            if path == "/repos/acme/widgets/git/ref/heads/main":
                return FakeHTTPResponse({"object": {"sha": "base-commit"}})
            if path == "/repos/acme/widgets/git/commits/base-commit":
                return FakeHTTPResponse({"tree": {"sha": "base-tree"}})
            if path == "/repos/acme/widgets/git/trees":
                return FakeHTTPResponse({"sha": "new-tree"})
            if path == "/repos/acme/widgets/git/commits":
                return FakeHTTPResponse({"sha": "new-commit"})
            if path == "/repos/acme/widgets/git/refs":
                return FakeHTTPResponse({})
            if path == "/repos/acme/widgets/pulls":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer pr-token")
                self.assertEqual(body["title"], "Update README")
                self.assertEqual(body["head"], "feature")
                self.assertEqual(body["base"], "main")
                return FakeHTTPResponse(
                    {
                        "number": 42,
                        "title": "Update README",
                        "state": "open",
                        "html_url": "https://github.com/acme/widgets/pull/42",
                        "url": "https://api.github.com/repos/acme/widgets/pulls/42",
                        "head": {"ref": "feature"},
                        "base": {"ref": "main"},
                    }
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_create_pull_request(
                provider_module.CreatePullRequestInput(
                    owner="acme",
                    repo="widgets",
                    title="Update README",
                    message="Update README",
                    files=[
                        provider_module.FileChangeInput(
                            path="README.md", content="hello"
                        )
                    ],
                    branch="feature",
                    base="main",
                ),
                github_request(),
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["commit"]["sha"], "new-commit")
        self.assertEqual(data["pull_request"]["number"], 42)
        self.assertEqual(
            data["pull_request"]["html_url"],
            "https://github.com/acme/widgets/pull/42",
        )

    def test_create_issue_comment_falls_back_to_pull_request_permission(
        self,
    ) -> None:
        calls: list[tuple[str, str, dict[str, Any], str]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            calls.append((method, path, body, auth_header(request)))

            if path == "/app/installations/99/access_tokens":
                if body["permissions"] == {"issues": "write"}:
                    raise http_error(request.full_url, HTTPStatus.FORBIDDEN)
                self.assertEqual(body["permissions"], {"pull_requests": "write"})
                return FakeHTTPResponse({"token": "pr-token"})
            if path == "/repos/acme/widgets/issues/7/comments":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer pr-token")
                self.assertEqual(body, {"body": "Likely fix: update the snapshot."})
                return FakeHTTPResponse(
                    {
                        "id": 123,
                        "node_id": "IC_kw",
                        "url": "https://api.github.com/repos/acme/widgets/issues/comments/123",
                        "html_url": "https://github.com/acme/widgets/pull/7#issuecomment-123",
                        "body": "Likely fix: update the snapshot.",
                        "user": {"login": "example-app[bot]"},
                        "created_at": "2026-05-01T00:00:00Z",
                        "updated_at": "2026-05-01T00:00:00Z",
                    }
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_create_issue_comment(
                provider_module.CreateIssueCommentInput(
                    owner="acme",
                    repo="widgets",
                    issue_number=7,
                    body="Likely fix: update the snapshot.",
                ),
                github_request(),
            )

        data = cast(dict[str, Any], result)["data"]["comment"]
        self.assertEqual(data["id"], 123)
        self.assertEqual(data["user"]["login"], "example-app[bot]")
        self.assertEqual(
            [
                call[2].get("permissions")
                for call in calls
                if call[1].endswith("access_tokens")
            ],
            [{"issues": "write"}, {"pull_requests": "write"}],
        )

    def test_ci_read_operations_use_github_shapes_and_pagination(self) -> None:
        calls: list[tuple[str, str, dict[str, Any]]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            calls.append((method, path, body))

            if path == "/app/installations/99/access_tokens":
                if body["permissions"] == {"checks": "read"}:
                    return FakeHTTPResponse({"token": "checks-token"})
                if body["permissions"] == {"actions": "read"}:
                    return FakeHTTPResponse({"token": "actions-token"})
            if path == "/repos/acme/widgets/check-runs/123":
                self.assertEqual(auth_header(request), "Bearer checks-token")
                return FakeHTTPResponse(
                    {
                        "id": 123,
                        "name": "Build Gestalt",
                        "status": "completed",
                        "conclusion": "failure",
                        "html_url": "https://github.com/acme/widgets/runs/123",
                        "details_url": "https://ci.example/runs/123",
                        "head_sha": "abc123",
                    }
                )
            if path == "/repos/acme/widgets/check-runs/123/annotations":
                self.assertEqual(
                    urllib.parse.urlparse(request.full_url).query,
                    "per_page=2&page=3",
                )
                self.assertEqual(auth_header(request), "Bearer checks-token")
                return FakeHTTPResponse(
                    [
                        {
                            "path": "README.md",
                            "start_line": 4,
                            "end_line": 4,
                            "annotation_level": "failure",
                            "message": "broken",
                        }
                    ]
                )
            if path == "/repos/acme/widgets/actions/runs/456":
                self.assertEqual(auth_header(request), "Bearer actions-token")
                return FakeHTTPResponse(
                    {
                        "id": 456,
                        "name": "CI",
                        "status": "completed",
                        "conclusion": "failure",
                        "run_number": 12,
                        "html_url": "https://github.com/acme/widgets/actions/runs/456",
                    }
                )
            if path == "/repos/acme/widgets/actions/runs/456/jobs":
                self.assertEqual(
                    urllib.parse.urlparse(request.full_url).query,
                    "per_page=5&page=1&filter=all",
                )
                self.assertEqual(auth_header(request), "Bearer actions-token")
                return FakeHTTPResponse(
                    {
                        "total_count": 1,
                        "jobs": [
                            {
                                "id": 789,
                                "run_id": 456,
                                "name": "test",
                                "status": "completed",
                                "conclusion": "failure",
                                "html_url": "https://github.com/acme/widgets/actions/runs/456/job/789",
                            }
                        ],
                    }
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            check_run = provider_module.bot_get_check_run(
                provider_module.GetCheckRunInput(
                    owner="acme", repo="widgets", check_run_id=123
                ),
                github_request(),
            )
            annotations = provider_module.bot_list_check_run_annotations(
                provider_module.ListCheckRunAnnotationsInput(
                    owner="acme",
                    repo="widgets",
                    check_run_id=123,
                    per_page=2,
                    page=3,
                ),
                github_request(),
            )
            workflow_run = provider_module.bot_get_workflow_run(
                provider_module.GetWorkflowRunInput(
                    owner="acme", repo="widgets", run_id=456
                ),
                github_request(),
            )
            jobs = provider_module.bot_list_workflow_run_jobs(
                provider_module.ListWorkflowRunJobsInput(
                    owner="acme",
                    repo="widgets",
                    run_id=456,
                    filter="all",
                    per_page=5,
                    page=1,
                ),
                github_request(),
            )

        self.assertEqual(
            cast(dict[str, Any], check_run)["data"]["check_run"]["id"], 123
        )
        self.assertEqual(
            cast(dict[str, Any], annotations)["data"]["annotations"][0]["message"],
            "broken",
        )
        self.assertEqual(
            cast(dict[str, Any], workflow_run)["data"]["workflow_run"]["name"], "CI"
        )
        self.assertEqual(cast(dict[str, Any], jobs)["data"]["jobs"][0]["id"], 789)
        self.assertGreaterEqual(len(calls), 8)

    def test_commit_files_rejects_invalid_inputs_before_github_calls(self) -> None:
        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.bot_commit_files(
                provider_module.CommitFilesInput(
                    owner="acme",
                    repo="widgets",
                    message="Update README",
                    files=[
                        provider_module.FileChangeInput(
                            path="README.md", content="hello"
                        )
                    ],
                    branch="../feature",
                    base_branch="main",
                    installation_id=99,
                ),
                github_request(),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertIn("branch", response.body["error"])
        urlopen.assert_not_called()

        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.bot_commit_files(
                provider_module.CommitFilesInput(
                    owner="acme",
                    repo="widgets",
                    message="Update README",
                    files=[
                        provider_module.FileChangeInput(
                            path="README.md",
                            content="should not be here",
                            delete=True,
                        )
                    ],
                    branch="feature",
                    base_branch="main",
                    installation_id=99,
                ),
                github_request(),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertIn("delete cannot include content", response.body["error"])
        urlopen.assert_not_called()

    def test_bot_operations_require_matching_installation_subject(self) -> None:
        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.bot_open_pull_request(
                provider_module.OpenPullRequestInput(
                    owner="acme",
                    repo="widgets",
                    title="Update README",
                    head="feature",
                    base="main",
                    installation_id=100,
                ),
                github_request(installation_id=99),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertIn("installation_id", response.body["error"])
        urlopen.assert_not_called()

        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.bot_open_pull_request(
                provider_module.OpenPullRequestInput(
                    owner="acme",
                    repo="other",
                    title="Update README",
                    head="feature",
                    base="main",
                ),
                github_request(installation_id=99, repo="acme/widgets"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertIn("repository", response.body["error"])
        urlopen.assert_not_called()

        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.bot_create_issue_comment(
                provider_module.CreateIssueCommentInput(
                    owner="acme",
                    repo="other",
                    issue_number=7,
                    body="Looks broken.",
                ),
                github_request(installation_id=99, repo="acme/widgets"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertIn("repository", response.body["error"])
        urlopen.assert_not_called()

    def test_webhook_handler_filters_unsupported_and_configured_bot_events(
        self,
    ) -> None:
        push_payload = {
            "ref": "refs/heads/feature",
            "commits": [],
            "installation": {"id": 99},
            "repository": {"full_name": "acme/widgets"},
            "sender": {"login": "octocat"},
        }
        bot_payload = {
            "action": "opened",
            "installation": {"id": 99},
            "repository": {"full_name": "acme/widgets"},
            "pull_request": {"number": 7},
            "sender": {"login": "example-app[bot]"},
        }

        with (
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                side_effect=AssertionError("workflow manager should not be called"),
                create=True,
            ),
            mock.patch(
                "internals.webhook.bot_identity",
                return_value=GitHubBotIdentity(
                    name="Example App Bot",
                    login="example-app[bot]",
                    user_id="12345678",
                    email="12345678+example-app[bot]@users.noreply.github.com",
                ),
            ),
        ):
            push_result = provider_module.github_events_handle(
                push_payload, gestalt.Request()
            )
            bot_result = provider_module.github_events_handle(
                bot_payload, gestalt.Request()
            )

        self.assertEqual(
            push_result, {"ok": True, "ignored": "unsupported_event_type:push"}
        )
        self.assertEqual(bot_result, {"ok": True, "ignored": "configured_bot_sender"})

    def test_webhook_handler_ignores_bot_sender_when_identity_derivation_fails(
        self,
    ) -> None:
        payload = {
            "action": "opened",
            "installation": {"id": 99},
            "repository": {"full_name": "acme/widgets"},
            "pull_request": {"number": 7},
            "sender": {"login": "example-app[bot]"},
        }

        with (
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                side_effect=AssertionError("workflow manager should not be called"),
                create=True,
            ),
            mock.patch(
                "internals.webhook.bot_identity",
                side_effect=GitHubAPIError(502, "unavailable"),
            ),
        ):
            result = provider_module.github_events_handle(payload, gestalt.Request())

        self.assertEqual(result, {"ok": True, "ignored": "unresolved_bot_sender"})


if __name__ == "__main__":
    unittest.main()
