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
from google.protobuf import json_format
from gestalt.gen.v1 import agent_pb2 as _agent_pb2
from gestalt.gen.v1 import workflow_pb2 as _workflow_pb2
import yaml

import internals.client as client_module
import internals.operations as operations_module
from internals.config import GitHubBotIdentity
from internals.errors import GitHubAPIError
import provider as provider_module

agent_pb2: Any = _agent_pb2
workflow_pb2: Any = _workflow_pb2


class FakeHTTPResponse:
    def __init__(self, body: dict[str, Any] | None = None) -> None:
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
            id=f"workload:github_app_installation:{installation_id}:repo:{repo}",
            kind="workload",
            display_name=f"GitHub App installation {installation_id}",
            auth_source="github_app_webhook",
        )
    )


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
                    "providerOptions": {"temperature": 0},
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
        self.assertEqual(security["type"], "hmac")
        self.assertEqual(security["secret"]["env"], "GITHUB_WEBHOOK_SECRET")
        self.assertEqual(security["signatureHeader"], "X-Hub-Signature-256")
        self.assertEqual(security["signaturePrefix"], "sha256=")
        self.assertEqual(security["payloadTemplate"], "{raw_body}")

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

    def test_resolve_http_subject_maps_installation_to_workload(self) -> None:
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
            subject.id, "workload:github_app_installation:99:repo:acme/widgets"
        )
        self.assertEqual(subject.kind, "workload")
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
                "head": {"ref": "feature"},
                "base": {"ref": "main"},
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
        self.assertEqual(
            agent.tool_source, agent_pb2.AGENT_TOOL_SOURCE_MODE_NATIVE_SEARCH
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
        self.assertIn("GitHub App webhook", data["user_prompt"])
        self.assertEqual(
            data["payload"]["headers"]["X-Hub-Signature-256"], "[redacted]"
        )

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

        result = provider_module.github_events_handle(payload, gestalt.Request())

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.SERVICE_UNAVAILABLE)
        self.assertIn("failed to dispatch workflow run", response.body["error"])

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
            result = provider_module.github_events_handle(payload, gestalt.Request())

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.SERVICE_UNAVAILABLE)
        self.assertIn("workflow manager unavailable", response.body["error"])

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
