from __future__ import annotations

import io
import json
import pathlib
import unittest
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
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
import internals.review as review_module
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


class FakeAgentManager:
    def __init__(self, findings: list[dict[str, Any]]) -> None:
        self.findings = findings
        self.sessions: list[Any] = []
        self.turns: list[Any] = []

    def create_session(self, request: Any) -> Any:
        self.sessions.append(request)
        return agent_pb2.AgentSession(id="agent-session-1")

    def create_turn(self, request: Any) -> Any:
        self.turns.append(request)
        turn = agent_pb2.AgentTurn(
            id="agent-turn-1",
            session_id=request.session_id,
            status=agent_pb2.AGENT_EXECUTION_STATUS_SUCCEEDED,
        )
        turn.structured_output.update({"findings": self.findings})
        return turn

    def get_turn(self, request: Any) -> Any:
        raise AssertionError(f"unexpected get_turn call for {request.turn_id}")


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
        self.graphql_requests: list[tuple[str, str | None, dict[str, Any]]] = []
        self.graphql_responder: (
            Callable[[str, str | None, Mapping[str, Any] | None], dict[str, Any]] | None
        ) = None
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

    def graphql_json(
        self,
        query: str,
        token: str | None,
        variables: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.graphql_requests.append((query, token, dict(variables or {})))
        if self.graphql_responder is not None:
            return self.graphql_responder(query, token, variables)
        return {"data": {}}

    def repository_default_branch(self, token: str, owner: str, repo: str) -> str:
        return "main"

    def repository_installation(self, owner: str, repo: str) -> dict[str, Any]:
        self.requests.append(("GET", f"/repos/{owner}/{repo}/installation", None, {}))
        return {"id": 99}

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

    def test_resolve_installation_subject_discovers_and_validates_repo(self) -> None:
        client = RecordingGitHubClient()

        subject = operations_module.resolve_installation_subject(
            operations_module.GitHubResolveInstallationRequest(
                owner="acme", repo="widgets"
            ),
            client=client,
        )

        self.assertEqual(subject.installation_id, 99)
        self.assertEqual(
            subject.subject_id,
            "service_account:github_app_installation:99:repo:acme/widgets",
        )
        self.assertEqual(client.tokens, [(99, ("widgets",), {})])

    def test_bot_resolve_installation_returns_service_account_subject(self) -> None:
        client = RecordingGitHubClient()
        original_client = operations_module.DEFAULT_GITHUB_CLIENT
        operations_module.DEFAULT_GITHUB_CLIENT = client
        try:
            result = provider_module.bot_resolve_installation(
                provider_module.ResolveInstallationInput(owner="acme", repo="widgets")
            )
        finally:
            operations_module.DEFAULT_GITHUB_CLIENT = original_client

        installation = result["data"]["installation"]
        self.assertEqual(installation["installation_id"], 99)
        self.assertEqual(installation["repository"], "acme/widgets")
        self.assertEqual(
            installation["subject"]["id"],
            "service_account:github_app_installation:99:repo:acme/widgets",
        )

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

    def test_comment_operations_are_declared_in_catalog_and_policy_schema(
        self,
    ) -> None:
        plugin_root = pathlib.Path(__file__).resolve().parents[1]
        catalog = yaml.safe_load((plugin_root / "catalog.yaml").read_text())
        operations = {operation["id"]: operation for operation in catalog["operations"]}

        event = operations[provider_module.GITHUB_EVENT_OPERATION]
        review = operations[provider_module.REVIEW_PULL_REQUEST_OPERATION]
        installation = operations[provider_module.BOT_RESOLVE_INSTALLATION_OPERATION]
        pr = operations[provider_module.BOT_GET_PULL_REQUEST_OPERATION]
        pr_files = operations[provider_module.BOT_LIST_PULL_REQUEST_FILES_OPERATION]
        pr_review = operations[provider_module.BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION]
        close_pr = operations[provider_module.BOT_CLOSE_PULL_REQUEST_OPERATION]
        pr_threads = operations[
            provider_module.BOT_LIST_PULL_REQUEST_REVIEW_THREADS_OPERATION
        ]
        resolve_thread = operations[
            provider_module.BOT_RESOLVE_PULL_REQUEST_REVIEW_THREAD_OPERATION
        ]
        pr_comment = operations[
            provider_module.BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION
        ]
        issue_comment = operations[provider_module.BOT_CREATE_ISSUE_COMMENT_OPERATION]
        reaction = operations[provider_module.BOT_ADD_REACTION_OPERATION]
        add_labels = operations[provider_module.BOT_ADD_LABELS_OPERATION]
        remove_labels = operations[provider_module.BOT_REMOVE_LABELS_OPERATION]
        request_reviewers = operations[provider_module.BOT_REQUEST_REVIEWERS_OPERATION]
        self.assertIn("workflow targets", event["description"])
        self.assertIn("pull_request workflow signal", review["description"])
        self.assertIn("installation", installation["description"])
        self.assertIn("pull request metadata", pr["description"])
        self.assertIn("changed files", pr_files["description"])
        self.assertIn("inline comments", pr_review["description"])
        self.assertIn("Close", close_pr["description"])
        self.assertIn("review threads", pr_threads["description"])
        self.assertIn("Resolve", resolve_thread["description"])
        self.assertIn("pull request conversation", pr_comment["description"])
        self.assertIn("issue comment", issue_comment["description"])
        self.assertIn("reaction", reaction["description"])
        self.assertIn("labels", add_labels["description"])
        self.assertIn("labels", remove_labels["description"])
        self.assertIn("reviewers", request_reviewers["description"])
        self.assertIn(
            "pull_number", [parameter["name"] for parameter in pr["parameters"]]
        )
        self.assertIn(
            "per_page",
            [parameter["name"] for parameter in pr_files["parameters"]],
        )
        self.assertIn(
            "page",
            [parameter["name"] for parameter in pr_files["parameters"]],
        )
        self.assertIn(
            "comments",
            [parameter["name"] for parameter in pr_review["parameters"]],
        )
        self.assertIn(
            "autoResolveStaleFindings",
            [parameter["name"] for parameter in review["parameters"]],
        )
        self.assertIn(
            "comments_first",
            [parameter["name"] for parameter in pr_threads["parameters"]],
        )
        self.assertIn(
            "thread_id",
            [parameter["name"] for parameter in resolve_thread["parameters"]],
        )
        self.assertIn(
            "pull_number",
            [parameter["name"] for parameter in pr_comment["parameters"]],
        )
        self.assertNotIn(
            "issue_number",
            [parameter["name"] for parameter in pr_comment["parameters"]],
        )
        self.assertIn(
            "issue_number",
            [parameter["name"] for parameter in issue_comment["parameters"]],
        )
        self.assertIn(
            "subject_type",
            [parameter["name"] for parameter in reaction["parameters"]],
        )
        self.assertIn(
            "labels",
            [parameter["name"] for parameter in add_labels["parameters"]],
        )
        self.assertIn(
            "pull_number",
            [parameter["name"] for parameter in remove_labels["parameters"]],
        )
        self.assertIn(
            "team_reviewers",
            [parameter["name"] for parameter in request_reviewers["parameters"]],
        )

        schema = yaml.safe_load(
            (plugin_root / "schemas" / "config.schema.yaml").read_text()
        )
        enum = schema["properties"]["webhookPolicies"]["items"]["properties"]["action"][
            "properties"
        ]["allowedOperations"]["items"]["enum"]
        self.assertIn(provider_module.BOT_GET_PULL_REQUEST_OPERATION, enum)
        self.assertIn(provider_module.BOT_LIST_PULL_REQUEST_FILES_OPERATION, enum)
        self.assertIn(
            provider_module.BOT_LIST_PULL_REQUEST_REVIEW_THREADS_OPERATION, enum
        )
        self.assertIn(provider_module.BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION, enum)
        self.assertIn(
            provider_module.BOT_RESOLVE_PULL_REQUEST_REVIEW_THREAD_OPERATION, enum
        )
        self.assertIn(
            provider_module.BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
            enum,
        )
        self.assertIn(provider_module.BOT_CREATE_ISSUE_COMMENT_OPERATION, enum)
        self.assertIn(provider_module.BOT_ADD_REACTION_OPERATION, enum)
        self.assertIn(provider_module.BOT_ADD_LABELS_OPERATION, enum)
        self.assertIn(provider_module.BOT_REMOVE_LABELS_OPERATION, enum)
        self.assertIn(provider_module.BOT_REQUEST_REVIEWERS_OPERATION, enum)
        self.assertIn(provider_module.BOT_RESOLVE_INSTALLATION_OPERATION, enum)
        self.assertIn(provider_module.BOT_CLOSE_PULL_REQUEST_OPERATION, enum)
        workflow_schema = schema["properties"]["webhookPolicies"]["items"][
            "properties"
        ]["workflow"]["properties"]
        plugin_target_schema = workflow_schema["target"]["properties"]["plugin"]
        self.assertEqual(plugin_target_schema["required"], ["plugin", "operation"])
        self.assertIn("plugin", plugin_target_schema["properties"])
        self.assertIn("operation", plugin_target_schema["properties"])
        self.assertIn("connection", plugin_target_schema["properties"])
        self.assertIn("instance", plugin_target_schema["properties"])
        self.assertEqual(plugin_target_schema["properties"]["input"]["type"], "object")
        self.assertNotIn("pluginName", plugin_target_schema["properties"])

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

    def test_graphql_json_derives_enterprise_url_and_posts_payload(self) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "apiBaseUrl": "https://ghe.example/api/v3",
            },
        )
        calls: list[urllib.request.Request] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            calls.append(request)
            return FakeHTTPResponse({"data": {"viewer": {"login": "octocat"}}})

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            response = client_module.graphql_json(
                "query Example($name: String!) { viewer { login } }",
                "installation-token",
                {"name": "widgets"},
            )

        self.assertEqual(response["data"]["viewer"]["login"], "octocat")
        self.assertEqual(len(calls), 1)
        request = calls[0]
        self.assertEqual(request.full_url, "https://ghe.example/api/graphql")
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(auth_header(request), "Bearer installation-token")
        self.assertEqual(
            request_json(request),
            {
                "query": "query Example($name: String!) { viewer { login } }",
                "variables": {"name": "widgets"},
            },
        )

    def test_graphql_json_maps_graphql_errors_to_github_api_error(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            return FakeHTTPResponse(
                {
                    "data": None,
                    "errors": [
                        {
                            "type": "FORBIDDEN",
                            "message": "Resource not accessible by integration",
                        }
                    ],
                }
            )

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            with self.assertRaises(GitHubAPIError) as raised:
                client_module.graphql_json("query { viewer { login } }", "token")

        self.assertEqual(raised.exception.status, HTTPStatus.FORBIDDEN)
        self.assertIn("Resource not accessible", raised.exception.message)

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
        self.assertNotIn("bot.createPullRequestReview", agent.messages[0].text)
        self.assertNotIn(
            "bot.createPullRequestConversationComment", agent.messages[0].text
        )
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
        self.assertIn("bot.getPullRequest", agent.messages[0].text)
        self.assertIn("bot.listPullRequestFiles", agent.messages[0].text)
        self.assertIn("bot.listPullRequestReviewThreads", agent.messages[0].text)
        self.assertIn("bot.createPullRequestReview", agent.messages[0].text)
        self.assertIn(
            "bot.createPullRequestConversationComment", agent.messages[0].text
        )
        self.assertEqual(
            [tool.operation for tool in agent.tool_refs],
            [
                provider_module.BOT_GET_PULL_REQUEST_OPERATION,
                provider_module.BOT_LIST_PULL_REQUEST_FILES_OPERATION,
                provider_module.BOT_LIST_PULL_REQUEST_REVIEW_THREADS_OPERATION,
                provider_module.BOT_GET_CHECK_RUN_OPERATION,
                provider_module.BOT_LIST_CHECK_RUN_ANNOTATIONS_OPERATION,
                provider_module.BOT_GET_WORKFLOW_RUN_OPERATION,
                provider_module.BOT_LIST_WORKFLOW_RUN_JOBS_OPERATION,
                provider_module.BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
                provider_module.BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
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
        self.assertIn(
            "bot.createPullRequestConversationComment",
            data["agent_request"]["user_prompt"],
        )

    def test_explicit_policy_can_dispatch_to_configured_plugin_workflow_target(
        self,
    ) -> None:
        payload = {
            "action": "synchronize",
            "installation": {"id": 99},
            "repository": {
                "full_name": "acme/widgets",
                "name": "widgets",
                "owner": {"login": "acme"},
            },
            "pull_request": {
                "number": 7,
                "title": "Fix widgets",
                "state": "open",
                "html_url": "https://github.com/acme/widgets/pull/7",
                "head": {"ref": "feature", "sha": "abc123"},
                "base": {"ref": "main", "sha": "def456"},
            },
            "headers": {
                "X-GitHub-Event": "pull_request",
                "X-GitHub-Delivery": "delivery-pr-review",
            },
            "sender": {"login": "octocat"},
        }

        def configure_policy(workflow_config: dict[str, Any]) -> None:
            provider_module.configure(
                "github",
                {
                    "appId": "12345",
                    "appPrivateKey": "unused-in-tests",
                    "workflow": {"provider": "local"},
                    "agent": {"provider": "simple", "model": "fallback"},
                    "webhookPolicies": [
                        {
                            "id": "pr-review",
                            "match": {
                                "events": ["pull_request"],
                                "actions": ["synchronize"],
                            },
                            "workflow": workflow_config,
                        }
                    ],
                },
            )

        configure_policy({"provider": "temporal"})
        agent_request = self._workflow_signal_request(payload)

        configure_policy(
            {
                "provider": "temporal",
                "target": {
                    "plugin": {
                        "plugin": "github",
                        "operation": "reviewPullRequest",
                        "connection": "review-bot",
                        "instance": "prod",
                        "input": {
                            "maxComments": 10,
                            "changedLinesOnly": True,
                        },
                    }
                },
            }
        )
        plugin_request = self._workflow_signal_request(payload)

        self.assertEqual(plugin_request.provider_name, agent_request.provider_name)
        self.assertEqual(plugin_request.workflow_key, agent_request.workflow_key)
        self.assertEqual(plugin_request.idempotency_key, agent_request.idempotency_key)
        self.assertEqual(plugin_request.signal.name, agent_request.signal.name)
        self.assertEqual(
            plugin_request.signal.idempotency_key,
            agent_request.signal.idempotency_key,
        )
        self.assertEqual(
            json_format.MessageToDict(plugin_request.signal.payload),
            json_format.MessageToDict(agent_request.signal.payload),
        )
        self.assertEqual(
            json_format.MessageToDict(plugin_request.signal.metadata),
            json_format.MessageToDict(agent_request.signal.metadata),
        )
        self.assertEqual(agent_request.target.WhichOneof("kind"), "agent")
        self.assertEqual(plugin_request.target.WhichOneof("kind"), "plugin")

        plugin = plugin_request.target.plugin
        self.assertEqual(plugin.plugin_name, "github")
        self.assertEqual(plugin.operation, "reviewPullRequest")
        self.assertEqual(plugin.connection, "review-bot")
        self.assertEqual(plugin.instance, "prod")

        target_input = json_format.MessageToDict(plugin.input)
        self.assertEqual(target_input["maxComments"], 10)
        self.assertEqual(target_input["changedLinesOnly"], True)
        self.assertNotIn("pull_request", target_input)
        self.assertNotIn("repository", target_input)

        signal_payload = json_format.MessageToDict(plugin_request.signal.payload)
        self.assertEqual(signal_payload["repository"]["full_name"], "acme/widgets")
        self.assertEqual(signal_payload["agent_request"]["pull_request"]["number"], 7)

    def test_review_pull_request_posts_validated_inline_comments(self) -> None:
        agent_manager = FakeAgentManager(
            findings=[
                {
                    "path": "src/widget.py",
                    "line": 2,
                    "body": "This can throw when config is missing.",
                    "severity": "high",
                },
                {
                    "path": "src/widget.py",
                    "line": 1,
                    "body": "This is context and cannot receive a RIGHT-side comment.",
                },
            ]
        )
        created_reviews: list[Any] = []

        def fake_create_pull_request_review(request: Any, *, subject: Any) -> Any:
            created_reviews.append((request, subject))
            return {
                "id": 80,
                "state": "COMMENTED",
                "html_url": "https://github.com/acme/widgets/pull/7#pullrequestreview-80",
                "commit_id": "abc123",
                "body": "Automated review found 1 concrete issue.",
                "user": {"login": "example-app[bot]"},
            }

        request = github_request()
        request.workflow = {
            "signals": [
                {
                    "payload": {
                        "github_event": "pull_request",
                        "github_action": "synchronize",
                        "delivery_id": "delivery-pr-review",
                        "installation": {"id": 99},
                        "repository": {"full_name": "acme/widgets"},
                        "summary": {"repository": "acme/widgets", "number": 7},
                        "agent_request": {
                            "pull_request": {
                                "number": 7,
                                "head_sha": "abc123",
                            }
                        },
                    }
                }
            ]
        }

        with (
            mock.patch.object(
                gestalt.Request,
                "agent_manager",
                return_value=agent_manager,
                create=True,
            ),
            mock.patch(
                "internals.review.get_pull_request",
                return_value={
                    "number": 7,
                    "title": "Fix widgets",
                    "state": "open",
                    "html_url": "https://github.com/acme/widgets/pull/7",
                    "head": {"ref": "feature", "sha": "abc123"},
                    "base": {"ref": "main", "sha": "def456"},
                },
            ),
            mock.patch(
                "internals.review.list_pull_request_files",
                return_value=[
                    {
                        "filename": "src/widget.py",
                        "status": "modified",
                        "additions": 1,
                        "deletions": 0,
                        "changes": 1,
                        "patch": "@@ -1,2 +1,3 @@\n context\n+bad = True\n more",
                    }
                ],
            ),
            mock.patch(
                "internals.review.create_pull_request_review",
                side_effect=fake_create_pull_request_review,
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(
                    agentProvider="claude",
                    model="claude-opus-4-7",
                    maxComments=10,
                    changedLinesOnly=True,
                ),
                request,
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["ok"], True)
        self.assertEqual(data["posted"], True)
        self.assertEqual(data["comments"], 1)
        self.assertEqual(data["droppedFindings"], 1)
        self.assertEqual(data["repository"], "acme/widgets")
        self.assertEqual(data["pullNumber"], 7)

        self.assertEqual(len(agent_manager.sessions), 1)
        self.assertEqual(agent_manager.sessions[0].provider_name, "claude")
        self.assertEqual(len(agent_manager.turns), 1)
        prompt = agent_manager.turns[0].messages[1].text
        self.assertIn('"repository": "acme/widgets"', prompt)
        self.assertIn("+bad = True", prompt)
        self.assertTrue(agent_manager.turns[0].response_schema.fields)

        self.assertEqual(len(created_reviews), 1)
        review_request, review_subject = created_reviews[0]
        self.assertEqual(review_subject.id, request.subject.id)
        self.assertEqual(review_request.owner, "acme")
        self.assertEqual(review_request.repo, "widgets")
        self.assertEqual(review_request.pull_number, 7)
        self.assertEqual(review_request.installation_id, 99)
        self.assertEqual(review_request.commit_id, "abc123")
        self.assertEqual(len(review_request.comments), 1)
        self.assertEqual(review_request.comments[0].path, "src/widget.py")
        self.assertEqual(review_request.comments[0].line, 2)
        self.assertEqual(review_request.comments[0].side, "RIGHT")
        self.assertTrue(
            review_request.comments[0].body.startswith(
                "[high] This can throw when config is missing.\n\n"
            )
        )
        self.assertRegex(
            review_request.comments[0].body,
            r"<!-- gestalt:github-review-finding v1 "
            r"fingerprint=[0-9a-f]{64} source=github\.reviewPullRequest -->$",
        )

    def test_review_pull_request_resolves_stale_marked_bot_thread(self) -> None:
        agent_manager = FakeAgentManager(
            findings=[
                {
                    "path": "src/widget.py",
                    "line": 2,
                    "body": "Current bug still exists.",
                }
            ]
        )
        resolved_requests: list[Any] = []
        list_thread_cursors: list[str] = []
        request = github_request()
        request.workflow = {
            "signals": [
                {
                    "payload": {
                        "github_event": "pull_request",
                        "github_action": "synchronize",
                        "installation": {"id": 99},
                        "repository": {"full_name": "acme/widgets"},
                        "summary": {"repository": "acme/widgets", "number": 7},
                    }
                }
            ]
        }

        def fake_resolve(request: Any, *, subject: Any) -> dict[str, Any]:
            resolved_requests.append((request, subject))
            return {"id": request.thread_id, "isResolved": True}

        def fake_list_threads(request: Any, *, subject: Any) -> dict[str, Any]:
            list_thread_cursors.append(request.after)
            if not request.after:
                return {
                    "threads": [],
                    "pageInfo": {"hasNextPage": True, "endCursor": "cursor-2"},
                }
            return {
                "threads": [
                    {
                        "id": "thread-stale",
                        "isResolved": False,
                        "viewerCanResolve": True,
                        "commentsTruncated": False,
                        "comments": [
                            {
                                "authorLogin": "example-app[bot]",
                                "body": review_module.review_comment_body_with_marker(
                                    "Old bug no longer exists.",
                                    "a" * 64,
                                ),
                            }
                        ],
                    }
                ],
                "pageInfo": {"hasNextPage": False, "endCursor": ""},
            }

        with (
            mock.patch.object(
                gestalt.Request,
                "agent_manager",
                return_value=agent_manager,
                create=True,
            ),
            mock.patch(
                "internals.review.get_pull_request",
                return_value={
                    "number": 7,
                    "head": {"ref": "feature", "sha": "abc123"},
                    "base": {"ref": "main", "sha": "def456"},
                },
            ),
            mock.patch(
                "internals.review.list_pull_request_files",
                return_value=[
                    {
                        "filename": "src/widget.py",
                        "status": "modified",
                        "patch": "@@ -1 +1,2 @@\n context\n+bad = True",
                    }
                ],
            ),
            mock.patch(
                "internals.review.create_pull_request_review",
                return_value={"id": 80, "state": "COMMENTED"},
            ),
            mock.patch(
                "internals.review.bot_identity_or_none",
                return_value=GitHubBotIdentity(
                    name="Example App Bot",
                    login="example-app[bot]",
                    user_id="12345678",
                    email="12345678+example-app[bot]@users.noreply.github.com",
                ),
            ),
            mock.patch(
                "internals.review.list_pull_request_review_threads",
                side_effect=fake_list_threads,
            ),
            mock.patch(
                "internals.review.resolve_pull_request_review_thread",
                side_effect=fake_resolve,
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(), request
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["resolvedThreads"], ["thread-stale"])
        self.assertEqual(data["skippedResolutionReasons"], [])
        self.assertEqual(list_thread_cursors, ["", "cursor-2"])
        self.assertEqual(len(resolved_requests), 1)
        resolve_request, resolve_subject = resolved_requests[0]
        self.assertEqual(resolve_request.thread_id, "thread-stale")
        self.assertEqual(resolve_request.installation_id, 99)
        self.assertEqual(resolve_subject.id, request.subject.id)

    def test_review_pull_request_records_resolution_list_failure_after_post(
        self,
    ) -> None:
        agent_manager = FakeAgentManager(
            findings=[
                {
                    "path": "src/widget.py",
                    "line": 2,
                    "body": "Current bug still exists.",
                }
            ]
        )
        request = github_request()
        request.workflow = {
            "signals": [
                {
                    "payload": {
                        "github_event": "pull_request",
                        "github_action": "synchronize",
                        "installation": {"id": 99},
                        "repository": {"full_name": "acme/widgets"},
                        "summary": {"repository": "acme/widgets", "number": 7},
                    }
                }
            ]
        }

        with (
            mock.patch.object(
                gestalt.Request,
                "agent_manager",
                return_value=agent_manager,
                create=True,
            ),
            mock.patch(
                "internals.review.get_pull_request",
                return_value={
                    "number": 7,
                    "head": {"ref": "feature", "sha": "abc123"},
                    "base": {"ref": "main", "sha": "def456"},
                },
            ),
            mock.patch(
                "internals.review.list_pull_request_files",
                return_value=[
                    {
                        "filename": "src/widget.py",
                        "status": "modified",
                        "patch": "@@ -1 +1,2 @@\n context\n+bad = True",
                    }
                ],
            ),
            mock.patch(
                "internals.review.create_pull_request_review",
                return_value={"id": 80, "state": "COMMENTED"},
            ),
            mock.patch(
                "internals.review.bot_identity_or_none",
                return_value=GitHubBotIdentity(
                    name="Example App Bot",
                    login="example-app[bot]",
                    user_id="12345678",
                    email="12345678+example-app[bot]@users.noreply.github.com",
                ),
            ),
            mock.patch(
                "internals.review.list_pull_request_review_threads",
                side_effect=GitHubAPIError(502, "GitHub unavailable"),
            ),
            mock.patch(
                "internals.review.resolve_pull_request_review_thread",
                side_effect=AssertionError("threads should not be resolved"),
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(), request
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["posted"], True)
        self.assertEqual(data["resolvedThreads"], [])
        self.assertEqual(
            data["skippedResolutionReasons"],
            [{"threadId": "", "reason": "list_failed: GitHub unavailable"}],
        )

    def test_review_pull_request_skips_current_and_human_replied_threads(
        self,
    ) -> None:
        agent_manager = FakeAgentManager(
            findings=[
                {
                    "path": "src/widget.py",
                    "line": 2,
                    "body": "Current bug still exists.",
                }
            ]
        )
        subject = review_module.PullRequestSubject(
            owner="acme",
            repo="widgets",
            repository="acme/widgets",
            pull_number=7,
            installation_id=99,
        )
        current_fingerprint = review_module.review_finding_fingerprint(
            subject,
            review_module.ValidatedFinding(
                path="src/widget.py",
                line=2,
                body="Current bug still exists.",
            ),
        )
        request = github_request()
        request.workflow = {
            "signals": [
                {
                    "payload": {
                        "github_event": "pull_request",
                        "github_action": "synchronize",
                        "installation": {"id": 99},
                        "repository": {"full_name": "acme/widgets"},
                        "summary": {"repository": "acme/widgets", "number": 7},
                    }
                }
            ]
        }

        with (
            mock.patch.object(
                gestalt.Request,
                "agent_manager",
                return_value=agent_manager,
                create=True,
            ),
            mock.patch(
                "internals.review.get_pull_request",
                return_value={
                    "number": 7,
                    "head": {"ref": "feature", "sha": "abc123"},
                    "base": {"ref": "main", "sha": "def456"},
                },
            ),
            mock.patch(
                "internals.review.list_pull_request_files",
                return_value=[
                    {
                        "filename": "src/widget.py",
                        "status": "modified",
                        "patch": "@@ -1 +1,2 @@\n context\n+bad = True",
                    }
                ],
            ),
            mock.patch(
                "internals.review.create_pull_request_review",
                return_value={"id": 80, "state": "COMMENTED"},
            ),
            mock.patch(
                "internals.review.bot_identity_or_none",
                return_value=GitHubBotIdentity(
                    name="Example App Bot",
                    login="example-app[bot]",
                    user_id="12345678",
                    email="12345678+example-app[bot]@users.noreply.github.com",
                ),
            ),
            mock.patch(
                "internals.review.list_pull_request_review_threads",
                return_value={
                    "threads": [
                        {
                            "id": "thread-current",
                            "isResolved": False,
                            "viewerCanResolve": True,
                            "commentsTruncated": False,
                            "comments": [
                                {
                                    "authorLogin": "example-app[bot]",
                                    "body": review_module.review_comment_body_with_marker(
                                        "Current bug still exists.",
                                        current_fingerprint,
                                    ),
                                }
                            ],
                        },
                        {
                            "id": "thread-human",
                            "isResolved": False,
                            "viewerCanResolve": True,
                            "commentsTruncated": False,
                            "comments": [
                                {
                                    "authorLogin": "example-app[bot]",
                                    "body": review_module.review_comment_body_with_marker(
                                        "Old bug",
                                        "b" * 64,
                                    ),
                                },
                                {
                                    "authorLogin": "octocat",
                                    "body": "I still see this.",
                                },
                            ],
                        },
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": ""},
                },
            ),
            mock.patch(
                "internals.review.resolve_pull_request_review_thread",
                side_effect=AssertionError("no thread should be resolved"),
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(), request
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["resolvedThreads"], [])
        self.assertEqual(
            data["skippedResolutionReasons"],
            [
                {"threadId": "thread-current", "reason": "current_finding"},
                {"threadId": "thread-human", "reason": "human_reply"},
            ],
        )

    def test_review_pull_request_drops_unanchored_findings_without_posting(
        self,
    ) -> None:
        agent_manager = FakeAgentManager(
            findings=[
                {
                    "path": "src/widget.py",
                    "line": 1,
                    "body": "This is context and cannot receive a RIGHT-side comment.",
                }
            ]
        )
        resolved_requests: list[Any] = []
        request = github_request()
        request.workflow = {
            "signals": [
                {
                    "payload": {
                        "github_event": "pull_request",
                        "github_action": "opened",
                        "installation": {"id": 99},
                        "repository": {"full_name": "acme/widgets"},
                        "summary": {"repository": "acme/widgets", "number": 7},
                    }
                }
            ]
        }

        with (
            mock.patch.object(
                gestalt.Request,
                "agent_manager",
                return_value=agent_manager,
                create=True,
            ),
            mock.patch(
                "internals.review.get_pull_request",
                return_value={
                    "number": 7,
                    "head": {"ref": "feature", "sha": "abc123"},
                    "base": {"ref": "main", "sha": "def456"},
                },
            ),
            mock.patch(
                "internals.review.list_pull_request_files",
                return_value=[
                    {
                        "filename": "src/widget.py",
                        "status": "modified",
                        "patch": "@@ -1 +1,2 @@\n context\n+added = True",
                    }
                ],
            ),
            mock.patch(
                "internals.review.create_pull_request_review",
                side_effect=AssertionError("review should not be posted"),
            ),
            mock.patch(
                "internals.review.bot_identity_or_none",
                return_value=GitHubBotIdentity(
                    name="Example App Bot",
                    login="example-app[bot]",
                    user_id="12345678",
                    email="12345678+example-app[bot]@users.noreply.github.com",
                ),
            ),
            mock.patch(
                "internals.review.list_pull_request_review_threads",
                return_value={
                    "threads": [
                        {
                            "id": "thread-stale",
                            "isResolved": False,
                            "viewerCanResolve": True,
                            "commentsTruncated": False,
                            "comments": [
                                {
                                    "authorLogin": "example-app[bot]",
                                    "body": review_module.review_comment_body_with_marker(
                                        "Old bug",
                                        "c" * 64,
                                    ),
                                }
                            ],
                        }
                    ],
                    "pageInfo": {"hasNextPage": False, "endCursor": ""},
                },
            ),
            mock.patch(
                "internals.review.resolve_pull_request_review_thread",
                side_effect=lambda request, *, subject: (
                    resolved_requests.append(request)
                    or {"id": request.thread_id, "isResolved": True}
                ),
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(), request
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["posted"], False)
        self.assertEqual(data["comments"], 0)
        self.assertEqual(data["reason"], "no_valid_findings")
        self.assertEqual(data["droppedFindings"], 1)
        self.assertEqual(data["resolvedThreads"], ["thread-stale"])
        self.assertEqual(len(resolved_requests), 1)

    def test_review_pull_request_dry_run_does_not_post_or_resolve(self) -> None:
        agent_manager = FakeAgentManager(
            findings=[
                {
                    "path": "src/widget.py",
                    "line": 2,
                    "body": "Dry run finding.",
                }
            ]
        )
        request = github_request()
        request.workflow = {
            "signals": [
                {
                    "payload": {
                        "github_event": "pull_request",
                        "github_action": "opened",
                        "installation": {"id": 99},
                        "repository": {"full_name": "acme/widgets"},
                        "summary": {"repository": "acme/widgets", "number": 7},
                    }
                }
            ]
        }

        with (
            mock.patch.object(
                gestalt.Request,
                "agent_manager",
                return_value=agent_manager,
                create=True,
            ),
            mock.patch(
                "internals.review.get_pull_request",
                return_value={
                    "number": 7,
                    "head": {"ref": "feature", "sha": "abc123"},
                    "base": {"ref": "main", "sha": "def456"},
                },
            ),
            mock.patch(
                "internals.review.list_pull_request_files",
                return_value=[
                    {
                        "filename": "src/widget.py",
                        "status": "modified",
                        "patch": "@@ -1 +1,2 @@\n context\n+bad = True",
                    }
                ],
            ),
            mock.patch(
                "internals.review.create_pull_request_review",
                side_effect=AssertionError("review should not be posted"),
            ),
            mock.patch(
                "internals.review.list_pull_request_review_threads",
                side_effect=AssertionError("threads should not be listed"),
            ),
            mock.patch(
                "internals.review.resolve_pull_request_review_thread",
                side_effect=AssertionError("threads should not be resolved"),
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(dryRun=True), request
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["posted"], False)
        self.assertEqual(data["dry_run"], True)
        self.assertEqual(data["comments"], 1)
        self.assertEqual(data["resolvedThreads"], [])

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
        self.assertEqual(
            [tool.operation for tool in request.target.agent.tool_refs],
            [
                provider_module.BOT_GET_PULL_REQUEST_OPERATION,
                provider_module.BOT_LIST_PULL_REQUEST_FILES_OPERATION,
                provider_module.BOT_LIST_PULL_REQUEST_REVIEW_THREADS_OPERATION,
                provider_module.BOT_GET_CHECK_RUN_OPERATION,
                provider_module.BOT_LIST_CHECK_RUN_ANNOTATIONS_OPERATION,
                provider_module.BOT_GET_WORKFLOW_RUN_OPERATION,
                provider_module.BOT_LIST_WORKFLOW_RUN_JOBS_OPERATION,
            ],
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
            (
                {"workflow": "local"},
                "workflow must be an object",
            ),
            (
                {
                    "workflow": {
                        "provider": "local",
                        "target": {
                            "plugin": {
                                "plugin": "github",
                                "operation": "reviewPullRequest",
                            }
                        },
                    }
                },
                "workflow.target is not supported",
            ),
            (
                {"webhookPolicies": [{"id": "bad-workflow", "workflow": "nope"}]},
                "workflow must be an object",
            ),
            (
                {
                    "webhookPolicies": [
                        {"id": "bad-target", "workflow": {"target": "plugin"}}
                    ]
                },
                "workflow.target must be an object",
            ),
            (
                {
                    "webhookPolicies": [
                        {"id": "missing-plugin", "workflow": {"target": {}}}
                    ]
                },
                "workflow.target.plugin is required",
            ),
            (
                {
                    "webhookPolicies": [
                        {
                            "id": "bad-plugin-target",
                            "workflow": {"target": {"plugin": "github"}},
                        }
                    ]
                },
                "workflow.target.plugin must be an object",
            ),
            (
                {
                    "webhookPolicies": [
                        {
                            "id": "missing-plugin-name",
                            "workflow": {
                                "target": {"plugin": {"operation": "reviewPullRequest"}}
                            },
                        }
                    ]
                },
                "workflow.target.plugin.plugin is required",
            ),
            (
                {
                    "webhookPolicies": [
                        {
                            "id": "missing-plugin-operation",
                            "workflow": {"target": {"plugin": {"plugin": "github"}}},
                        }
                    ]
                },
                "workflow.target.plugin.operation is required",
            ),
            (
                {
                    "webhookPolicies": [
                        {
                            "id": "bad-plugin-input",
                            "workflow": {
                                "target": {
                                    "plugin": {
                                        "plugin": "github",
                                        "operation": "reviewPullRequest",
                                        "input": "bad",
                                    }
                                }
                            },
                        }
                    ]
                },
                "workflow.target.plugin.input must be an object",
            ),
            (
                {
                    "webhookPolicies": [
                        {
                            "id": "non-json-plugin-input",
                            "workflow": {
                                "target": {
                                    "plugin": {
                                        "plugin": "github",
                                        "operation": "reviewPullRequest",
                                        "input": {"bad": object()},
                                    }
                                }
                            },
                        }
                    ]
                },
                "workflow.target.plugin.input must be JSON-compatible",
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
                                provider_module.BOT_REQUEST_REVIEWERS_OPERATION,
                                provider_module.BOT_ADD_REACTION_OPERATION,
                                provider_module.BOT_RESOLVE_PULL_REQUEST_REVIEW_THREAD_OPERATION,
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
                provider_module.BOT_RESOLVE_PULL_REQUEST_REVIEW_THREAD_OPERATION,
                provider_module.BOT_ADD_REACTION_OPERATION,
                provider_module.BOT_REQUEST_REVIEWERS_OPERATION,
                provider_module.BOT_CREATE_PULL_REQUEST_OPERATION,
            ],
        )

    def test_new_mutation_operations_are_explicit_policy_opt_ins(self) -> None:
        new_operations = {
            provider_module.BOT_ADD_REACTION_OPERATION,
            provider_module.BOT_ADD_LABELS_OPERATION,
            provider_module.BOT_REMOVE_LABELS_OPERATION,
            provider_module.BOT_REQUEST_REVIEWERS_OPERATION,
        }
        pull_request_payload = {
            "action": "opened",
            "installation": {"id": 99},
            "repository": {"full_name": "acme/widgets"},
            "pull_request": {"number": 7},
            "headers": {"X-GitHub-Event": "pull_request"},
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
                    {
                        "id": "pull-defaults",
                        "action": {"mode": "pull_request"},
                    }
                ],
            },
        )
        defaults = self._workflow_signal_request(pull_request_payload)
        default_operations = [
            tool.operation for tool in defaults.target.agent.tool_refs
        ]
        self.assertFalse(new_operations.intersection(default_operations))

        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {
                        "id": "explicit-new-mutations",
                        "action": {
                            "allowedOperations": [
                                provider_module.BOT_REQUEST_REVIEWERS_OPERATION,
                                provider_module.BOT_REMOVE_LABELS_OPERATION,
                                provider_module.BOT_ADD_LABELS_OPERATION,
                                provider_module.BOT_ADD_REACTION_OPERATION,
                            ]
                        },
                    }
                ],
            },
        )
        explicit = self._workflow_signal_request(pull_request_payload)
        agent = explicit.target.agent
        self.assertEqual(
            [tool.operation for tool in agent.tool_refs],
            [
                provider_module.BOT_ADD_REACTION_OPERATION,
                provider_module.BOT_ADD_LABELS_OPERATION,
                provider_module.BOT_REMOVE_LABELS_OPERATION,
                provider_module.BOT_REQUEST_REVIEWERS_OPERATION,
            ],
        )
        self.assertIn("bot.addReaction", agent.messages[0].text)
        self.assertIn("bot.addLabels", agent.messages[0].text)
        self.assertIn("bot.removeLabels", agent.messages[0].text)
        self.assertIn("bot.requestReviewers", agent.messages[0].text)

        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {
                        "id": "add-label-only",
                        "action": {
                            "allowedOperations": [
                                provider_module.BOT_ADD_REACTION_OPERATION,
                                provider_module.BOT_ADD_LABELS_OPERATION,
                            ]
                        },
                    }
                ],
            },
        )
        add_only = self._workflow_signal_request(pull_request_payload)
        add_only_text = add_only.target.agent.messages[0].text
        self.assertIn("pull requests", add_only_text)
        self.assertIn("bot.addLabels", add_only_text)
        self.assertNotIn("bot.removeLabels", add_only_text)

    def test_explicit_pr_review_policy_exposes_diff_and_review_tools(self) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {
                        "id": "pr-review",
                        "match": {
                            "events": ["pull_request"],
                            "actions": ["opened", "synchronize", "reopened"],
                        },
                        "action": {
                            "mode": "comment",
                            "allowedOperations": [
                                "bot.getPullRequest",
                                "bot.listPullRequestFiles",
                                "bot.createPullRequestReview",
                                "bot.createPullRequestConversationComment",
                            ],
                        },
                    }
                ],
            },
        )

        request = self._workflow_signal_request(
            {
                "action": "synchronize",
                "installation": {"id": 99},
                "repository": {"full_name": "acme/widgets"},
                "pull_request": {
                    "number": 7,
                    "title": "Fix widgets",
                    "state": "open",
                    "html_url": "https://github.com/acme/widgets/pull/7",
                    "head": {"ref": "feature", "sha": "abc123"},
                    "base": {"ref": "main", "sha": "def456"},
                },
                "headers": {"X-GitHub-Event": "pull_request"},
                "sender": {"login": "octocat"},
            }
        )

        self.assertEqual(
            request.workflow_key,
            "github:99:acme/widgets:7:policy:pr-review",
        )
        agent = request.target.agent
        self.assertIn(
            "inspect pull request metadata and diff patches",
            agent.messages[0].text,
        )
        self.assertEqual(
            [tool.operation for tool in agent.tool_refs],
            [
                provider_module.BOT_GET_PULL_REQUEST_OPERATION,
                provider_module.BOT_LIST_PULL_REQUEST_FILES_OPERATION,
                provider_module.BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
                provider_module.BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
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

        pr_issue_comment = self._workflow_signal_payload(
            {
                **base,
                "action": "created",
                "issue": {
                    "number": 9,
                    "title": "Refactor widgets",
                    "state": "open",
                    "html_url": "https://github.com/acme/widgets/pull/9",
                    "pull_request": {
                        "url": "https://api.github.com/repos/acme/widgets/pulls/9",
                        "html_url": "https://github.com/acme/widgets/pull/9",
                    },
                },
                "comment": {
                    "id": 112,
                    "html_url": "https://github.com/acme/widgets/pull/9#issuecomment-112",
                    "body": "please check this PR",
                    "user": {"login": "octocat"},
                },
                "headers": {"X-GitHub-Event": "issue_comment"},
            }
        )
        self.assertEqual(pr_issue_comment["github_event"], "issue_comment")
        self.assertEqual(pr_issue_comment["summary"]["pull_request_numbers"], [9])
        self.assertTrue(pr_issue_comment["agent_request"]["issue"]["is_pull_request"])
        self.assertEqual(pr_issue_comment["agent_request"]["pull_request"]["number"], 9)
        self.assertIn(
            "pull_request_numbers: [9]",
            pr_issue_comment["agent_request"]["user_prompt"],
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

    def test_close_pull_request_uses_pull_request_write_permission(self) -> None:
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
                self.assertEqual(body["permissions"], {"pull_requests": "write"})
                return FakeHTTPResponse({"token": "pr-token"})
            if path == "/repos/acme/widgets/pulls/7":
                self.assertEqual(method, "PATCH")
                self.assertEqual(auth_header(request), "Bearer pr-token")
                self.assertEqual(body, {"state": "closed"})
                return FakeHTTPResponse(
                    {
                        "number": 7,
                        "title": "Update README",
                        "state": "closed",
                        "html_url": "https://github.com/acme/widgets/pull/7",
                        "url": "https://api.github.com/repos/acme/widgets/pulls/7",
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
            result = provider_module.bot_close_pull_request(
                provider_module.ClosePullRequestInput(
                    owner="acme",
                    repo="widgets",
                    pull_number=7,
                ),
                github_request(),
            )

        data = cast(dict[str, Any], result)["data"]["pull_request"]
        self.assertEqual(data["number"], 7)
        self.assertEqual(data["state"], "closed")
        self.assertEqual(
            [
                call[2].get("permissions")
                for call in calls
                if call[1].endswith("access_tokens")
            ],
            [{"pull_requests": "write"}],
        )

    def test_create_issue_comment_uses_issue_write_permission(self) -> None:
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
                self.assertEqual(body["permissions"], {"issues": "write"})
                return FakeHTTPResponse({"token": "issue-token"})
            if path == "/repos/acme/widgets/issues/7/comments":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer issue-token")
                self.assertEqual(body, {"body": "I can reproduce this issue."})
                return FakeHTTPResponse(
                    {
                        "id": 123,
                        "node_id": "IC_kw",
                        "url": "https://api.github.com/repos/acme/widgets/issues/comments/123",
                        "html_url": "https://github.com/acme/widgets/issues/7#issuecomment-123",
                        "body": "I can reproduce this issue.",
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
                    body="I can reproduce this issue.",
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
            [{"issues": "write"}],
        )

    def test_create_pull_request_conversation_comment_uses_pull_request_write_permission(
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
                self.assertEqual(body["permissions"], {"pull_requests": "write"})
                return FakeHTTPResponse({"token": "pr-token"})
            if path == "/repos/acme/widgets/issues/7/comments":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer pr-token")
                self.assertEqual(body, {"body": "Likely fix: update the snapshot."})
                return FakeHTTPResponse(
                    {
                        "id": 124,
                        "node_id": "IC_kw2",
                        "url": "https://api.github.com/repos/acme/widgets/issues/comments/124",
                        "html_url": "https://github.com/acme/widgets/pull/7#issuecomment-124",
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
            result = provider_module.bot_create_pull_request_conversation_comment(
                provider_module.CreatePullRequestConversationCommentInput(
                    owner="acme",
                    repo="widgets",
                    pull_number=7,
                    body="Likely fix: update the snapshot.",
                ),
                github_request(),
            )

        data = cast(dict[str, Any], result)["data"]["comment"]
        self.assertEqual(data["id"], 124)
        self.assertEqual(data["user"]["login"], "example-app[bot]")
        self.assertEqual(
            [
                call[2].get("permissions")
                for call in calls
                if call[1].endswith("access_tokens")
            ],
            [{"pull_requests": "write"}],
        )

    def test_create_pull_request_review_posts_inline_comments_with_pr_write_permission(
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
                self.assertEqual(body["permissions"], {"pull_requests": "write"})
                return FakeHTTPResponse({"token": "pr-token"})
            if path == "/repos/acme/widgets/pulls/7/reviews":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer pr-token")
                self.assertEqual(
                    body,
                    {
                        "body": "Bugbot review: I found two concrete issues.",
                        "event": "COMMENT",
                        "comments": [
                            {
                                "path": "src/widget.py",
                                "body": "This branch can throw when config is missing.",
                                "line": 27,
                                "side": "RIGHT",
                            },
                            {
                                "path": "src/widget.py",
                                "body": "This loop skips empty inputs.",
                                "line": 45,
                                "side": "RIGHT",
                                "start_line": 41,
                                "start_side": "RIGHT",
                            },
                            {
                                "path": "README.md",
                                "body": "This legacy diff position still works.",
                                "position": 6,
                            },
                        ],
                        "commit_id": "ecdd80bb57125d7ba9641ffaa4d7d2c19d3f3091",
                    },
                )
                return FakeHTTPResponse(
                    {
                        "id": 80,
                        "node_id": "PRR_kw",
                        "state": "COMMENTED",
                        "html_url": "https://github.com/acme/widgets/pull/7#pullrequestreview-80",
                        "pull_request_url": "https://api.github.com/repos/acme/widgets/pulls/7",
                        "commit_id": "ecdd80bb57125d7ba9641ffaa4d7d2c19d3f3091",
                        "body": "Bugbot review: I found two concrete issues.",
                        "user": {"login": "example-app[bot]"},
                        "submitted_at": "2026-05-01T00:00:00Z",
                    }
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_create_pull_request_review(
                provider_module.CreatePullRequestReviewInput(
                    owner="acme",
                    repo="widgets",
                    pull_number=7,
                    body="Bugbot review: I found two concrete issues.",
                    commit_id="ecdd80bb57125d7ba9641ffaa4d7d2c19d3f3091",
                    comments=[
                        provider_module.PullRequestReviewCommentInput(
                            path="/src/widget.py",
                            body="This branch can throw when config is missing.",
                            line=27,
                            side="right",
                        ),
                        provider_module.PullRequestReviewCommentInput(
                            path="src/widget.py",
                            body="This loop skips empty inputs.",
                            start_line=41,
                            start_side="RIGHT",
                            line=45,
                            side="RIGHT",
                        ),
                        provider_module.PullRequestReviewCommentInput(
                            path="README.md",
                            body="This legacy diff position still works.",
                            position=6,
                        ),
                    ],
                ),
                github_request(),
            )

        data = cast(dict[str, Any], result)["data"]["review"]
        self.assertEqual(data["id"], 80)
        self.assertEqual(data["state"], "COMMENTED")
        self.assertEqual(data["user"]["login"], "example-app[bot]")
        self.assertEqual(
            [
                call[2].get("permissions")
                for call in calls
                if call[1].endswith("access_tokens")
            ],
            [{"pull_requests": "write"}],
        )

    def test_add_reaction_uses_target_specific_permissions(self) -> None:
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
                self.assertEqual(body["repositories"], ["widgets"])
                permissions = body["permissions"]
                if permissions == {"issues": "write"}:
                    return FakeHTTPResponse({"token": "issue-token"})
                if permissions == {"pull_requests": "write"}:
                    return FakeHTTPResponse({"token": "pr-token"})
                self.fail(f"unexpected permissions {permissions}")
            if path == "/repos/acme/widgets/issues/7/reactions":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer issue-token")
                self.assertEqual(body, {"content": "eyes"})
                return FakeHTTPResponse(
                    {
                        "id": 1,
                        "node_id": "REA_1",
                        "content": "eyes",
                        "user": {"login": "example-app[bot]"},
                        "created_at": "2026-05-01T00:00:00Z",
                    }
                )
            if path == "/repos/acme/widgets/issues/8/reactions":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer issue-token")
                self.assertEqual(body, {"content": "heart"})
                return FakeHTTPResponse(
                    {
                        "id": 4,
                        "node_id": "REA_4",
                        "content": "heart",
                        "user": {"login": "example-app[bot]"},
                    }
                )
            if path == "/repos/acme/widgets/issues/comments/124/reactions":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer issue-token")
                self.assertEqual(body, {"content": "rocket"})
                return FakeHTTPResponse(
                    {
                        "id": 2,
                        "node_id": "REA_2",
                        "content": "rocket",
                        "user": {"login": "example-app[bot]"},
                    }
                )
            if path == "/repos/acme/widgets/pulls/comments/333/reactions":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer pr-token")
                self.assertEqual(body, {"content": "+1"})
                return FakeHTTPResponse(
                    {
                        "id": 3,
                        "node_id": "REA_3",
                        "content": "+1",
                        "user": {"login": "example-app[bot]"},
                    }
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            issue = provider_module.bot_add_reaction(
                provider_module.AddReactionInput(
                    owner="acme",
                    repo="widgets",
                    subject_type="issue",
                    issue_number=7,
                    content="eyes",
                ),
                github_request(),
            )
            pull_request = provider_module.bot_add_reaction(
                provider_module.AddReactionInput(
                    owner="acme",
                    repo="widgets",
                    subject_type="pull_request",
                    pull_number=8,
                    content="heart",
                ),
                github_request(),
            )
            issue_comment = provider_module.bot_add_reaction(
                provider_module.AddReactionInput(
                    owner="acme",
                    repo="widgets",
                    subject_type="issue_comment",
                    comment_id=124,
                    content="rocket",
                ),
                github_request(),
            )
            review_comment = provider_module.bot_add_reaction(
                provider_module.AddReactionInput(
                    owner="acme",
                    repo="widgets",
                    subject_type="pull_request_review_comment",
                    comment_id=333,
                    content="+1",
                ),
                github_request(),
            )

        self.assertEqual(cast(dict[str, Any], issue)["data"]["reaction"]["id"], 1)
        self.assertEqual(
            cast(dict[str, Any], pull_request)["data"]["reaction"]["content"],
            "heart",
        )
        self.assertEqual(
            cast(dict[str, Any], issue_comment)["data"]["reaction"]["content"],
            "rocket",
        )
        self.assertEqual(
            cast(dict[str, Any], review_comment)["data"]["reaction"]["user"]["login"],
            "example-app[bot]",
        )
        self.assertEqual(
            [
                call[2]
                for call in calls
                if call[1] == "/app/installations/99/access_tokens"
            ],
            [
                {"repositories": ["widgets"], "permissions": {"issues": "write"}},
                {"repositories": ["widgets"], "permissions": {"issues": "write"}},
                {"repositories": ["widgets"], "permissions": {"issues": "write"}},
                {
                    "repositories": ["widgets"],
                    "permissions": {"pull_requests": "write"},
                },
            ],
        )

    def test_label_operations_use_target_permissions_and_list_responses(self) -> None:
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
                self.assertEqual(body["repositories"], ["widgets"])
                permissions = body["permissions"]
                if permissions == {"pull_requests": "write"}:
                    return FakeHTTPResponse({"token": "pr-token"})
                if permissions == {"issues": "write"}:
                    return FakeHTTPResponse({"token": "issue-token"})
                self.fail(f"unexpected permissions {permissions}")
            if path == "/repos/acme/widgets/issues/7/labels":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer pr-token")
                self.assertEqual(body, {"labels": ["bug", "needs review"]})
                return FakeHTTPResponse(
                    [
                        {
                            "id": 10,
                            "node_id": "LA_kw",
                            "name": "bug",
                            "color": "d73a4a",
                        },
                        {
                            "id": 11,
                            "node_id": "LA_kw2",
                            "name": "needs review",
                            "color": "ededed",
                        },
                    ]
                )
            if path == "/repos/acme/widgets/issues/13/labels/needs%20review%2Ftriage":
                self.assertEqual(method, "DELETE")
                self.assertEqual(auth_header(request), "Bearer issue-token")
                self.assertEqual(body, {})
                return FakeHTTPResponse(
                    [{"id": 10, "node_id": "LA_kw", "name": "bug", "color": "d73a4a"}]
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            added = provider_module.bot_add_labels(
                provider_module.AddLabelsInput(
                    owner="acme",
                    repo="widgets",
                    subject_type="pull_request",
                    pull_number=7,
                    labels=[" bug ", "needs review"],
                ),
                github_request(),
            )
            removed = provider_module.bot_remove_labels(
                provider_module.RemoveLabelsInput(
                    owner="acme",
                    repo="widgets",
                    subject_type="issue",
                    issue_number=13,
                    labels=["needs review/triage"],
                ),
                github_request(),
            )

        self.assertEqual(
            cast(dict[str, Any], added)["data"]["labels"],
            [
                {"id": 10, "node_id": "LA_kw", "name": "bug", "color": "d73a4a"},
                {
                    "id": 11,
                    "node_id": "LA_kw2",
                    "name": "needs review",
                    "color": "ededed",
                },
            ],
        )
        self.assertEqual(
            cast(dict[str, Any], removed)["data"]["removed"], ["needs review/triage"]
        )
        self.assertEqual(
            cast(dict[str, Any], removed)["data"]["labels"],
            [{"id": 10, "node_id": "LA_kw", "name": "bug", "color": "d73a4a"}],
        )
        self.assertEqual(
            [
                call[2]
                for call in calls
                if call[1] == "/app/installations/99/access_tokens"
            ],
            [
                {
                    "repositories": ["widgets"],
                    "permissions": {"pull_requests": "write"},
                },
                {"repositories": ["widgets"], "permissions": {"issues": "write"}},
            ],
        )

    def test_label_operations_reject_malformed_github_label_response(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            path = request_path(request)
            if path == "/app/installations/99/access_tokens":
                return FakeHTTPResponse({"token": "issue-token"})
            if path == "/repos/acme/widgets/issues/7/labels":
                return FakeHTTPResponse({"labels": []})
            self.fail(f"unexpected request {request.get_method()} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_add_labels(
                provider_module.AddLabelsInput(
                    owner="acme",
                    repo="widgets",
                    subject_type="issue",
                    issue_number=7,
                    labels=["bug"],
                ),
                github_request(),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_GATEWAY)
        self.assertIn("GitHub labels response was not a list", response.body["error"])

    def test_request_reviewers_uses_pr_write_permission(self) -> None:
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
                self.assertEqual(
                    body,
                    {
                        "repositories": ["widgets"],
                        "permissions": {"pull_requests": "write"},
                    },
                )
                return FakeHTTPResponse({"token": "pr-token"})
            if path == "/repos/acme/widgets/pulls/7/requested_reviewers":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer pr-token")
                self.assertEqual(
                    body,
                    {"reviewers": ["octocat"], "team_reviewers": ["backend"]},
                )
                return FakeHTTPResponse(
                    {
                        "number": 7,
                        "title": "Fix widgets",
                        "state": "open",
                        "html_url": "https://github.com/acme/widgets/pull/7",
                        "head": {"ref": "feature", "sha": "abc123"},
                        "base": {"ref": "main", "sha": "def456"},
                    }
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_request_reviewers(
                provider_module.RequestReviewersInput(
                    owner="acme",
                    repo="widgets",
                    pull_number=7,
                    reviewers=[" octocat "],
                    team_reviewers=["backend"],
                ),
                github_request(),
            )

        data = cast(dict[str, Any], result)["data"]["pull_request"]
        self.assertEqual(data["number"], 7)
        self.assertEqual(data["head"], "feature")
        self.assertEqual(
            [
                call[2]
                for call in calls
                if call[1] == "/app/installations/99/access_tokens"
            ],
            [
                {
                    "repositories": ["widgets"],
                    "permissions": {"pull_requests": "write"},
                }
            ],
        )

    def test_new_mutation_operations_reject_invalid_inputs_before_github_calls(
        self,
    ) -> None:
        cases: list[tuple[str, Callable[[], Any], str]] = [
            (
                "reaction content",
                lambda: provider_module.bot_add_reaction(
                    provider_module.AddReactionInput(
                        owner="acme",
                        repo="widgets",
                        subject_type="issue",
                        issue_number=7,
                        content="party",
                    ),
                    github_request(),
                ),
                "content must be one of",
            ),
            (
                "reaction subject",
                lambda: provider_module.bot_add_reaction(
                    provider_module.AddReactionInput(
                        owner="acme",
                        repo="widgets",
                        subject_type="discussion",
                        issue_number=7,
                        content="eyes",
                    ),
                    github_request(),
                ),
                "subject_type must be one of",
            ),
            (
                "missing reaction comment id",
                lambda: provider_module.bot_add_reaction(
                    provider_module.AddReactionInput(
                        owner="acme",
                        repo="widgets",
                        subject_type="issue_comment",
                        content="eyes",
                    ),
                    github_request(),
                ),
                "comment_id is required",
            ),
            (
                "empty labels",
                lambda: provider_module.bot_add_labels(
                    provider_module.AddLabelsInput(
                        owner="acme",
                        repo="widgets",
                        subject_type="issue",
                        issue_number=7,
                        labels=[],
                    ),
                    github_request(),
                ),
                "labels must contain at least one value",
            ),
            (
                "duplicate labels",
                lambda: provider_module.bot_add_labels(
                    provider_module.AddLabelsInput(
                        owner="acme",
                        repo="widgets",
                        subject_type="issue",
                        issue_number=7,
                        labels=["bug", " bug "],
                    ),
                    github_request(),
                ),
                "duplicates",
            ),
            (
                "missing pull label target",
                lambda: provider_module.bot_remove_labels(
                    provider_module.RemoveLabelsInput(
                        owner="acme",
                        repo="widgets",
                        subject_type="pull_request",
                        labels=["bug"],
                    ),
                    github_request(),
                ),
                "pull_number is required",
            ),
            (
                "blank reviewer",
                lambda: provider_module.bot_request_reviewers(
                    provider_module.RequestReviewersInput(
                        owner="acme",
                        repo="widgets",
                        pull_number=7,
                        reviewers=[" "],
                    ),
                    github_request(),
                ),
                "reviewers[0] is required",
            ),
            (
                "no reviewers",
                lambda: provider_module.bot_request_reviewers(
                    provider_module.RequestReviewersInput(
                        owner="acme",
                        repo="widgets",
                        pull_number=7,
                    ),
                    github_request(),
                ),
                "reviewers or team_reviewers",
            ),
        ]

        for name, call, expected in cases:
            with self.subTest(name=name):
                with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
                    result = call()
                self.assertIsInstance(result, gestalt.Response)
                response = cast(gestalt.Response[dict[str, str]], result)
                self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
                self.assertIn(expected, response.body["error"])
                urlopen.assert_not_called()

    def test_list_pull_request_review_threads_uses_graphql_pr_read_permission(
        self,
    ) -> None:
        client = RecordingGitHubClient()
        graphql_calls: list[tuple[str, str | None, dict[str, Any]]] = []

        def fake_graphql_json(
            query: str,
            token: str | None,
            variables: Mapping[str, Any] | None = None,
        ) -> dict[str, Any]:
            graphql_calls.append((query, token, dict(variables or {})))
            return {
                "data": {
                    "repository": {
                        "pullRequest": {
                            "reviewThreads": {
                                "pageInfo": {
                                    "hasNextPage": True,
                                    "endCursor": "cursor-2",
                                },
                                "nodes": [
                                    {
                                        "id": "thread-1",
                                        "isResolved": False,
                                        "isOutdated": False,
                                        "viewerCanResolve": True,
                                        "path": "src/widget.py",
                                        "line": 27,
                                        "startLine": None,
                                        "originalLine": 27,
                                        "originalStartLine": None,
                                        "diffSide": "RIGHT",
                                        "startDiffSide": None,
                                        "comments": {
                                            "totalCount": 2,
                                            "nodes": [
                                                {
                                                    "id": "comment-1",
                                                    "databaseId": 123,
                                                    "author": {
                                                        "login": "example-app[bot]"
                                                    },
                                                    "body": "Marked finding",
                                                    "createdAt": "2026-05-01T00:00:00Z",
                                                    "url": "https://github.com/acme/widgets/pull/7#discussion_r123",
                                                }
                                            ],
                                        },
                                    }
                                ],
                            }
                        }
                    }
                }
            }

        client.graphql_responder = fake_graphql_json
        threads = operations_module.list_pull_request_review_threads(
            operations_module.GitHubListPullRequestReviewThreadsRequest(
                owner="acme",
                repo="widgets",
                pull_number=7,
                first=500,
                after="cursor-1",
                comments_first=100,
                installation_id=99,
            ),
            subject=github_request().subject,
            client=client,
        )

        self.assertEqual(
            client.tokens,
            [(99, ("widgets",), {"pull_requests": "read"})],
        )
        self.assertEqual(len(graphql_calls), 1)
        query, token, variables = graphql_calls[0]
        self.assertIn("reviewThreads", query)
        self.assertEqual(token, "token:pull_requests:read")
        self.assertEqual(
            variables,
            {
                "owner": "acme",
                "repo": "widgets",
                "number": 7,
                "first": 100,
                "after": "cursor-1",
                "commentsFirst": 50,
            },
        )
        self.assertEqual(threads["pageInfo"]["hasNextPage"], True)
        self.assertEqual(threads["pageInfo"]["endCursor"], "cursor-2")
        thread = threads["threads"][0]
        self.assertEqual(thread["id"], "thread-1")
        self.assertEqual(thread["line"], 27)
        self.assertEqual(thread["commentsCount"], 2)
        self.assertEqual(thread["commentsTruncated"], True)
        self.assertEqual(thread["comments"][0]["authorLogin"], "example-app[bot]")

    def test_resolve_pull_request_review_thread_verifies_pr_before_mutation(
        self,
    ) -> None:
        client = RecordingGitHubClient()
        graphql_calls: list[tuple[str, dict[str, Any]]] = []
        responses = [
            {
                "data": {
                    "node": {
                        "__typename": "PullRequestReviewThread",
                        "id": "thread-1",
                        "isResolved": False,
                        "pullRequest": {
                            "number": 7,
                            "repository": {
                                "name": "widgets",
                                "owner": {"login": "acme"},
                            },
                        },
                    }
                }
            },
            {
                "data": {
                    "resolveReviewThread": {
                        "thread": {"id": "thread-1", "isResolved": True}
                    }
                }
            },
        ]

        def fake_graphql_json(
            query: str,
            token: str | None,
            variables: Mapping[str, Any] | None = None,
        ) -> dict[str, Any]:
            graphql_calls.append((query, dict(variables or {})))
            return responses.pop(0)

        client.graphql_responder = fake_graphql_json
        thread = operations_module.resolve_pull_request_review_thread(
            operations_module.GitHubResolvePullRequestReviewThreadRequest(
                owner="acme",
                repo="widgets",
                pull_number=7,
                thread_id="thread-1",
                installation_id=99,
            ),
            subject=github_request().subject,
            client=client,
        )

        self.assertEqual(thread, {"id": "thread-1", "isResolved": True})
        self.assertEqual(
            client.tokens,
            [(99, ("widgets",), {"pull_requests": "write"})],
        )
        self.assertIn("node(id: $threadId)", graphql_calls[0][0])
        self.assertIn("resolveReviewThread", graphql_calls[1][0])
        self.assertEqual(graphql_calls[0][1], {"threadId": "thread-1"})
        self.assertEqual(graphql_calls[1][1], {"threadId": "thread-1"})

    def test_resolve_pull_request_review_thread_rejects_mismatched_thread(
        self,
    ) -> None:
        client = RecordingGitHubClient()
        graphql_calls: list[dict[str, Any]] = []

        def fake_graphql_json(
            query: str,
            token: str | None,
            variables: Mapping[str, Any] | None = None,
        ) -> dict[str, Any]:
            graphql_calls.append(dict(variables or {}))
            return {
                "data": {
                    "node": {
                        "__typename": "PullRequestReviewThread",
                        "id": "thread-1",
                        "isResolved": False,
                        "pullRequest": {
                            "number": 8,
                            "repository": {
                                "name": "widgets",
                                "owner": {"login": "acme"},
                            },
                        },
                    }
                }
            }

        client.graphql_responder = fake_graphql_json
        with self.assertRaisesRegex(ValueError, "requested pull request"):
            operations_module.resolve_pull_request_review_thread(
                operations_module.GitHubResolvePullRequestReviewThreadRequest(
                    owner="acme",
                    repo="widgets",
                    pull_number=7,
                    thread_id="thread-1",
                    installation_id=99,
                ),
                subject=github_request().subject,
                client=client,
            )

        self.assertEqual(graphql_calls, [{"threadId": "thread-1"}])

    def test_create_pull_request_review_rejects_invalid_inputs_before_github_calls(
        self,
    ) -> None:
        invalid_comments = [
            (
                [],
                "comments must contain at least one comment",
            ),
            (
                [
                    provider_module.PullRequestReviewCommentInput(
                        path="src/widget.py", body="Missing line.", side="RIGHT"
                    )
                ],
                "line is required unless position is set",
            ),
            (
                [
                    provider_module.PullRequestReviewCommentInput(
                        path="src/widget.py",
                        body="Invalid line.",
                        line=-1,
                        side="RIGHT",
                    )
                ],
                "line must be greater than 0",
            ),
            (
                [
                    provider_module.PullRequestReviewCommentInput(
                        path="src/widget.py",
                        body="Invalid side.",
                        line=1,
                        side="CENTER",
                    )
                ],
                "side must be LEFT or RIGHT",
            ),
            (
                [
                    provider_module.PullRequestReviewCommentInput(
                        path="src/widget.py",
                        body="Invalid range.",
                        start_line=3,
                        line=2,
                        side="RIGHT",
                        start_side="RIGHT",
                    )
                ],
                "start_line must be less than or equal to line",
            ),
            (
                [
                    provider_module.PullRequestReviewCommentInput(
                        path="src/widget.py",
                        body="Invalid start side.",
                        line=2,
                        side="RIGHT",
                        start_side="RIGHT",
                    )
                ],
                "start_side requires start_line",
            ),
            (
                [
                    provider_module.PullRequestReviewCommentInput(
                        path="src/widget.py",
                        body="Ambiguous coordinates.",
                        position=6,
                        line=2,
                        side="RIGHT",
                    )
                ],
                "position cannot be combined",
            ),
            (
                [
                    provider_module.PullRequestReviewCommentInput(
                        path="../widget.py",
                        body="Invalid path.",
                        position=6,
                    )
                ],
                "path must not contain '..'",
            ),
            (
                [
                    provider_module.PullRequestReviewCommentInput(
                        path="src/widget.py",
                        body="",
                        position=6,
                    )
                ],
                "body is required",
            ),
        ]

        for comments, expected in invalid_comments:
            with self.subTest(expected=expected):
                with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
                    result = provider_module.bot_create_pull_request_review(
                        provider_module.CreatePullRequestReviewInput(
                            owner="acme",
                            repo="widgets",
                            pull_number=7,
                            body="Review body",
                            comments=comments,
                        ),
                        github_request(),
                    )

                self.assertIsInstance(result, gestalt.Response)
                response = cast(gestalt.Response[dict[str, str]], result)
                self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
                self.assertIn(expected, response.body["error"])
                urlopen.assert_not_called()

    def test_pull_request_read_operations_use_pr_read_permission_and_bound_patches(
        self,
    ) -> None:
        calls: list[tuple[str, str, dict[str, Any], str]] = []
        long_patch = "@@ -1,2 +1,3 @@\n" + ("x" * 9000)

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            calls.append((method, path, body, auth_header(request)))

            if path == "/app/installations/99/access_tokens":
                self.assertEqual(body["permissions"], {"pull_requests": "read"})
                return FakeHTTPResponse({"token": "pr-read-token"})
            if path == "/repos/acme/widgets/pulls/7":
                self.assertEqual(method, "GET")
                self.assertEqual(auth_header(request), "Bearer pr-read-token")
                return FakeHTTPResponse(
                    {
                        "number": 7,
                        "title": "Fix widgets",
                        "state": "open",
                        "html_url": "https://github.com/acme/widgets/pull/7",
                        "url": "https://api.github.com/repos/acme/widgets/pulls/7",
                        "head": {"ref": "feature", "sha": "abc123"},
                        "base": {"ref": "main", "sha": "def456"},
                    }
                )
            if path == "/repos/acme/widgets/pulls/7/files":
                self.assertEqual(method, "GET")
                self.assertEqual(
                    urllib.parse.urlparse(request.full_url).query,
                    "per_page=2&page=3",
                )
                self.assertEqual(auth_header(request), "Bearer pr-read-token")
                return FakeHTTPResponse(
                    [
                        {
                            "sha": "file-sha",
                            "filename": "src/widget.py",
                            "status": "renamed",
                            "previous_filename": "src/old_widget.py",
                            "additions": 3,
                            "deletions": 1,
                            "changes": 4,
                            "blob_url": "https://github.com/acme/widgets/blob/abc/src/widget.py",
                            "raw_url": "https://github.com/acme/widgets/raw/abc/src/widget.py",
                            "contents_url": "https://api.github.com/repos/acme/widgets/contents/src/widget.py",
                            "patch": long_patch,
                        },
                        {
                            "filename": "src/short.py",
                            "status": "modified",
                            "additions": 1,
                            "deletions": 0,
                            "changes": 1,
                            "patch": "@@ -1 +1 @@\n-value\n+value  ",
                        },
                        "ignored-non-object",
                    ]
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            pull_request = provider_module.bot_get_pull_request(
                provider_module.GetPullRequestInput(
                    owner="acme", repo="widgets", pull_number=7
                ),
                github_request(),
            )
            files = provider_module.bot_list_pull_request_files(
                provider_module.ListPullRequestFilesInput(
                    owner="acme",
                    repo="widgets",
                    pull_number=7,
                    per_page=2,
                    page=3,
                ),
                github_request(),
            )

        pull_data = cast(dict[str, Any], pull_request)["data"]["pull_request"]
        self.assertEqual(pull_data["head_sha"], "abc123")
        self.assertEqual(pull_data["base_sha"], "def456")
        file_data = cast(dict[str, Any], files)["data"]["files"][0]
        self.assertEqual(file_data["filename"], "src/widget.py")
        self.assertEqual(file_data["previous_filename"], "src/old_widget.py")
        self.assertEqual(file_data["changes"], 4)
        self.assertEqual(file_data["patch_limit"], 8192)
        self.assertEqual(file_data["patch_truncated"], True)
        self.assertLess(len(file_data["patch"]), len(long_patch))
        self.assertLessEqual(len(file_data["patch"]), file_data["patch_limit"])
        self.assertTrue(file_data["patch"].endswith("\n...<truncated>"))
        short_file_data = cast(dict[str, Any], files)["data"]["files"][1]
        self.assertEqual(short_file_data["patch"], "@@ -1 +1 @@\n-value\n+value  ")
        self.assertEqual(short_file_data["patch_truncated"], False)
        self.assertEqual(cast(dict[str, Any], files)["data"]["count"], 2)
        self.assertEqual(
            [
                call[2].get("permissions")
                for call in calls
                if call[1].endswith("access_tokens")
            ],
            [{"pull_requests": "read"}, {"pull_requests": "read"}],
        )

    def test_list_pull_request_files_rejects_invalid_pagination_before_github_calls(
        self,
    ) -> None:
        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.bot_list_pull_request_files(
                provider_module.ListPullRequestFilesInput(
                    owner="acme",
                    repo="widgets",
                    pull_number=7,
                    per_page=101,
                ),
                github_request(),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertIn("per_page", response.body["error"])
        urlopen.assert_not_called()

    def test_list_pull_request_files_rejects_malformed_github_response(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            path = request_path(request)
            if path == "/app/installations/99/access_tokens":
                return FakeHTTPResponse({"token": "pr-read-token"})
            if path == "/repos/acme/widgets/pulls/7/files":
                return FakeHTTPResponse({"files": []})
            self.fail(f"unexpected request {request.get_method()} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_list_pull_request_files(
                provider_module.ListPullRequestFilesInput(
                    owner="acme", repo="widgets", pull_number=7
                ),
                github_request(),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_GATEWAY)
        self.assertIn(
            "pull request files response was not a list", response.body["error"]
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
            result = provider_module.bot_create_pull_request_review(
                provider_module.CreatePullRequestReviewInput(
                    owner="acme",
                    repo="widgets",
                    pull_number=7,
                    body="Review body",
                    comments=[
                        provider_module.PullRequestReviewCommentInput(
                            path="README.md",
                            body="Inline comment.",
                            position=1,
                        )
                    ],
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
            result = provider_module.bot_get_pull_request(
                provider_module.GetPullRequestInput(
                    owner="acme",
                    repo="widgets",
                    pull_number=7,
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
            result = provider_module.bot_create_pull_request_review(
                provider_module.CreatePullRequestReviewInput(
                    owner="acme",
                    repo="other",
                    pull_number=7,
                    body="Review body",
                    comments=[
                        provider_module.PullRequestReviewCommentInput(
                            path="README.md",
                            body="Inline comment.",
                            position=1,
                        )
                    ],
                ),
                github_request(installation_id=99, repo="acme/widgets"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertIn("repository", response.body["error"])
        urlopen.assert_not_called()

        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.bot_list_pull_request_files(
                provider_module.ListPullRequestFilesInput(
                    owner="acme",
                    repo="other",
                    pull_number=7,
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

        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.bot_create_pull_request_conversation_comment(
                provider_module.CreatePullRequestConversationCommentInput(
                    owner="acme",
                    repo="other",
                    pull_number=7,
                    body="Looks broken.",
                ),
                github_request(installation_id=99, repo="acme/widgets"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertIn("repository", response.body["error"])
        urlopen.assert_not_called()

        new_operation_calls: list[Callable[[], Any]] = [
            lambda: provider_module.bot_add_reaction(
                provider_module.AddReactionInput(
                    owner="acme",
                    repo="other",
                    subject_type="issue",
                    issue_number=7,
                    content="eyes",
                ),
                github_request(installation_id=99, repo="acme/widgets"),
            ),
            lambda: provider_module.bot_add_labels(
                provider_module.AddLabelsInput(
                    owner="acme",
                    repo="other",
                    subject_type="issue",
                    issue_number=7,
                    labels=["bug"],
                ),
                github_request(installation_id=99, repo="acme/widgets"),
            ),
            lambda: provider_module.bot_remove_labels(
                provider_module.RemoveLabelsInput(
                    owner="acme",
                    repo="other",
                    subject_type="pull_request",
                    pull_number=7,
                    labels=["bug"],
                ),
                github_request(installation_id=99, repo="acme/widgets"),
            ),
            lambda: provider_module.bot_request_reviewers(
                provider_module.RequestReviewersInput(
                    owner="acme",
                    repo="other",
                    pull_number=7,
                    reviewers=["octocat"],
                ),
                github_request(installation_id=99, repo="acme/widgets"),
            ),
        ]
        for call in new_operation_calls:
            with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
                result = call()

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
