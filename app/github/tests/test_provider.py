from __future__ import annotations

import io
import json
import pathlib
import unittest
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, is_dataclass
from email.message import Message
from http import HTTPStatus
from typing import Any, cast
from unittest import mock

import gestalt
import yaml

import internals.client as client_module
import internals.operations as operations_module
from internals.config import GitHubBotIdentity, GitHubUserIdentity
from internals.errors import GitHubAPIError
import provider as provider_module


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


class FakeHTTPResponse:
    def __init__(self, body: Any = None) -> None:
        self._body = json.dumps(body or {}).encode("utf-8")

    def __enter__(self) -> FakeHTTPResponse:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


class FakeWorkflowClient:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.publish_event_requests: list[gestalt.WorkflowPublishEvent] = []

    def __enter__(self) -> FakeWorkflowClient:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def publish_event(self, request: gestalt.WorkflowPublishEvent) -> object:
        self.publish_event_requests.append(request)
        if self.fail:
            raise RuntimeError("workflow client unavailable")
        return object()


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
            id=f"service_account:github_webhook:{installation_id}",
            kind="service_account",
            display_name=f"GitHub App installation {installation_id}",
            auth_source="github_webhook",
        ),
        external_identity=github_app_external_identity(repo),
    )


def github_app_external_identity(
    repo: str = "acme/widgets",
) -> gestalt.ExternalIdentity:
    return gestalt.ExternalIdentity(type="github_app_installation", id=f"repo:{repo}")


def github_agent_request(
    installation_id: int = 99,
    repo: str = "acme/widgets",
    external_identity_type: str = "github_identity",
    external_identity_id: str = "user:222",
) -> gestalt.Request:
    req = github_request(installation_id=installation_id, repo=repo)
    req.agent_external_identity = gestalt.ExternalIdentity(
        type=external_identity_type,
        id=external_identity_id,
    )
    return req


class GitHubExternalIdentityRequest:
    def __init__(
        self,
        *,
        external_identity: gestalt.ExternalIdentity,
        subject_installation_id: int = 123,
        subject_repo: str = "acme/other",
    ) -> None:
        self.external_identity = external_identity
        self.subject = github_request(
            installation_id=subject_installation_id, repo=subject_repo
        ).subject


def github_external_identity_request(
    *,
    identity_type: str = "github_app_installation",
    identity_id: str = "repo:acme/widgets",
    subject_installation_id: int = 123,
    subject_repo: str = "acme/other",
) -> GitHubExternalIdentityRequest:
    return GitHubExternalIdentityRequest(
        external_identity=gestalt.ExternalIdentity(
            type=identity_type,
            id=identity_id,
        ),
        subject_installation_id=subject_installation_id,
        subject_repo=subject_repo,
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

    def repository_installation_id(self, owner: str, repo: str) -> int:
        self.requests.append(("GET", f"/repos/{owner}/{repo}/installation", None, {}))
        return 99

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

    def app_installations(
        self, *, per_page: int = 100, page: int = 1
    ) -> list[dict[str, Any]]:
        return []

    def installation_repositories(
        self, access_token: str, *, per_page: int = 100, page: int = 1
    ) -> list[dict[str, Any]]:
        return []

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

    def user_identity_by_id(self, user_id: str) -> GitHubUserIdentity | None:
        if user_id != "222":
            return None
        return GitHubUserIdentity(
            name="Grace Hopper",
            login="ghopper",
            user_id="222",
            email="222+ghopper@users.noreply.github.com",
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
            },
        )
        self.addCleanup(provider_module.configure, "github", {})

    def test_resolve_repository_installation_discovers_and_validates_repo(self) -> None:
        client = RecordingGitHubClient()

        resolution = operations_module.resolve_repository_installation(
            "acme", "widgets", client=client
        )

        self.assertEqual(resolution.installation_id, 99)
        self.assertEqual(resolution.owner, "acme")
        self.assertEqual(resolution.repo, "widgets")
        self.assertEqual(client.tokens, [(99, ("widgets",), {})])

    def test_bot_resolve_installation_returns_run_as_identity(self) -> None:
        client = RecordingGitHubClient()
        with mock.patch.object(operations_module, "DEFAULT_GITHUB_CLIENT", client):
            result = provider_module.bot_resolve_installation(
                provider_module.ResolveInstallationInput(owner="acme", repo="widgets"),
                github_request(),
            )

        installation = result["data"]
        self.assertEqual(installation["installation_id"], 99)
        self.assertEqual(installation["repository"], "acme/widgets")
        self.assertEqual(
            installation["external_identity"],
            {"type": "github_app_installation", "id": "repo:acme/widgets"},
        )
        self.assertEqual(client.tokens, [(99, ("widgets",), {})])

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

    def test_catalog_and_schema_expose_events_and_generic_bot_operations(
        self,
    ) -> None:
        plugin_root = pathlib.Path(__file__).resolve().parents[1]
        catalog = yaml.safe_load((plugin_root / "catalog.yaml").read_text())
        operations = {operation["id"]: operation for operation in catalog["operations"]}
        operation_ids = set(operations)

        event = operations[provider_module.GITHUB_EVENT_OPERATION]
        resolve = operations[provider_module.BOT_RESOLVE_INSTALLATION_OPERATION]
        repo = operations[provider_module.BOT_GET_REPOSITORY_OPERATION]
        search_code = operations[provider_module.BOT_SEARCH_CODE_OPERATION]
        get_content = operations[provider_module.BOT_GET_CONTENT_OPERATION]
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
        suite_check_runs = operations[
            provider_module.BOT_LIST_CHECK_SUITE_CHECK_RUNS_OPERATION
        ]
        self.assertIn("canonical workflow", event["description"])
        self.assertNotIn("user.createPullRequest", operation_ids)
        self.assertNotIn("reviewPullRequest", operation_ids)
        self.assertFalse(
            any(
                operation_id.startswith("actionPreferences.")
                for operation_id in operation_ids
            )
        )
        self.assertIn("runAs identities", resolve["description"])
        self.assertIn("repository metadata", repo["description"])
        self.assertIn("Search code", search_code["description"])
        self.assertIn("file content", get_content["description"])
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
        self.assertIn("check suite", suite_check_runs["description"])
        self.assertIn(
            "pull_number", [parameter["name"] for parameter in pr["parameters"]]
        )
        self.assertEqual(
            ["owner", "repo"],
            [parameter["name"] for parameter in resolve["parameters"]],
        )
        self.assertIn(
            "owner",
            [parameter["name"] for parameter in repo["parameters"]],
        )
        self.assertIn(
            "query",
            [parameter["name"] for parameter in search_code["parameters"]],
        )
        self.assertIn(
            "path",
            [parameter["name"] for parameter in get_content["parameters"]],
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
        self.assertIn(
            "check_suite_id",
            [parameter["name"] for parameter in suite_check_runs["parameters"]],
        )
        for operation in operations.values():
            self.assertNotIn(
                "installation_id",
                [parameter["name"] for parameter in operation.get("parameters", [])],
            )
        schema = yaml.safe_load(
            (plugin_root / "schemas" / "config.schema.yaml").read_text()
        )
        schema_properties = schema["properties"]
        self.assertNotIn("webhookPolicies", schema_properties)
        self.assertNotIn("actionPreferences", schema_properties)
        self.assertNotIn("agent", schema_properties)
        self.assertEqual(schema_properties["workflow"]["required"], ["provider"])
        self.assertEqual(
            schema_properties["workflow"]["additionalProperties"],
            False,
        )
        self.assertEqual(schema["additionalProperties"], False)

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
        self.assertEqual(identity.slug, "example-app")
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

    def test_rest_json_preserves_github_validation_error_details(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            raise http_error(
                request.full_url,
                HTTPStatus.UNPROCESSABLE_ENTITY,
                json.dumps(
                    {
                        "message": "Validation Failed",
                        "errors": [
                            {
                                "resource": "PullRequest",
                                "field": "head",
                                "code": "invalid",
                                "message": "head is not a branch",
                            }
                        ],
                    }
                ),
            )

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            with self.assertRaises(GitHubAPIError) as raised:
                client_module.github_json(
                    "POST",
                    "/repos/acme/widgets/pulls",
                    "installation-token",
                    {"title": "Smoke test", "head": "missing", "base": "main"},
                )

        error = raised.exception
        self.assertEqual(error.status, HTTPStatus.UNPROCESSABLE_ENTITY)
        self.assertEqual(
            error.details,
            "PullRequest.head (invalid, head is not a branch)",
        )
        self.assertEqual(
            error.message,
            "Validation Failed: PullRequest.head (invalid, head is not a branch)",
        )

        response = provider_module._github_error(error)
        self.assertEqual(response.status, HTTPStatus.UNPROCESSABLE_ENTITY)
        self.assertEqual(response.body["error"], error.message)
        self.assertEqual(response.body["details"], error.details)

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
                    coauthors=[provider_module.CoAuthorInput(name="", email="")],
                ),
                github_request(),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertIn("coauthor name and email are required", response.body["error"])
        urlopen.assert_not_called()

    def test_delegated_agent_subject_supplies_commit_author(self) -> None:
        recording_client = RecordingGitHubClient()
        with mock.patch.object(
            provider_module, "DEFAULT_GITHUB_CLIENT", recording_client
        ):
            request = provider_module._commit_request_from_input(
                provider_module.CommitFilesInput(
                    owner="acme",
                    repo="widgets",
                    message="Update README",
                    files=[
                        provider_module.FileChangeInput(path="README.md", content="hi")
                    ],
                    author_name="Spoofed User",
                    author_email="spoof@example.com",
                ),
                github_agent_request(),
            )

        self.assertEqual(request.author_name, "Grace Hopper")
        self.assertEqual(request.author_email, "222+ghopper@users.noreply.github.com")

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
                coauthors=(
                    operations_module.GitHubCoAuthor(
                        name="Ada", email="ada@example.com"
                    ),
                ),
            ),
            subject=github_request().subject,
            pull_request_permissions=True,
            external_identity=github_app_external_identity(),
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
                "/repos/acme/widgets/installation",
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
        self.assertEqual(subject.id, "service_account:github_webhook:99")
        self.assertEqual(subject.kind, "service_account")
        self.assertEqual(subject.auth_source, "github_webhook")
        self.assertIn("acme/widgets", subject.display_name)

    def test_webhook_handler_publishes_canonical_workflow_event(self) -> None:
        workflow_client = FakeWorkflowClient()
        payload = {
            "headers": {
                "X-GitHub-Event": "pull_request",
                "X-GitHub-Delivery": "delivery-123",
            },
            "action": "opened",
            "installation": {"id": 99.0},
            "repository": {
                "full_name": "acme/widgets",
                "name": "widgets",
                "owner": {"login": "acme"},
            },
            "pull_request": {
                "number": 7,
                "title": "Add feature",
                "state": "open",
                "html_url": "https://github.example/acme/widgets/pull/7",
                "head": {"ref": "feature", "sha": "head-sha"},
                "base": {"ref": "main", "sha": "base-sha"},
            },
            "sender": {"login": "octocat"},
        }

        with mock.patch.object(
            gestalt.Request,
            "workflows",
            return_value=workflow_client,
            create=True,
        ):
            result = provider_module.github_events_handle(payload, gestalt.Request())

        self.assertEqual(
            operation_body(result),
            {
                "ok": True,
                "published": True,
                "workflow_event_id": "github:delivery-123",
                "workflow_provider": "local",
            },
        )
        self.assertEqual(len(workflow_client.publish_event_requests), 1)
        request = workflow_client.publish_event_requests[0]
        self.assertEqual(request.provider_name, "local")
        event = request.event
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.id, "github:delivery-123")
        self.assertEqual(event.source, "github")
        self.assertEqual(event.spec_version, "1.0")
        self.assertEqual(event.type, "github.pull_request")
        self.assertEqual(event.subject, "repo:acme/widgets")
        self.assertEqual(event.datacontenttype, "application/json")
        self.assertEqual(
            sdk_value_to_dict(event.data),
            {
                "github": {
                    "installation_id": 99,
                    "event_type": "pull_request",
                    "action": "opened",
                    "repository": "acme/widgets",
                    "repository_owner": "acme",
                    "repository_name": "widgets",
                    "sender": "octocat",
                    "delivery_id": "delivery-123",
                    "number": 7,
                    "head_ref": "feature",
                    "head_sha": "head-sha",
                    "base_ref": "main",
                    "base_sha": "base-sha",
                    "title": "Add feature",
                    "state": "open",
                    "html_url": "https://github.example/acme/widgets/pull/7",
                    "event_header": "pull_request",
                },
                "raw": payload,
            },
        )

    def test_webhook_handler_uses_digest_id_and_installation_subject_without_headers(
        self,
    ) -> None:
        workflow_client = FakeWorkflowClient()
        payload = {
            "action": "completed",
            "installation": {"id": 99},
            "check_run": {
                "id": 1234,
                "name": "ci",
                "status": "completed",
                "conclusion": "success",
                "head_sha": "abc123",
            },
            "sender": {"login": "octocat"},
        }

        with mock.patch.object(
            gestalt.Request,
            "workflows",
            return_value=workflow_client,
            create=True,
        ):
            result = provider_module.github_events_handle(payload, gestalt.Request())

        self.assertEqual(operation_body(result)["ok"], True)
        self.assertEqual(len(workflow_client.publish_event_requests), 1)
        event = workflow_client.publish_event_requests[0].event
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.id, f"github:{provider_module.payload_digest(payload)}")
        self.assertEqual(event.type, "github.check_run")
        self.assertEqual(event.subject, "installation:99")
        data = sdk_value_to_dict(event.data)
        self.assertEqual(data["github"]["event_type"], "check_run")
        self.assertEqual(data["github"]["check_run_id"], 1234)
        self.assertNotIn("event_header", data["github"])
        self.assertEqual(data["raw"], payload)

    def test_webhook_handler_publish_failure_is_retryable_server_error(self) -> None:
        workflow_client = FakeWorkflowClient(fail=True)
        payload = {
            "headers": {
                "X-GitHub-Event": "pull_request",
                "X-GitHub-Delivery": "delivery-123",
            },
            "action": "opened",
            "installation": {"id": 99},
            "repository": {"full_name": "acme/widgets"},
            "pull_request": {"number": 7},
            "sender": {"login": "octocat"},
        }

        with mock.patch.object(
            gestalt.Request,
            "workflows",
            return_value=workflow_client,
            create=True,
        ):
            result = provider_module.github_events_handle(payload, gestalt.Request())

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(
            response.body,
            {"error": "failed to publish workflow event: workflow client unavailable"},
        )
        self.assertEqual(len(workflow_client.publish_event_requests), 1)

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

            if path == "/user/222":
                self.assertEqual(method, "GET")
                self.assertEqual(auth_header(request), "")
                return FakeHTTPResponse(
                    {"id": 222, "login": "ghopper", "name": "Grace Hopper"}
                )
            if path == "/app":
                self.assertEqual(method, "GET")
                return FakeHTTPResponse(
                    {"name": "Example App Bot", "slug": "example-app"}
                )
            if path == "/users/example-app%5Bbot%5D":
                self.assertEqual(method, "GET")
                self.assertEqual(auth_header(request), "")
                return FakeHTTPResponse({"id": 12345678, "login": "example-app[bot]"})
            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
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
                self.assertEqual(
                    body["author"],
                    {
                        "name": "Grace Hopper",
                        "email": "222+ghopper@users.noreply.github.com",
                    },
                )
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
                    coauthors=[
                        provider_module.CoAuthorInput(
                            name="Ada", email="ada@example.com"
                        )
                    ],
                ),
                github_agent_request(),
            )

        self.assertIsInstance(result, dict)
        data = cast(dict[str, Any], result)["data"]["commit"]
        self.assertEqual(data["sha"], "new-commit")
        self.assertEqual(data["branch"], "feature")
        self.assertEqual(data["base_branch"], "main")
        self.assertTrue(data["branch_created"])
        self.assertEqual(calls[-1][1], "/repos/acme/widgets/git/refs")

    def test_commit_files_rejects_stale_expected_head(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            if path == "/user/222":
                return FakeHTTPResponse(
                    {"id": 222, "login": "ghopper", "name": "Grace Hopper"}
                )
            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
            if path == "/app/installations/99/access_tokens":
                self.assertEqual(body["permissions"], {"contents": "write"})
                return FakeHTTPResponse({"token": "installation-token"})
            if path == "/repos/acme/widgets/git/ref/heads/feature":
                return FakeHTTPResponse({"object": {"sha": "newer-head"}})
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
                    include_bot_coauthor=False,
                    expected_head_sha="reviewed-head",
                ),
                github_agent_request(),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, Any]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertIn("branch head changed", response.body["error"])

    def test_create_pull_request_commits_files_then_opens_pr(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)

            if path == "/user/222":
                return FakeHTTPResponse(
                    {"id": 222, "login": "ghopper", "name": "Grace Hopper"}
                )
            if path == "/app":
                return FakeHTTPResponse(
                    {"name": "Example App Bot", "slug": "example-app"}
                )
            if path == "/users/example-app%5Bbot%5D":
                self.assertEqual(auth_header(request), "")
                return FakeHTTPResponse({"id": 12345678, "login": "example-app[bot]"})
            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
            if path == "/app/installations/99/access_tokens":
                permissions = body["permissions"]
                if permissions == {"contents": "write", "pull_requests": "write"}:
                    return FakeHTTPResponse({"token": "write-token"})
                if permissions == {"contents": "read", "pull_requests": "write"}:
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
                self.assertEqual(
                    body["author"],
                    {
                        "name": "Grace Hopper",
                        "email": "222+ghopper@users.noreply.github.com",
                    },
                )
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
                github_agent_request(),
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["commit"]["sha"], "new-commit")
        self.assertEqual(data["pull_request"]["number"], 42)
        self.assertEqual(
            data["pull_request"]["html_url"],
            "https://github.com/acme/widgets/pull/42",
        )

    def test_open_pull_request_uses_contents_read_for_ref_visibility(self) -> None:
        calls: list[tuple[str, str, dict[str, Any], str]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            calls.append((method, path, body, auth_header(request)))

            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})

            if path == "/app/installations/99/access_tokens":
                self.assertEqual(
                    body["permissions"],
                    {"contents": "read", "pull_requests": "write"},
                )
                return FakeHTTPResponse({"token": "pr-token"})
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
            result = provider_module.bot_open_pull_request(
                provider_module.OpenPullRequestInput(
                    owner="acme",
                    repo="widgets",
                    title="Update README",
                    head="feature",
                    base="main",
                ),
                github_agent_request(),
            )

        data = cast(dict[str, Any], result)["data"]["pull_request"]
        self.assertEqual(data["number"], 42)
        self.assertEqual(
            [
                call[2].get("permissions")
                for call in calls
                if call[1].endswith("access_tokens")
            ],
            [{"contents": "read", "pull_requests": "write"}],
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

            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})

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

            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})

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

            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})

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

            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})

            if path == "/app/installations/99/access_tokens":
                self.assertEqual(body["permissions"], {"pull_requests": "write"})
                return FakeHTTPResponse({"token": "pr-token"})
            if path == "/repos/acme/widgets/pulls/7/reviews":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer pr-token")
                self.assertEqual(
                    body,
                    {
                        "body": "Review: I found two concrete issues.",
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
                        "body": "Review: I found two concrete issues.",
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
                    body="Review: I found two concrete issues.",
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

            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})

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

            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})

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
            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
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

            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})

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
            ),
            subject=github_request().subject,
            external_identity=github_app_external_identity(),
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
            ),
            subject=github_request().subject,
            external_identity=github_app_external_identity(),
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
                ),
                subject=github_request().subject,
                external_identity=github_app_external_identity(),
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
                "line is required",
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
                        path="../widget.py",
                        body="Invalid path.",
                        line=6,
                        side="RIGHT",
                    )
                ],
                "path must not contain '..'",
            ),
            (
                [
                    provider_module.PullRequestReviewCommentInput(
                        path="src/widget.py",
                        body="",
                        line=6,
                        side="RIGHT",
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

            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})

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
                        "head": {
                            "ref": "feature",
                            "sha": "abc123",
                            "repo": {
                                "full_name": "acme/widgets",
                                "name": "widgets",
                                "owner": {"login": "acme"},
                            },
                        },
                        "base": {
                            "ref": "main",
                            "sha": "def456",
                            "repo": {
                                "full_name": "acme/widgets",
                                "name": "widgets",
                                "owner": {"login": "acme"},
                            },
                        },
                        "maintainer_can_modify": False,
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
        self.assertEqual(pull_data["head_ref"], "feature")
        self.assertEqual(pull_data["base_ref"], "main")
        self.assertEqual(pull_data["head_repo"]["full_name"], "acme/widgets")
        self.assertEqual(pull_data["base_repo"]["full_name"], "acme/widgets")
        self.assertEqual(pull_data["head_repo_is_base_repo"], True)
        self.assertEqual(pull_data["maintainer_can_modify"], False)
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

    def test_bot_repository_code_operations_use_installation_token(self) -> None:
        calls: list[tuple[str, str, dict[str, Any], str]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            calls.append((method, path, body, auth_header(request)))

            if path == "/repos/acme/widgets/installation":
                self.assertEqual(auth_header(request), "Bearer app-jwt")
                return FakeHTTPResponse({"id": 99})
            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
            if path == "/app/installations/99/access_tokens":
                self.assertEqual(body["repositories"], ["widgets"])
                self.assertEqual(body["permissions"], {"contents": "read"})
                return FakeHTTPResponse({"token": "contents-read-token"})
            if path == "/repos/acme/widgets":
                self.assertEqual(auth_header(request), "Bearer contents-read-token")
                return FakeHTTPResponse(
                    {
                        "id": 123,
                        "name": "widgets",
                        "full_name": "acme/widgets",
                        "owner": {"login": "acme"},
                        "private": True,
                        "default_branch": "main",
                        "html_url": "https://github.com/acme/widgets",
                    }
                )
            if path == "/search/code":
                self.assertEqual(auth_header(request), "Bearer contents-read-token")
                query = urllib.parse.parse_qs(
                    urllib.parse.urlparse(request.full_url).query
                )
                self.assertEqual(query["per_page"], ["2"])
                self.assertEqual(query["page"], ["3"])
                self.assertEqual(query["q"], ["Widget repo:acme/widgets path:src"])
                return FakeHTTPResponse(
                    {
                        "total_count": 1,
                        "incomplete_results": False,
                        "items": [
                            {
                                "name": "widget.py",
                                "path": "src/widget.py",
                                "sha": "abc123",
                                "html_url": "https://github.com/acme/widgets/blob/main/src/widget.py",
                                "repository": {"full_name": "acme/widgets"},
                                "score": 1.0,
                            }
                        ],
                    }
                )
            if path == "/repos/acme/widgets/contents/src/widget.py":
                self.assertEqual(auth_header(request), "Bearer contents-read-token")
                self.assertEqual(
                    urllib.parse.parse_qs(
                        urllib.parse.urlparse(request.full_url).query
                    )["ref"],
                    ["main"],
                )
                return FakeHTTPResponse(
                    {
                        "type": "file",
                        "size": 12,
                        "encoding": "base64",
                        "content": "aGVsbG8gd29ybGQK",
                    }
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            repo = provider_module.bot_get_repository(
                provider_module.RepositoryInput(owner="acme", repo="widgets"),
                github_external_identity_request(),
            )
            search = provider_module.bot_search_code(
                provider_module.SearchCodeInput(
                    owner="acme",
                    repo="widgets",
                    query="Widget",
                    path="src",
                    per_page=2,
                    page=3,
                ),
                github_external_identity_request(),
            )
            content = provider_module.bot_get_content(
                provider_module.GetContentInput(
                    owner="acme",
                    repo="widgets",
                    path="src/widget.py",
                ),
                github_external_identity_request(),
            )

        self.assertEqual(
            cast(dict[str, Any], repo)["data"]["repository"]["full_name"],
            "acme/widgets",
        )
        self.assertEqual(
            cast(dict[str, Any], search)["data"]["items"][0]["path"],
            "src/widget.py",
        )
        self.assertEqual(
            cast(dict[str, Any], content)["data"]["content"], "hello world\n"
        )
        self.assertEqual(
            [
                call[2].get("permissions")
                for call in calls
                if call[1].endswith("access_tokens")
            ],
            [{"contents": "read"}, {"contents": "read"}, {"contents": "read"}],
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
            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
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

            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})

            if path == "/app/installations/99/access_tokens":
                if body["permissions"] == {"checks": "read"}:
                    return FakeHTTPResponse({"token": "checks-token"})
                if body["permissions"] == {"checks": "write"}:
                    return FakeHTTPResponse({"token": "checks-write-token"})
                if body["permissions"] == {"actions": "read"}:
                    return FakeHTTPResponse({"token": "actions-token"})
            if path == "/repos/acme/widgets/check-runs" and method == "POST":
                self.assertEqual(auth_header(request), "Bearer checks-write-token")
                if body["name"] == "Completed Review":
                    self.assertEqual(body["head_sha"], "def456")
                    self.assertEqual(body["conclusion"], "success")
                    self.assertEqual(body["status"], "completed")
                else:
                    self.assertEqual(body["name"], "Gestalt Review")
                    self.assertEqual(body["head_sha"], "abc123")
                    self.assertEqual(body["status"], "in_progress")
                return FakeHTTPResponse(
                    {
                        "id": 999,
                        "name": body["name"],
                        "status": body["status"],
                        "head_sha": body["head_sha"],
                    }
                )
            if path == "/repos/acme/widgets/check-runs/999" and method == "PATCH":
                self.assertEqual(auth_header(request), "Bearer checks-write-token")
                self.assertEqual(body["conclusion"], "success")
                self.assertEqual(body["status"], "completed")
                self.assertEqual(body["output"]["title"], "Review complete")
                return FakeHTTPResponse(
                    {
                        "id": 999,
                        "name": "Gestalt Review",
                        "status": "completed",
                        "conclusion": "success",
                    }
                )
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
            if path == "/repos/acme/widgets/check-suites/321/check-runs":
                self.assertEqual(
                    urllib.parse.urlparse(request.full_url).query,
                    "check_name=Build+Gestalt&status=completed&filter=latest&per_page=4&page=2",
                )
                self.assertEqual(auth_header(request), "Bearer checks-token")
                return FakeHTTPResponse(
                    {
                        "total_count": 1,
                        "check_runs": [
                            {
                                "id": 654,
                                "name": "Build Gestalt",
                                "status": "completed",
                                "conclusion": "failure",
                                "html_url": "https://github.com/acme/widgets/runs/654",
                                "head_sha": "abc123",
                            }
                        ],
                    }
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
            created_check = provider_module.bot_create_check_run(
                provider_module.CreateCheckRunInput(
                    owner="acme",
                    repo="widgets",
                    name="Gestalt Review",
                    head_sha="abc123",
                    status="in_progress",
                ),
                github_request(),
            )
            completed_created_check = provider_module.bot_create_check_run(
                provider_module.CreateCheckRunInput(
                    owner="acme",
                    repo="widgets",
                    name="Completed Review",
                    head_sha="def456",
                    conclusion="success",
                ),
                github_request(),
            )
            updated_check = provider_module.bot_update_check_run(
                provider_module.UpdateCheckRunInput(
                    owner="acme",
                    repo="widgets",
                    check_run_id=999,
                    conclusion="success",
                    output=provider_module.CheckRunOutputInput(
                        title="Review complete",
                        summary="No findings.",
                    ),
                ),
                github_request(),
            )
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
            suite_runs = provider_module.bot_list_check_suite_check_runs(
                provider_module.ListCheckSuiteCheckRunsInput(
                    owner="acme",
                    repo="widgets",
                    check_suite_id=321,
                    check_name="Build Gestalt",
                    status="completed",
                    filter="latest",
                    per_page=4,
                    page=2,
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
            cast(dict[str, Any], created_check)["data"]["check_run"]["id"], 999
        )
        self.assertEqual(
            cast(dict[str, Any], completed_created_check)["data"]["check_run"][
                "status"
            ],
            "completed",
        )
        self.assertEqual(
            cast(dict[str, Any], updated_check)["data"]["check_run"]["conclusion"],
            "success",
        )
        self.assertEqual(
            cast(dict[str, Any], check_run)["data"]["check_run"]["id"], 123
        )
        self.assertEqual(
            cast(dict[str, Any], annotations)["data"]["annotations"][0]["message"],
            "broken",
        )
        self.assertEqual(
            cast(dict[str, Any], suite_runs)["data"]["check_runs"][0]["id"], 654
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
                ),
                github_request(),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertIn("delete cannot include content", response.body["error"])
        urlopen.assert_not_called()

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
                    force=True,
                    expected_head_sha="abc123",
                ),
                github_request(),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertIn("force cannot be combined", response.body["error"])
        urlopen.assert_not_called()

    def test_resolve_installation_returns_external_identity(
        self,
    ) -> None:
        calls: list[tuple[str, str, str]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            calls.append(
                (request.get_method(), request_path(request), auth_header(request))
            )
            if request_path(request) == "/repos/acme/widgets/installation":
                self.assertEqual(request.get_method(), "GET")
                self.assertEqual(auth_header(request), "Bearer app-jwt")
                return FakeHTTPResponse({"id": 99})
            if request_path(request) == "/app/installations/99/access_tokens":
                self.assertEqual(request.get_method(), "POST")
                self.assertEqual(auth_header(request), "Bearer app-jwt")
                self.assertEqual(request_json(request)["repositories"], ["widgets"])
                return FakeHTTPResponse({"token": "install-token"})
            self.fail(
                f"unexpected request {request.get_method()} {request_path(request)}"
            )

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_resolve_installation(
                provider_module.ResolveInstallationInput(owner="acme", repo="widgets"),
                gestalt.Request(),
            )

        self.assertIsInstance(result, dict)
        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["installation_id"], 99)
        self.assertEqual(
            data["external_identity"],
            {"type": "github_app_installation", "id": "repo:acme/widgets"},
        )
        self.assertEqual(
            calls,
            [
                ("GET", "/repos/acme/widgets/installation", "Bearer app-jwt"),
                ("POST", "/app/installations/99/access_tokens", "Bearer app-jwt"),
            ],
        )

    def test_bot_operations_use_external_identity(self) -> None:
        calls: list[tuple[str, str, dict[str, Any], str]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            calls.append((method, path, body, auth_header(request)))

            if path == "/repos/acme/widgets/installation":
                self.assertEqual(method, "GET")
                self.assertEqual(auth_header(request), "Bearer app-jwt")
                return FakeHTTPResponse({"id": 99})
            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
            if path == "/app/installations/99/access_tokens":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer app-jwt")
                self.assertEqual(body["repositories"], ["widgets"])
                self.assertEqual(
                    body["permissions"],
                    {"contents": "read", "pull_requests": "write"},
                )
                return FakeHTTPResponse({"token": "pr-token"})
            if path == "/repos/acme/widgets/pulls":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer pr-token")
                self.assertEqual(body["head"], "feature")
                return FakeHTTPResponse(
                    {
                        "number": 7,
                        "title": "Update README",
                        "state": "open",
                        "html_url": "https://github.example/acme/widgets/pull/7",
                        "url": "https://api.github.example/repos/acme/widgets/pulls/7",
                        "head": {"ref": "feature", "sha": "head-sha"},
                        "base": {"ref": "main", "sha": "base-sha"},
                    }
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_open_pull_request(
                provider_module.OpenPullRequestInput(
                    owner="acme",
                    repo="widgets",
                    title="Update README",
                    head="feature",
                    base="main",
                ),
                github_external_identity_request(
                    subject_installation_id=123, subject_repo="acme/other"
                ),
            )

        self.assertIsInstance(result, dict)
        data = cast(dict[str, Any], result)["data"]["pull_request"]
        self.assertEqual(data["number"], 7)
        self.assertEqual(
            [call[1] for call in calls],
            [
                "/repos/acme/widgets/installation",
                "/app/installations/99/access_tokens",
                "/repos/acme/widgets/pulls",
            ],
        )

    def test_bot_operations_read_external_identity_from_sdk_request(self) -> None:
        calls: list[str] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            path = request_path(request)
            calls.append(path)
            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
            if path == "/app/installations/99/access_tokens":
                return FakeHTTPResponse({"token": "pr-token"})
            if path == "/repos/acme/widgets/pulls":
                return FakeHTTPResponse(
                    {
                        "number": 7,
                        "title": "Update README",
                        "state": "open",
                        "html_url": "https://github.example/acme/widgets/pull/7",
                        "url": "https://api.github.example/repos/acme/widgets/pulls/7",
                        "head": {"ref": "feature", "sha": "head-sha"},
                        "base": {"ref": "main", "sha": "base-sha"},
                    }
                )
            self.fail(f"unexpected request {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_open_pull_request(
                provider_module.OpenPullRequestInput(
                    owner="acme",
                    repo="widgets",
                    title="Update README",
                    head="feature",
                    base="main",
                ),
                gestalt.Request(
                    subject=github_request(
                        installation_id=123, repo="acme/other"
                    ).subject,
                    external_identity=gestalt.ExternalIdentity(
                        type="github_app_installation",
                        id="repo:acme/widgets",
                    ),
                ),
            )

        self.assertIsInstance(result, dict)
        self.assertEqual(calls[0], "/repos/acme/widgets/installation")
        self.assertIn("/repos/acme/widgets/pulls", calls)

    def test_bot_operations_treat_empty_external_identity_as_absent(self) -> None:
        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.bot_create_issue_comment(
                provider_module.CreateIssueCommentInput(
                    owner="acme",
                    repo="widgets",
                    issue_number=7,
                    body="Looks good.",
                ),
                GitHubExternalIdentityRequest(
                    external_identity=gestalt.ExternalIdentity(type="", id=""),
                    subject_installation_id=99,
                    subject_repo="acme/widgets",
                ),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertIn("external_identity", response.body["error"])
        urlopen.assert_not_called()

    def test_external_identity_invalid_mismatch_and_unresolvable_fail_closed(
        self,
    ) -> None:
        valid_request = github_external_identity_request(
            subject_installation_id=99, subject_repo="acme/widgets"
        )

        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.bot_create_issue_comment(
                provider_module.CreateIssueCommentInput(
                    owner="acme",
                    repo="widgets",
                    issue_number=7,
                    body="Looks broken.",
                ),
                github_external_identity_request(
                    identity_type="github_identity",
                    identity_id="repo:acme/widgets",
                    subject_installation_id=99,
                    subject_repo="acme/widgets",
                ),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertIn("external_identity.type", response.body["error"])
        urlopen.assert_not_called()

        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.bot_create_issue_comment(
                provider_module.CreateIssueCommentInput(
                    owner="acme",
                    repo="widgets",
                    issue_number=7,
                    body="Looks broken.",
                ),
                github_external_identity_request(
                    identity_id="repo:acme/other",
                    subject_installation_id=99,
                    subject_repo="acme/widgets",
                ),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertIn("external_identity.id", response.body["error"])
        urlopen.assert_not_called()

        calls: list[str] = []

        def unresolved_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            calls.append(request_path(request))
            if request_path(request) == "/repos/acme/widgets/installation":
                raise http_error(request.full_url, HTTPStatus.NOT_FOUND)
            self.fail(
                f"unexpected request {request.get_method()} {request_path(request)}"
            )

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen",
                side_effect=unresolved_urlopen,
            ),
        ):
            result = provider_module.bot_create_issue_comment(
                provider_module.CreateIssueCommentInput(
                    owner="acme",
                    repo="widgets",
                    issue_number=7,
                    body="Looks broken.",
                ),
                valid_request,
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertIn("could not be resolved", response.body["error"])
        self.assertEqual(calls, ["/repos/acme/widgets/installation"])

    def test_bot_operations_require_external_identity(self) -> None:
        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.bot_open_pull_request(
                provider_module.OpenPullRequestInput(
                    owner="acme",
                    repo="widgets",
                    title="Update README",
                    head="feature",
                    base="main",
                ),
                gestalt.Request(subject=github_request().subject),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertIn("external_identity", response.body["error"])
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
                "workflows",
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
                "workflows",
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
