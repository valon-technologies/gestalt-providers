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
from http.client import HTTPMessage
from typing import Any, cast
from unittest import mock

import gestalt
import yaml
from gestalt.authorization import RelationshipTargetSubject

import internals.client as client_module
import internals.operations as operations_module
from internals.config import GitHubBotIdentity, GitHubUserIdentity
from internals.errors import GitHubAPIError
from internals.operations import MERGE_QUEUE_QUERY, SEARCH_PULL_REQUESTS_QUERY
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
        self.deliver_event_requests: list[gestalt.WorkflowDeliverEvent] = []

    def __enter__(self) -> FakeWorkflowClient:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def deliver_event(self, request: gestalt.WorkflowDeliverEvent) -> object:
        self.deliver_event_requests.append(request)
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


class FakeAuthorization:
    def __init__(
        self,
        *,
        allowed: bool = True,
        resources: Sequence[gestalt.AuthorizationResource] = (),
    ) -> None:
        self.allowed = allowed
        self.resources = tuple(resources)
        self.check_access_requests: list[gestalt.CheckAccessRequest] = []
        self.list_relationships_requests: list[gestalt.ListRelationshipsRequest] = []
        self.relationship_adds: list[gestalt.AddRelationshipRequest] = []

    def check_access(
        self, request: gestalt.CheckAccessRequest
    ) -> gestalt.CheckAccessResponse:
        self.check_access_requests.append(request)
        return gestalt.CheckAccessResponse(allowed=self.allowed)

    def list_relationships(
        self, request: gestalt.ListRelationshipsRequest
    ) -> gestalt.ListRelationshipsResponse:
        self.list_relationships_requests.append(request)
        target = request.filter.target if request.filter is not None else None
        relation = request.filter.relation if request.filter is not None else ""
        return gestalt.ListRelationshipsResponse(
            relationships=[
                gestalt.Relationship(
                    tuple=gestalt.RelationshipTuple(
                        target=target,
                        relation=relation,
                        resource=resource,
                    )
                )
                for resource in self.resources
            ]
        )

    def add_relationship(
        self, request: gestalt.AddRelationshipRequest
    ) -> gestalt.AddRelationshipResponse:
        self.relationship_adds.append(request)
        return gestalt.AddRelationshipResponse(relationship=request.relationship)


def github_request(
    installation_id: int = 99, repo: str = "acme/widgets"
) -> gestalt.Request:
    return gestalt.Request(
        subject=gestalt.Subject(
            id=f"service_account:github_webhook:{installation_id}",
        ),
    )


def github_agent_request(
    installation_id: int = 99,
    repo: str = "acme/widgets",
    agent_subject_id: str = "user:gestalt-123",
) -> gestalt.Request:
    request = github_request(installation_id=installation_id, repo=repo)
    request.agent_subject = gestalt.Subject(
        id=agent_subject_id,
    )
    return request


class GitHubAuthorizedRequest:
    def __init__(
        self,
        *,
        authorization: FakeAuthorization | None = None,
        subject_installation_id: int = 123,
        subject_repo: str = "acme/other",
    ) -> None:
        self._authorization = authorization or FakeAuthorization()
        self.subject = github_request(
            installation_id=subject_installation_id, repo=subject_repo
        ).subject

    def authorization(self) -> FakeAuthorization:
        return self._authorization


def github_authorized_request(
    *,
    authorization: FakeAuthorization | None = None,
    subject_installation_id: int = 123,
    subject_repo: str = "acme/other",
) -> GitHubAuthorizedRequest:
    return GitHubAuthorizedRequest(
        authorization=authorization,
        subject_installation_id=subject_installation_id,
        subject_repo=subject_repo,
    )


def github_user_link_authorization(user_id: str = "222") -> FakeAuthorization:
    return FakeAuthorization(
        resources=(
            gestalt.AuthorizationResource(
                type=provider_module.GITHUB_USER_RESOURCE_TYPE,
                id=f"github.com/{user_id}",
            ),
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
        if path == "/user":
            return {
                "id": 222,
                "login": "ghopper",
                "name": "Grace Hopper",
                "email": "grace@example.com",
            }
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

    def workflow_job_logs(
        self, token: str, owner: str, repo: str, job_id: int
    ) -> str:
        self.requests.append(
            ("GET", f"/repos/{owner}/{repo}/actions/jobs/{job_id}/logs", token, {})
        )
        return "log line one\nlog line two\n"


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
        self.authorization = FakeAuthorization()
        authorization_patch = mock.patch.object(
            gestalt.Request,
            "authorization",
            return_value=self.authorization,
            create=True,
        )
        authorization_patch.start()
        self.addCleanup(authorization_patch.stop)

    def test_manifest_declares_github_app_webhook_contract(self) -> None:
        manifest_path = pathlib.Path(__file__).resolve().parents[1] / "manifest.yaml"
        manifest = yaml.safe_load(manifest_path.read_text())

        spec = manifest["spec"]
        webhook = spec["http"]["event"]
        security = spec["securitySchemes"]["github_app"]
        default_connection = spec["connections"]["default"]

        self.assertEqual(spec["defaultConnection"], "default")
        self.assertEqual(default_connection["mode"], "subject")
        self.assertEqual(default_connection["auth"]["type"], "oauth2")
        self.assertEqual(
            default_connection["auth"]["authorizationUrl"],
            "https://github.com/login/oauth/authorize",
        )
        self.assertEqual(
            default_connection["auth"]["tokenUrl"],
            "https://github.com/login/oauth/access_token",
        )
        self.assertEqual(default_connection["auth"]["scopes"], ["read:user"])
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

    def test_identity_link_self_links_current_subject_to_authenticated_github_user(
        self,
    ) -> None:
        client = RecordingGitHubClient()
        request = gestalt.Request(
            token="ghu-user",
            subject=gestalt.Subject(id="user:gestalt-123"),
        )

        with mock.patch.object(provider_module, "DEFAULT_GITHUB_CLIENT", client):
            response = provider_module.github_identity_link_self({}, request)

        self.assertEqual(
            operation_body(response),
            {
                "ok": True,
                "user": {
                    "id": "222",
                    "login": "ghopper",
                    "name": "Grace Hopper",
                    "email": "grace@example.com",
                },
                "resource": {
                    "type": provider_module.GITHUB_USER_RESOURCE_TYPE,
                    "id": "222",
                },
            },
        )
        self.assertEqual(client.requests, [("GET", "/user", "ghu-user", {})])
        self.assertEqual(len(self.authorization.relationship_adds), 1)
        relationship = self.authorization.relationship_adds[0].relationship
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
            relationship_tuple.relation, provider_module.GITHUB_USER_LINKED_ACTION
        )
        self.assertEqual(resource.type, provider_module.GITHUB_USER_RESOURCE_TYPE)
        self.assertEqual(resource.id, "222")
        self.assertEqual(
            resource.properties,
            {"login": "ghopper", "name": "Grace Hopper"},
        )
        self.assertEqual(
            relationship.source_layer, gestalt.SourceLayerValues.RUNTIME
        )

    def test_catalog_and_schema_expose_events_and_generic_bot_operations(
        self,
    ) -> None:
        plugin_root = pathlib.Path(__file__).resolve().parents[1]
        catalog = yaml.safe_load((plugin_root / "catalog.yaml").read_text())
        operations = {operation["id"]: operation for operation in catalog["operations"]}
        operation_ids = set(operations)

        event = operations[provider_module.GITHUB_EVENT_OPERATION]
        identity = operations[provider_module.IDENTITY_LINK_SELF_OPERATION]
        repo = operations[provider_module.BOT_GET_REPOSITORY_OPERATION]
        search_code = operations[provider_module.BOT_SEARCH_CODE_OPERATION]
        get_content = operations[provider_module.BOT_GET_CONTENT_OPERATION]
        pr = operations[provider_module.BOT_GET_PULL_REQUEST_OPERATION]
        pr_files = operations[provider_module.BOT_LIST_PULL_REQUEST_FILES_OPERATION]
        pr_review = operations[provider_module.BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION]
        pr_reviews = operations[provider_module.BOT_LIST_PULL_REQUEST_REVIEWS_OPERATION]
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
        commit_check_runs = operations[
            provider_module.BOT_LIST_COMMIT_CHECK_RUNS_OPERATION
        ]
        list_workflow_runs = operations[
            provider_module.BOT_LIST_WORKFLOW_RUNS_OPERATION
        ]
        workflow_job_logs = operations[
            provider_module.BOT_GET_WORKFLOW_JOB_LOGS_OPERATION
        ]
        list_issue_comments = operations[
            provider_module.BOT_LIST_ISSUE_COMMENTS_OPERATION
        ]
        search_pull_requests = operations[
            provider_module.BOT_SEARCH_PULL_REQUESTS_OPERATION
        ]
        merge_queue = operations[provider_module.BOT_GET_MERGE_QUEUE_OPERATION]
        list_pull_requests = operations[
            provider_module.BOT_LIST_PULL_REQUESTS_OPERATION
        ]
        list_pull_requests_for_commit = operations[
            provider_module.BOT_LIST_PULL_REQUESTS_FOR_COMMIT_OPERATION
        ]
        list_org_members = operations[provider_module.BOT_LIST_ORG_MEMBERS_OPERATION]
        list_repo_contributors = operations[
            provider_module.BOT_LIST_REPO_CONTRIBUTORS_OPERATION
        ]
        get_user = operations[provider_module.BOT_GET_USER_OPERATION]
        self.assertIn("canonical workflow", event["description"])
        self.assertIn("GitHub user", identity["description"])
        self.assertNotIn("user.createPullRequest", operation_ids)
        self.assertNotIn("reviewPullRequest", operation_ids)
        self.assertFalse(
            any(
                operation_id.startswith("actionPreferences.")
                for operation_id in operation_ids
            )
        )
        self.assertIn("repository metadata", repo["description"])
        self.assertIn("Search code", search_code["description"])
        self.assertIn("file content", get_content["description"])
        self.assertIn("pull request metadata", pr["description"])
        self.assertIn("changed files", pr_files["description"])
        self.assertIn("inline comments", pr_review["description"])
        self.assertIn("pull request reviews", pr_reviews["description"])
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
        self.assertIn("commit ref", commit_check_runs["description"])
        self.assertIn("workflow runs", list_workflow_runs["description"])
        self.assertIn("plain-text logs", workflow_job_logs["description"])
        self.assertIn("comments", list_issue_comments["description"])
        self.assertIn("Search pull requests", search_pull_requests["description"])
        self.assertIn("merge queue", merge_queue["description"])
        self.assertIn("List pull requests", list_pull_requests["description"])
        self.assertIn("commit", list_pull_requests_for_commit["description"])
        self.assertIn("organization members", list_org_members["description"])
        self.assertIn("contributors", list_repo_contributors["description"])
        self.assertIn("user by login", get_user["description"])
        self.assertIn(
            "head_sha",
            [parameter["name"] for parameter in list_workflow_runs["parameters"]],
        )
        self.assertIn(
            "workflow_id",
            [parameter["name"] for parameter in list_workflow_runs["parameters"]],
        )
        self.assertIn(
            "ref",
            [parameter["name"] for parameter in commit_check_runs["parameters"]],
        )
        self.assertIn(
            "job_id",
            [parameter["name"] for parameter in workflow_job_logs["parameters"]],
        )
        self.assertIn(
            "issue_number",
            [parameter["name"] for parameter in list_issue_comments["parameters"]],
        )
        self.assertIn(
            "query",
            [parameter["name"] for parameter in search_pull_requests["parameters"]],
        )
        self.assertIn(
            "branch",
            [parameter["name"] for parameter in merge_queue["parameters"]],
        )
        self.assertIn(
            "commit_sha",
            [
                parameter["name"]
                for parameter in list_pull_requests_for_commit["parameters"]
            ],
        )
        self.assertIn(
            "login",
            [parameter["name"] for parameter in get_user["parameters"]],
        )
        self.assertIn(
            "pull_number", [parameter["name"] for parameter in pr["parameters"]]
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
            "per_page",
            [parameter["name"] for parameter in pr_reviews["parameters"]],
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
        self.assertIn("clientId", schema_properties)
        self.assertIn("clientSecret", schema_properties)
        self.assertEqual(
            schema["required"],
            ["clientId", "clientSecret", "workflow"],
        )
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
            mock.patch.object(
                gestalt.Request,
                "authorization",
                return_value=github_user_link_authorization(),
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
        authorization = github_user_link_authorization()
        with (
            mock.patch.object(
                provider_module, "DEFAULT_GITHUB_CLIENT", recording_client
            ),
            mock.patch.object(
                gestalt.Request,
                "authorization",
                return_value=authorization,
            ),
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
        request = authorization.list_relationships_requests[0]
        self.assertIsNotNone(request.filter)
        assert request.filter is not None
        self.assertIsNotNone(request.filter.target)
        assert request.filter.target is not None
        target_kind = request.filter.target.kind
        self.assertIsInstance(target_kind, RelationshipTargetSubject)
        assert isinstance(target_kind, RelationshipTargetSubject)
        self.assertEqual(target_kind.value.id, "user:gestalt-123")

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
            authorization=FakeAuthorization(),
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
            mock.patch.object(
                gestalt.Request,
                "authorization",
                return_value=github_user_link_authorization(),
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

    def test_webhook_handler_delivers_canonical_workflow_event(self) -> None:
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
                "delivered": True,
                "workflow_event_id": "github:delivery-123",
                "workflow_provider": "local",
            },
        )
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        request = workflow_client.deliver_event_requests[0]
        self.assertEqual(request.provider_name, "local")
        event = request.event
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.id, "github:delivery-123")
        self.assertEqual(event.source, "github")
        self.assertEqual(event.spec_version, "1.0")
        self.assertEqual(event.type, "github.pull_request.opened")
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
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        event = workflow_client.deliver_event_requests[0].event
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.id, f"github:{provider_module.payload_digest(payload)}")
        self.assertEqual(event.type, "github.check_run.completed")
        self.assertEqual(event.subject, "installation:99")
        data = sdk_value_to_dict(event.data)
        self.assertEqual(data["github"]["event_type"], "check_run")
        self.assertEqual(data["github"]["check_run_id"], 1234)
        self.assertNotIn("event_header", data["github"])
        self.assertEqual(data["raw"], payload)

    def test_webhook_handler_uses_base_event_type_when_action_is_absent(
        self,
    ) -> None:
        workflow_client = FakeWorkflowClient()
        payload = {
            "headers": {
                "X-GitHub-Event": "pull_request",
                "X-GitHub-Delivery": "delivery-123",
            },
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

        self.assertEqual(operation_body(result)["ok"], True)
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        event = workflow_client.deliver_event_requests[0].event
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.type, "github.pull_request")
        data = sdk_value_to_dict(event.data)
        self.assertEqual(data["github"]["event_type"], "pull_request")
        self.assertNotIn("action", data["github"])

    def test_webhook_handler_qualifies_inferred_repository_event_type(
        self,
    ) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "webhookEvents": ["repository"],
            },
        )
        workflow_client = FakeWorkflowClient()
        payload = {
            "action": "deleted",
            "installation": {"id": 99},
            "repository": {"full_name": "acme/widgets"},
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
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)
        event = workflow_client.deliver_event_requests[0].event
        self.assertIsNotNone(event)
        assert event is not None
        self.assertEqual(event.type, "github.repository.deleted")
        data = sdk_value_to_dict(event.data)
        self.assertEqual(data["github"]["event_type"], "repository")
        self.assertEqual(data["github"]["action"], "deleted")

    def test_webhook_handler_delivery_failure_is_retryable_server_error(self) -> None:
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
            {"error": "failed to deliver workflow event: workflow client unavailable"},
        )
        self.assertEqual(len(workflow_client.deliver_event_requests), 1)

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
            mock.patch.object(
                gestalt.Request,
                "authorization",
                return_value=github_user_link_authorization(),
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
        data = cast(dict[str, Any], result)["commit"]
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
            mock.patch.object(
                gestalt.Request,
                "authorization",
                return_value=github_user_link_authorization(),
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

        data = cast(dict[str, Any], result)
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

        data = cast(dict[str, Any], result)["pull_request"]
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

        data = cast(dict[str, Any], result)["pull_request"]
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

        data = cast(dict[str, Any], result)["comment"]
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

    def test_create_issue_uses_issue_write_permission(self) -> None:
        calls: list[tuple[str, str, dict[str, Any], str]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            calls.append((method, path, body, auth_header(request)))

            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
            if path == "/app/installations/99/access_tokens":
                self.assertEqual(body["permissions"], {"issues": "write"})
                return FakeHTTPResponse({"token": "issue-token"})
            if path == "/repos/acme/widgets/issues":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer issue-token")
                self.assertEqual(
                    body,
                    {
                        "title": "Broken modal",
                        "body": "The modal does not close.",
                        "labels": ["bug"],
                        "assignees": ["example-user"],
                    },
                )
                return FakeHTTPResponse(
                    {
                        "number": 12,
                        "title": "Broken modal",
                        "body": "The modal does not close.",
                        "state": "open",
                        "html_url": "https://github.com/acme/widgets/issues/12",
                        "url": "https://api.github.com/repos/acme/widgets/issues/12",
                        "id": 1200,
                        "node_id": "I_kw",
                        "created_at": "2026-05-01T00:00:00Z",
                        "updated_at": "2026-05-01T00:00:00Z",
                        "closed_at": None,
                        "labels": [{"id": 1, "node_id": "LA_kw", "name": "bug"}],
                        "assignees": [{"login": "example-user"}],
                    }
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_create_issue(
                provider_module.CreateIssueInput(
                    owner="acme",
                    repo="widgets",
                    title="Broken modal",
                    body="The modal does not close.",
                    labels=["bug"],
                    assignees=["example-user"],
                ),
                github_request(),
            )

        data = cast(dict[str, Any], result)["issue"]
        self.assertEqual(data["number"], 12)
        self.assertEqual(data["state"], "open")
        self.assertEqual(data["html_url"], "https://github.com/acme/widgets/issues/12")
        self.assertEqual(
            [
                call[2].get("permissions")
                for call in calls
                if call[1].endswith("access_tokens")
            ],
            [{"issues": "write"}],
        )

    def test_update_issue_uses_issue_write_permission(self) -> None:
        calls: list[tuple[str, str, dict[str, Any], str]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            calls.append((method, path, body, auth_header(request)))

            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
            if path == "/app/installations/99/access_tokens":
                self.assertEqual(body["permissions"], {"issues": "write"})
                return FakeHTTPResponse({"token": "issue-token"})
            if path == "/repos/acme/widgets/issues/12":
                self.assertEqual(method, "PATCH")
                self.assertEqual(auth_header(request), "Bearer issue-token")
                self.assertEqual(
                    body,
                    {
                        "title": "Broken modal (fixed)",
                        "body": "Updated description.",
                        "state": "closed",
                    },
                )
                return FakeHTTPResponse(
                    {
                        "number": 12,
                        "title": "Broken modal (fixed)",
                        "body": "Updated description.",
                        "state": "closed",
                        "html_url": "https://github.com/acme/widgets/issues/12",
                        "url": "https://api.github.com/repos/acme/widgets/issues/12",
                        "id": 1200,
                        "node_id": "I_kw",
                        "created_at": "2026-05-01T00:00:00Z",
                        "updated_at": "2026-05-02T00:00:00Z",
                        "closed_at": "2026-05-02T00:00:00Z",
                    }
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_update_issue(
                provider_module.UpdateIssueInput(
                    owner="acme",
                    repo="widgets",
                    issue_number=12,
                    title="Broken modal (fixed)",
                    body="Updated description.",
                    state="closed",
                ),
                github_request(),
            )

        data = cast(dict[str, Any], result)["issue"]
        self.assertEqual(data["number"], 12)
        self.assertEqual(data["state"], "closed")
        self.assertEqual(data["closed_at"], "2026-05-02T00:00:00Z")

    def test_get_issue_uses_issue_read_permission(self) -> None:
        calls: list[tuple[str, str, dict[str, Any], str]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            calls.append((method, path, body, auth_header(request)))

            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
            if path == "/app/installations/99/access_tokens":
                self.assertEqual(body["permissions"], {"issues": "read"})
                return FakeHTTPResponse({"token": "issue-token"})
            if path == "/repos/acme/widgets/issues/12":
                self.assertEqual(method, "GET")
                self.assertEqual(auth_header(request), "Bearer issue-token")
                return FakeHTTPResponse(
                    {
                        "number": 12,
                        "title": "Broken modal",
                        "body": "The modal does not close.",
                        "state": "open",
                        "html_url": "https://github.com/acme/widgets/issues/12",
                        "url": "https://api.github.com/repos/acme/widgets/issues/12",
                        "id": 1200,
                        "node_id": "I_kw",
                        "created_at": "2026-05-01T00:00:00Z",
                        "updated_at": "2026-05-01T00:00:00Z",
                        "closed_at": None,
                    }
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_get_issue(
                provider_module.GetIssueInput(
                    owner="acme",
                    repo="widgets",
                    issue_number=12,
                ),
                github_request(),
            )

        data = cast(dict[str, Any], result)["issue"]
        self.assertEqual(data["number"], 12)
        self.assertEqual(data["title"], "Broken modal")
        self.assertEqual(
            [
                call[2].get("permissions")
                for call in calls
                if call[1].endswith("access_tokens")
            ],
            [{"issues": "read"}],
        )

    def test_list_issues_uses_issue_read_permission(self) -> None:
        calls: list[tuple[str, str, dict[str, Any], str]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            method = request.get_method()
            path = request_path(request)
            query = urllib.parse.parse_qs(urllib.parse.urlparse(request.full_url).query)
            body = request_json(request)
            calls.append((method, path, body, auth_header(request)))

            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
            if path == "/app/installations/99/access_tokens":
                self.assertEqual(body["permissions"], {"issues": "read"})
                return FakeHTTPResponse({"token": "issue-token"})
            if path == "/repos/acme/widgets/issues":
                self.assertEqual(method, "GET")
                self.assertEqual(auth_header(request), "Bearer issue-token")
                self.assertEqual(query["state"], ["all"])
                self.assertEqual(query["sort"], ["created"])
                self.assertEqual(query["direction"], ["desc"])
                self.assertEqual(query["per_page"], ["100"])
                return FakeHTTPResponse(
                    [
                        {
                            "number": 12,
                            "title": "Broken modal",
                            "body": "The modal does not close.",
                            "state": "open",
                            "html_url": "https://github.com/acme/widgets/issues/12",
                            "url": "https://api.github.com/repos/acme/widgets/issues/12",
                            "id": 1200,
                            "node_id": "I_kw",
                            "created_at": "2026-05-01T00:00:00Z",
                            "updated_at": "2026-05-01T00:00:00Z",
                            "closed_at": None,
                        }
                    ]
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_list_issues(
                provider_module.ListIssuesInput(
                    owner="acme",
                    repo="widgets",
                    state="all",
                    per_page=100,
                ),
                github_request(),
            )

        data = cast(dict[str, Any], result)
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["issues"][0]["number"], 12)
        self.assertEqual(data["issues"][0]["title"], "Broken modal")
        self.assertEqual(
            [
                call[2].get("permissions")
                for call in calls
                if call[1].endswith("access_tokens")
            ],
            [{"issues": "read"}],
        )

    def test_issue_summary_preserves_labels_and_assignees(self) -> None:
        summary = operations_module.issue_summary(
            {
                "number": 12,
                "title": "Broken modal",
                "body": "The modal does not close.",
                "state": "open",
                "html_url": "https://github.com/acme/widgets/issues/12",
                "url": "https://api.github.com/repos/acme/widgets/issues/12",
                "id": 1200,
                "node_id": "I_kw",
                "created_at": "2026-05-01T00:00:00Z",
                "updated_at": "2026-05-01T00:00:00Z",
                "closed_at": None,
                "labels": [
                    {
                        "id": 1,
                        "node_id": "LA_kw",
                        "name": "bug",
                        "color": "f29513",
                    }
                ],
                "assignees": [{"login": "example-user"}],
            }
        )
        self.assertEqual(summary["labels"][0]["name"], "bug")
        self.assertEqual(summary["assignees"][0]["login"], "example-user")

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

        data = cast(dict[str, Any], result)["comment"]
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

        data = cast(dict[str, Any], result)["review"]
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

        self.assertEqual(cast(dict[str, Any], issue)["reaction"]["id"], 1)
        self.assertEqual(
            cast(dict[str, Any], pull_request)["reaction"]["content"],
            "heart",
        )
        self.assertEqual(
            cast(dict[str, Any], issue_comment)["reaction"]["content"],
            "rocket",
        )
        self.assertEqual(
            cast(dict[str, Any], review_comment)["reaction"]["user"]["login"],
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
            cast(dict[str, Any], added)["labels"],
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
            cast(dict[str, Any], removed)["removed"], ["needs review/triage"]
        )
        self.assertEqual(
            cast(dict[str, Any], removed)["labels"],
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

        data = cast(dict[str, Any], result)["pull_request"]
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
            authorization=FakeAuthorization(),
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
            authorization=FakeAuthorization(),
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
                authorization=FakeAuthorization(),
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
                        "merged": False,
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
            if path == "/repos/acme/widgets/pulls/7/reviews":
                self.assertEqual(method, "GET")
                self.assertEqual(
                    urllib.parse.urlparse(request.full_url).query,
                    "per_page=100&page=1",
                )
                self.assertEqual(auth_header(request), "Bearer pr-read-token")
                return FakeHTTPResponse(
                    [
                        {
                            "id": 42,
                            "node_id": "PRR_kwDO",
                            "state": "APPROVED",
                            "html_url": "https://github.com/acme/widgets/pull/7#pullrequestreview-42",
                            "pull_request_url": "https://api.github.com/repos/acme/widgets/pulls/7",
                            "commit_id": "abc123",
                            "body": "Looks good",
                            "user": {"login": "octocat"},
                            "submitted_at": "2026-05-01T12:00:00Z",
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
            reviews = provider_module.bot_list_pull_request_reviews(
                provider_module.ListPullRequestReviewsInput(
                    owner="acme",
                    repo="widgets",
                    pull_number=7,
                    per_page=100,
                    page=1,
                ),
                github_request(),
            )

        pull_data = cast(dict[str, Any], pull_request)["pull_request"]
        self.assertEqual(pull_data["head_sha"], "abc123")
        self.assertEqual(pull_data["base_sha"], "def456")
        self.assertEqual(pull_data["head_ref"], "feature")
        self.assertEqual(pull_data["base_ref"], "main")
        self.assertEqual(pull_data["merged"], False)
        self.assertEqual(pull_data["head_repo"]["full_name"], "acme/widgets")
        self.assertEqual(pull_data["base_repo"]["full_name"], "acme/widgets")
        self.assertEqual(pull_data["head_repo_is_base_repo"], True)
        self.assertEqual(pull_data["maintainer_can_modify"], False)
        file_data = cast(dict[str, Any], files)["files"][0]
        self.assertEqual(file_data["filename"], "src/widget.py")
        self.assertEqual(file_data["previous_filename"], "src/old_widget.py")
        self.assertEqual(file_data["changes"], 4)
        self.assertEqual(file_data["patch_limit"], 8192)
        self.assertEqual(file_data["patch_truncated"], True)
        self.assertLess(len(file_data["patch"]), len(long_patch))
        self.assertLessEqual(len(file_data["patch"]), file_data["patch_limit"])
        self.assertTrue(file_data["patch"].endswith("\n...<truncated>"))
        short_file_data = cast(dict[str, Any], files)["files"][1]
        self.assertEqual(short_file_data["patch"], "@@ -1 +1 @@\n-value\n+value  ")
        self.assertEqual(short_file_data["patch_truncated"], False)
        self.assertEqual(cast(dict[str, Any], files)["count"], 2)
        review_data = cast(dict[str, Any], reviews)["reviews"][0]
        self.assertEqual(review_data["state"], "APPROVED")
        self.assertEqual(review_data["user"]["login"], "octocat")
        self.assertEqual(cast(dict[str, Any], reviews)["count"], 1)
        self.assertEqual(
            [
                call[2].get("permissions")
                for call in calls
                if call[1].endswith("access_tokens")
            ],
            [
                {"pull_requests": "read"},
                {"pull_requests": "read"},
                {"pull_requests": "read"},
            ],
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
                github_authorized_request(),
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
                github_authorized_request(),
            )
            content = provider_module.bot_get_content(
                provider_module.GetContentInput(
                    owner="acme",
                    repo="widgets",
                    path="src/widget.py",
                ),
                github_authorized_request(),
            )

        self.assertEqual(
            cast(dict[str, Any], repo)["repository"]["full_name"],
            "acme/widgets",
        )
        self.assertEqual(
            cast(dict[str, Any], search)["items"][0]["path"],
            "src/widget.py",
        )
        self.assertEqual(
            cast(dict[str, Any], content)["content"], "hello world\n"
        )
        self.assertEqual(
            [
                call[2].get("permissions")
                for call in calls
                if call[1].endswith("access_tokens")
            ],
            [{"contents": "read"}, {"contents": "read"}, {"contents": "read"}],
        )

    def test_bot_list_commits_succeeds_with_top_level_array_response(self) -> None:
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
                self.assertEqual(body["permissions"], {"contents": "read"})
                return FakeHTTPResponse({"token": "contents-read-token"})
            if path == "/repos/acme/widgets/commits":
                self.assertEqual(auth_header(request), "Bearer contents-read-token")
                query = urllib.parse.parse_qs(
                    urllib.parse.urlparse(request.full_url).query
                )
                self.assertEqual(query["sha"], ["main"])
                self.assertEqual(query["path"], ["src/widget.py"])
                self.assertEqual(query["per_page"], ["2"])
                self.assertEqual(query["page"], ["3"])
                return FakeHTTPResponse(
                    [
                        {
                            "sha": "abc123",
                            "html_url": "https://github.com/acme/widgets/commit/abc123",
                            "commit": {
                                "message": "Fix widget",
                                "author": {
                                    "name": "Ada",
                                    "date": "2026-06-16T00:00:00Z",
                                },
                            },
                        }
                    ]
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_list_commits(
                provider_module.ListCommitsInput(
                    owner="acme",
                    repo="widgets",
                    sha="main",
                    path="src/widget.py",
                    per_page=2,
                    page=3,
                ),
                github_authorized_request(),
            )

        data = cast(dict[str, Any], result)
        self.assertEqual(data["commits"][0]["sha"], "abc123")
        self.assertEqual(
            [
                call[2].get("permissions")
                for call in calls
                if call[1].endswith("access_tokens")
            ],
            [{"contents": "read"}],
        )

    def test_bot_list_commits_returns_bad_gateway_for_object_response(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            path = request_path(request)
            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
            if path == "/app/installations/99/access_tokens":
                return FakeHTTPResponse({"token": "contents-read-token"})
            if path == "/repos/acme/widgets/commits":
                return FakeHTTPResponse({"message": "not a list"})
            self.fail(f"unexpected request {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_list_commits(
                provider_module.ListCommitsInput(owner="acme", repo="widgets"),
                github_authorized_request(),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_GATEWAY)
        self.assertIn("not a list", response.body["error"])

    def test_bot_compare_refs_succeeds_with_object_response(self) -> None:
        calls: list[tuple[str, str, dict[str, Any], str]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            calls.append((method, path, body, auth_header(request)))

            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
            if path == "/app/installations/99/access_tokens":
                self.assertEqual(body["repositories"], ["widgets"])
                self.assertEqual(body["permissions"], {"contents": "read"})
                return FakeHTTPResponse({"token": "contents-read-token"})
            if path == "/repos/acme/widgets/compare/main...feature":
                self.assertEqual(auth_header(request), "Bearer contents-read-token")
                return FakeHTTPResponse(
                    {
                        "status": "ahead",
                        "ahead_by": 1,
                        "behind_by": 0,
                        "total_commits": 1,
                        "html_url": "https://github.com/acme/widgets/compare/main...feature",
                        "permalink_url": "https://github.com/acme/widgets/compare/acme:main...acme:feature",
                        "commits": [],
                        "files": [],
                    }
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_compare_refs(
                provider_module.CompareRefsInput(
                    owner="acme",
                    repo="widgets",
                    base="main",
                    head="feature",
                ),
                github_authorized_request(),
            )

        data = cast(dict[str, Any], result)
        self.assertEqual(data["status"], "ahead")
        self.assertEqual(
            [
                call[2].get("permissions")
                for call in calls
                if call[1].endswith("access_tokens")
            ],
            [{"contents": "read"}],
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
            cast(dict[str, Any], created_check)["check_run"]["id"], 999
        )
        self.assertEqual(
            cast(dict[str, Any], completed_created_check)["check_run"][
                "status"
            ],
            "completed",
        )
        self.assertEqual(
            cast(dict[str, Any], updated_check)["check_run"]["conclusion"],
            "success",
        )
        self.assertEqual(
            cast(dict[str, Any], check_run)["check_run"]["id"], 123
        )
        self.assertEqual(
            cast(dict[str, Any], annotations)["annotations"][0]["message"],
            "broken",
        )
        self.assertEqual(
            cast(dict[str, Any], suite_runs)["check_runs"][0]["id"], 654
        )
        self.assertEqual(
            cast(dict[str, Any], workflow_run)["workflow_run"]["name"], "CI"
        )
        self.assertEqual(cast(dict[str, Any], jobs)["jobs"][0]["id"], 789)
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

    def test_workflow_job_logs_redirect_handler_strips_authorization(self) -> None:
        handler = client_module._WorkflowJobLogsRedirectHandler()
        request = urllib.request.Request(
            "https://api.github.com/repos/acme/widgets/actions/jobs/789/logs",
            headers={"Authorization": "Bearer actions-token"},
        )
        headers = HTTPMessage()
        headers.add_header("Location", "https://objects.github.example/log.txt")
        redirected = handler.redirect_request(
            request,
            io.BytesIO(b""),
            302,
            "Found",
            headers,
            "https://objects.github.example/log.txt",
        )
        self.assertIsNotNone(redirected)
        assert redirected is not None
        self.assertEqual(auth_header(redirected), "")

    def test_workflow_job_logs_returns_opener_body(self) -> None:
        response = mock.Mock()
        response.read.return_value = b"job log content\n"
        response.__enter__ = mock.Mock(return_value=response)
        response.__exit__ = mock.Mock(return_value=None)
        opener = mock.Mock()
        opener.open.return_value = response
        with mock.patch(
            "internals.client.urllib.request.build_opener", return_value=opener
        ):
            logs = client_module.workflow_job_logs(
                "actions-token", "acme", "widgets", 789
            )
        self.assertEqual(logs, "job log content\n")

    def test_cicd_bot_read_operations_use_expected_paths_and_permissions(self) -> None:
        calls: list[tuple[str, str, dict[str, Any], str]] = []
        graphql_calls: list[tuple[str, dict[str, Any], str]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            method = request.get_method()
            path = request_path(request)
            query = urllib.parse.parse_qs(urllib.parse.urlparse(request.full_url).query)
            body = request_json(request)
            calls.append((method, path, body, auth_header(request)))

            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
            if path == "/app/installations/99/access_tokens":
                permissions = body.get("permissions", {})
                if permissions == {"actions": "read"}:
                    return FakeHTTPResponse({"token": "actions-token"})
                if permissions == {"issues": "read"}:
                    return FakeHTTPResponse({"token": "issues-token"})
                if permissions == {"pull_requests": "read"}:
                    return FakeHTTPResponse({"token": "pull-requests-token"})
                if permissions == {"pull_requests": "read", "contents": "read"}:
                    return FakeHTTPResponse({"token": "search-pull-requests-token"})
                if permissions == {"contents": "read"}:
                    return FakeHTTPResponse({"token": "contents-token"})
                if permissions == {"members": "read"}:
                    return FakeHTTPResponse({"token": "members-token"})
                if permissions == {"metadata": "read"}:
                    return FakeHTTPResponse({"token": "metadata-token"})
                self.fail(f"unexpected permissions {permissions}")
            if path == "/repos/acme/widgets/actions/runs":
                self.assertEqual(
                    urllib.parse.parse_qs(
                        urllib.parse.urlparse(request.full_url).query
                    ),
                    urllib.parse.parse_qs(
                        "branch=main&event=push&head_sha=abc123&per_page=10&page=1"
                    ),
                )
                return FakeHTTPResponse(
                    {
                        "total_count": 1,
                        "workflow_runs": [
                            {
                                "id": 456,
                                "name": "CI",
                                "status": "completed",
                                "conclusion": "success",
                                "head_sha": "abc123",
                            }
                        ],
                    }
                )
            if path == "/repos/acme/widgets/issues/42/comments":
                return FakeHTTPResponse(
                    [
                        {
                            "id": 1,
                            "body": "deployed",
                            "user": {"login": "bot"},
                            "created_at": "2026-05-01T00:00:00Z",
                        }
                    ]
                )
            if path == "/repos/acme/widgets/pulls":
                return FakeHTTPResponse(
                    [
                        {
                            "number": 7,
                            "title": "Feature",
                            "state": "open",
                            "html_url": "https://github.com/acme/widgets/pull/7",
                        }
                    ]
                )
            if path == "/repos/acme/widgets/commits/abc123/pulls":
                return FakeHTTPResponse(
                    [
                        {
                            "number": 7,
                            "title": "Feature",
                            "state": "open",
                            "html_url": "https://github.com/acme/widgets/pull/7",
                        }
                    ]
                )
            if path == "/orgs/acme/members":
                return FakeHTTPResponse(
                    [
                        {
                            "id": 11,
                            "login": "octocat",
                            "type": "User",
                            "html_url": "https://github.com/octocat",
                        }
                    ]
                )
            if path == "/repos/acme/widgets/contributors":
                return FakeHTTPResponse(
                    [
                        {
                            "id": 11,
                            "login": "octocat",
                            "contributions": 42,
                            "type": "User",
                        }
                    ]
                )
            if path == "/users/octocat":
                return FakeHTTPResponse(
                    {
                        "id": 11,
                        "login": "octocat",
                        "name": "The Octocat",
                        "type": "User",
                        "html_url": "https://github.com/octocat",
                    }
                )
            self.fail(f"unexpected request {method} {path} {query}")

        def fake_graphql_json(
            query: str, token: str | None, variables: Mapping[str, Any] | None = None
        ) -> dict[str, Any]:
            graphql_calls.append((query, dict(variables or {}), token or ""))
            if "GestaltSearchPullRequests" in query:
                return {
                    "data": {
                        "search": {
                            "issueCount": 1,
                            "pageInfo": {
                                "hasNextPage": False,
                                "endCursor": "cursor-1",
                            },
                            "edges": [
                                {
                                    "cursor": "edge-cursor-1",
                                    "node": {
                                        "number": 7,
                                        "title": "Feature",
                                        "state": "OPEN",
                                        "url": "https://github.com/acme/widgets/pull/7",
                                        "createdAt": "2026-05-01T00:00:00Z",
                                        "updatedAt": "2026-05-02T00:00:00Z",
                                        "mergedAt": None,
                                        "author": {"login": "octocat"},
                                        "headRefName": "feature",
                                        "baseRefName": "main",
                                        "headRefOid": "abc123def456",
                                        "mergeCommit": {"oid": "merge123"},
                                        "commits": {
                                            "nodes": [
                                                {
                                                    "commit": {
                                                        "committedDate": "2026-04-30T00:00:00Z",
                                                        "author": {
                                                            "email": "octocat@github.com",
                                                            "name": "The Octocat",
                                                        },
                                                    }
                                                }
                                            ]
                                        },
                                    },
                                }
                            ],
                        }
                    }
                }
            if "GestaltMergeQueue" in query:
                return {
                    "data": {
                        "repository": {
                            "mergeQueue": {
                                "entries": {
                                    "totalCount": 1,
                                    "nodes": [
                                        {
                                            "position": 1,
                                            "enqueuedAt": "2026-05-01T00:00:00Z",
                                            "state": "QUEUED",
                                            "headCommit": {"oid": "abc123"},
                                            "pullRequest": {
                                                "number": 42,
                                                "title": "Queued PR",
                                            },
                                        }
                                    ]
                                },
                            }
                        }
                    }
                }
            self.fail("unexpected graphql query")

        job_logs_calls: list[tuple[str, str]] = []

        def fake_workflow_job_logs(
            token: str, owner: str, repo: str, job_id: int
        ) -> str:
            job_logs_calls.append((token, f"/repos/{owner}/{repo}/actions/jobs/{job_id}/logs"))
            return "job log content\n"

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch(
                "internals.client.graphql_json", side_effect=fake_graphql_json
            ),
            mock.patch(
                "internals.client.workflow_job_logs", side_effect=fake_workflow_job_logs
            ),
        ):
            workflow_runs = provider_module.bot_list_workflow_runs(
                provider_module.ListWorkflowRunsInput(
                    owner="acme",
                    repo="widgets",
                    branch="main",
                    event="push",
                    head_sha="abc123",
                    per_page=10,
                    page=1,
                ),
                github_request(),
            )
            logs = provider_module.bot_get_workflow_job_logs(
                provider_module.GetWorkflowJobLogsInput(
                    owner="acme",
                    repo="widgets",
                    job_id=789,
                ),
                github_request(),
            )
            comments = provider_module.bot_list_issue_comments(
                provider_module.ListIssueCommentsInput(
                    owner="acme",
                    repo="widgets",
                    issue_number=42,
                ),
                github_request(),
            )
            search = provider_module.bot_search_pull_requests(
                provider_module.SearchPullRequestsInput(
                    owner="acme",
                    repo="widgets",
                    query="repo:acme/widgets is:pr author:octocat",
                    first=25,
                ),
                github_request(),
            )
            merge_queue = provider_module.bot_get_merge_queue(
                provider_module.GetMergeQueueInput(
                    owner="acme",
                    repo="widgets",
                    branch="main",
                    first=50,
                ),
                github_request(),
            )
            pulls = provider_module.bot_list_pull_requests(
                provider_module.ListPullRequestsInput(
                    owner="acme",
                    repo="widgets",
                    state="open",
                ),
                github_request(),
            )
            commit_pulls = provider_module.bot_list_pull_requests_for_commit(
                provider_module.ListPullRequestsForCommitInput(
                    owner="acme",
                    repo="widgets",
                    commit_sha="abc123",
                ),
                github_request(),
            )
            members = provider_module.bot_list_org_members(
                provider_module.ListOrgMembersInput(
                    owner="acme",
                    repo="widgets",
                ),
                github_request(),
            )
            contributors = provider_module.bot_list_repo_contributors(
                provider_module.ListRepoContributorsInput(
                    owner="acme",
                    repo="widgets",
                ),
                github_request(),
            )
            user = provider_module.bot_get_user(
                provider_module.GetUserInput(
                    owner="acme",
                    repo="widgets",
                    login="octocat",
                ),
                github_request(),
            )

        self.assertEqual(
            cast(dict[str, Any], workflow_runs)["workflow_runs"][0]["id"], 456
        )
        self.assertEqual(cast(dict[str, Any], logs)["logs"], "job log content\n")
        self.assertEqual(
            cast(dict[str, Any], comments)["comments"][0]["body"], "deployed"
        )
        self.assertEqual(
            cast(dict[str, Any], search)["pull_requests"][0]["number"], 7
        )
        self.assertEqual(
            cast(dict[str, Any], search)["pull_requests"][0]["head_sha"],
            "abc123def456",
        )
        self.assertEqual(
            cast(dict[str, Any], search)["pull_requests"][0]["merge_commit_sha"],
            "merge123",
        )
        self.assertEqual(
            cast(dict[str, Any], search)["pull_requests"][0]["committed_at"],
            "2026-04-30T00:00:00Z",
        )
        self.assertEqual(
            cast(dict[str, Any], search)["pull_requests"][0]["author_email"],
            "octocat@github.com",
        )
        self.assertEqual(
            cast(dict[str, Any], search)["pull_requests"][0]["cursor"],
            "edge-cursor-1",
        )
        self.assertEqual(
            cast(dict[str, Any], merge_queue)["merge_queue"]["total_count"], 1
        )
        self.assertEqual(
            cast(dict[str, Any], merge_queue)["merge_queue"]["entries"][0][
                "merge_request_state"
            ],
            "QUEUED",
        )
        self.assertEqual(
            cast(dict[str, Any], merge_queue)["merge_queue"]["entries"][0][
                "pull_request"
            ]["number"],
            42,
        )
        self.assertNotIn("mergeRequest", MERGE_QUEUE_QUERY)
        self.assertIn("state", MERGE_QUEUE_QUERY)
        self.assertIn("pullRequest", MERGE_QUEUE_QUERY)
        self.assertIn("headRefOid", SEARCH_PULL_REQUESTS_QUERY)
        self.assertIn("edges", SEARCH_PULL_REQUESTS_QUERY)
        merge_queue_query = graphql_calls[1][0]
        self.assertNotIn("mergeRequest", merge_queue_query)
        self.assertEqual(
            cast(dict[str, Any], pulls)["pull_requests"][0]["number"], 7
        )
        self.assertEqual(
            cast(dict[str, Any], commit_pulls)["pull_requests"][0]["number"], 7
        )
        self.assertEqual(
            cast(dict[str, Any], members)["members"][0]["login"], "octocat"
        )
        self.assertEqual(
            cast(dict[str, Any], contributors)["contributors"][0][
                "contributions"
            ],
            42,
        )
        self.assertEqual(
            cast(dict[str, Any], user)["user"]["login"], "octocat"
        )
        self.assertEqual(
            job_logs_calls,
            [("actions-token", "/repos/acme/widgets/actions/jobs/789/logs")],
        )
        self.assertEqual(graphql_calls[0][1]["query"], "repo:acme/widgets is:pr author:octocat")
        self.assertEqual(graphql_calls[1][1]["branch"], "main")

    def test_list_workflow_runs_with_workflow_id_uses_workflow_scoped_path(self) -> None:
        requested_paths: list[str] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            path = request_path(request)
            requested_paths.append(path)
            body = request_json(request)
            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
            if path == "/app/installations/99/access_tokens":
                permissions = body.get("permissions", {})
                if permissions == {"actions": "read"}:
                    return FakeHTTPResponse({"token": "actions-token"})
                self.fail(f"unexpected permissions {permissions}")
            if path == "/repos/acme/widgets/actions/workflows/12345/runs":
                return FakeHTTPResponse(
                    {
                        "total_count": 1,
                        "workflow_runs": [
                            {
                                "id": 99,
                                "name": "Combined",
                                "status": "completed",
                                "conclusion": "success",
                                "head_sha": "abc123",
                            }
                        ],
                    }
                )
            self.fail(f"unexpected request {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_list_workflow_runs(
                provider_module.ListWorkflowRunsInput(
                    owner="acme",
                    repo="widgets",
                    workflow_id="12345",
                    event="merge_group",
                ),
                github_request(),
            )

        self.assertIn(
            "/repos/acme/widgets/actions/workflows/12345/runs",
            requested_paths,
        )
        self.assertEqual(
            cast(dict[str, Any], result)["workflow_runs"][0]["id"], 99
        )

    def test_list_commit_check_runs_uses_commit_ref_path_and_maps_output(self) -> None:
        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            path = request_path(request)
            body = request_json(request)
            if path == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
            if path == "/app/installations/99/access_tokens":
                permissions = body.get("permissions", {})
                if permissions == {"checks": "read"}:
                    return FakeHTTPResponse({"token": "checks-token"})
                self.fail(f"unexpected permissions {permissions}")
            if path == "/repos/acme/widgets/commits/abc123/check-runs":
                return FakeHTTPResponse(
                    {
                        "total_count": 1,
                        "check_runs": [
                            {
                                "id": 55,
                                "name": "CI",
                                "status": "completed",
                                "conclusion": "success",
                                "output": {
                                    "title": "All checks passed",
                                    "summary": "Everything is green",
                                },
                            }
                        ],
                    }
                )
            self.fail(f"unexpected request {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
        ):
            result = provider_module.bot_list_commit_check_runs(
                provider_module.ListCommitCheckRunsInput(
                    owner="acme",
                    repo="widgets",
                    ref="abc123",
                ),
                github_request(),
            )

        data = cast(dict[str, Any], result)
        self.assertEqual(data["check_runs"][0]["id"], 55)
        self.assertEqual(data["check_runs"][0]["output"]["title"], "All checks passed")

    def test_list_commit_check_runs_requires_ref(self) -> None:
        result = provider_module.bot_list_commit_check_runs(
            provider_module.ListCommitCheckRunsInput(
                owner="acme",
                repo="widgets",
                ref="",
            ),
            github_request(),
        )
        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)

    def test_cicd_bot_read_operations_return_forbidden_when_unauthorized(self) -> None:
        class UnauthorizedRequest:
            subject = github_request().subject

            def authorization(self) -> FakeAuthorization:
                return FakeAuthorization(allowed=False)

        result = provider_module.bot_list_workflow_runs(
            provider_module.ListWorkflowRunsInput(
                owner="acme",
                repo="widgets",
            ),
            UnauthorizedRequest(),  # type: ignore[arg-type]
        )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)


class ExtendedSummaryTests(unittest.TestCase):
    def test_workflow_run_summary_includes_display_title_and_head_ref(self) -> None:
        summary = operations_module.workflow_run_summary(
            {
                "id": 456,
                "name": "CI",
                "display_title": "Deploy (#9)",
                "status": "completed",
                "conclusion": "success",
                "head_branch": "pr-9-feature",
                "head_sha": "abc123",
                "path": ".github/workflows/combined.yml",
                "workflow_id": 224113632,
            }
        )
        self.assertEqual(summary["display_title"], "Deploy (#9)")
        self.assertEqual(summary["head_ref"], "pr-9-feature")
        self.assertEqual(summary["path"], ".github/workflows/combined.yml")
        self.assertEqual(summary["workflow_id"], 224113632)

    def test_workflow_run_job_summary_includes_created_at_and_steps(self) -> None:
        summary = operations_module.workflow_run_job_summary(
            {
                "id": 789,
                "run_id": 456,
                "name": "ci-fp-test",
                "status": "completed",
                "conclusion": "failure",
                "created_at": "2026-05-01T00:00:00Z",
                "started_at": "2026-05-01T00:01:00Z",
                "completed_at": "2026-05-01T00:10:00Z",
                "html_url": "https://github.com/runs/789",
                "steps": [
                    {
                        "name": "run tests",
                        "status": "completed",
                        "conclusion": "failure",
                        "number": 1,
                    }
                ],
            }
        )
        self.assertEqual(summary["created_at"], "2026-05-01T00:00:00Z")
        self.assertEqual(summary["steps"][0]["name"], "run tests")
        self.assertEqual(summary["steps"][0]["conclusion"], "failure")

    def test_commit_summary_includes_author_email_and_committer_date(self) -> None:
        summary = operations_module.commit_summary(
            {
                "sha": "abc123",
                "commit": {
                    "message": "Fix widget",
                    "author": {
                        "name": "Ada",
                        "email": "ada@valon.com",
                        "date": "2026-06-16T00:00:00Z",
                    },
                    "committer": {
                        "name": "Ada",
                        "email": "ada@valon.com",
                        "date": "2026-06-16T01:00:00Z",
                    },
                },
            }
        )
        self.assertEqual(summary["author_email"], "ada@valon.com")
        self.assertEqual(summary["date"], "2026-06-16T01:00:00Z")

    def test_pull_request_summary_includes_user_created_at_and_merge_commit_sha(self) -> None:
        summary = operations_module.pull_request_summary(
            {
                "number": 7,
                "title": "Feature",
                "state": "closed",
                "html_url": "https://github.com/acme/widgets/pull/7",
                "user": {"login": "octocat"},
                "created_at": "2026-05-01T00:00:00Z",
                "merge_commit_sha": "merge123",
                "head": {"ref": "feature", "sha": "abc123"},
                "base": {"ref": "main", "sha": "def456"},
            }
        )
        self.assertEqual(summary["user"], "octocat")
        self.assertEqual(summary["created_at"], "2026-05-01T00:00:00Z")
        self.assertEqual(summary["merge_commit_sha"], "merge123")


if __name__ == "__main__":
    unittest.main()
