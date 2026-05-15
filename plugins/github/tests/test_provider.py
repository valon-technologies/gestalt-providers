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
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

import gestalt
import yaml

import internals.client as client_module
import internals.identity as identity_module
import internals.operations as operations_module
import internals.preferences as preferences_module
import internals.review as review_module
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
        signal = request.signal or gestalt.WorkflowSignal()
        return gestalt.WorkflowManagerRunSignal(
            provider_name=request.provider_name,
            run=gestalt.WorkflowManagerBoundRun(
                id="workflow-run-123",
                status=gestalt.WORKFLOW_RUN_STATUS_RUNNING,
                target=request.target,
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


class FakeObjectStore:
    def __init__(self, records: dict[str, dict[str, Any]]) -> None:
        self.records = records
        self.fail = False

    def get(self, record_id: str) -> dict[str, Any]:
        if self.fail:
            raise RuntimeError("indexeddb unavailable")
        if record_id not in self.records:
            raise gestalt.NotFoundError("record not found")
        return dict(self.records[record_id])

    def put(self, record: dict[str, Any]) -> None:
        if self.fail:
            raise RuntimeError("indexeddb unavailable")
        self.records[str(record["id"])] = dict(record)

    def delete(self, record_id: str) -> None:
        if self.fail:
            raise RuntimeError("indexeddb unavailable")
        if record_id not in self.records:
            raise gestalt.NotFoundError("record not found")
        del self.records[record_id]


class FakeIndexedDB:
    def __init__(self, records: dict[str, dict[str, Any]] | None = None) -> None:
        self.records = records if records is not None else {}
        self.object_store_client = FakeObjectStore(self.records)

    def object_store(self, _name: str) -> FakeObjectStore:
        return self.object_store_client

    def close(self) -> None:
        return None


class FakeAuthorization:
    def __init__(
        self, subjects: list[Any] | None = None, *, fail: bool = False
    ) -> None:
        self.subjects = subjects if subjects is not None else []
        self.fail = fail
        self.requests: list[Any] = []

    def search_subjects(self, request: Any) -> Any:
        self.requests.append(request)
        if self.fail:
            raise RuntimeError("authorization unavailable")
        return SimpleNamespace(subjects=self.subjects)


class FakeAgentManager:
    def __init__(
        self,
        findings: list[dict[str, Any]],
        *,
        output_text: str | None = None,
        output_texts: list[str] | None = None,
        structured_output: dict[str, Any] | None = None,
        require_no_response_schema: bool = False,
        turn_error: Exception | None = None,
    ) -> None:
        self.findings = findings
        self.output_text = output_text
        self.output_texts = output_texts or []
        self.structured_output = structured_output
        self.require_no_response_schema = require_no_response_schema
        self.turn_error = turn_error
        self.sessions: list[Any] = []
        self.turns: list[Any] = []

    def create_session(self, request: Any) -> Any:
        self.sessions.append(request)
        return gestalt.AgentSession(id="agent-session-1")

    def create_turn(self, request: Any) -> Any:
        self.turns.append(request)
        response_schema = getattr(request, "response_schema", None)
        has_response_schema = bool(
            getattr(response_schema, "fields", None)
            if response_schema is not None
            else response_schema
        )
        if self.require_no_response_schema and has_response_schema:
            raise AssertionError("response_schema should not be set")
        if self.turn_error is not None:
            raise self.turn_error
        output_index = len(self.turns) - 1
        if output_index < len(self.output_texts):
            output_text = self.output_texts[output_index]
        elif self.output_text is not None:
            output_text = self.output_text
        else:
            output_text = json.dumps({"findings": self.findings})
        return gestalt.AgentTurn(
            id="agent-turn-1",
            session_id=request.session_id,
            status=gestalt.AGENT_EXECUTION_STATUS_SUCCEEDED,
            output_text=output_text,
            structured_output=self.structured_output,
        )

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


class RequestWithToolRefs:
    def __init__(self, request: gestalt.Request, refs: Sequence[Any]) -> None:
        self._request = request
        self.tool_refs_set = True
        self.tool_refs = list(refs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._request, name)


def request_with_tool_refs(request: gestalt.Request, refs: Sequence[Any]) -> Any:
    return RequestWithToolRefs(request, refs)


def github_agent_request(
    installation_id: int = 99,
    repo: str = "acme/widgets",
    external_identity_type: str = "github_identity",
    external_identity_id: str = "user:222",
) -> Any:
    req = github_request(installation_id=installation_id, repo=repo)
    return SimpleNamespace(
        subject=req.subject,
        agent_subject=SimpleNamespace(
            id="user:user-456",
            kind="user",
            display_name="Grace Hopper",
            auth_source="slack",
        ),
        agent_external_identity=SimpleNamespace(
            type=external_identity_type,
            id=external_identity_id,
        ),
    )


class GitHubExternalIdentityRequest:
    def __init__(
        self,
        *,
        external_identity: Mapping[str, str],
        subject_installation_id: int = 123,
        subject_repo: str = "acme/other",
    ) -> None:
        self.external_identity = dict(external_identity)
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
        external_identity={"type": identity_type, "id": identity_id},
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

    def current_user_identity(self, access_token: str) -> GitHubUserIdentity:
        if access_token != "user-token":
            raise AssertionError(f"unexpected user token {access_token}")
        return GitHubUserIdentity(
            name="Ada Lovelace",
            login="ada",
            user_id="101",
            email="",
        )

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
                "agent": {
                    "provider": "simple",
                    "model": "deep",
                    "modelOptions": {"temperature": 0},
                },
            },
        )
        self.addCleanup(provider_module.configure, "github", {})
        create_check_patch = mock.patch(
            "internals.review.create_check_run",
            side_effect=self._default_create_review_check_run,
        )
        update_check_patch = mock.patch(
            "internals.review.update_check_run",
            side_effect=self._default_update_review_check_run,
        )
        create_check_patch.start()
        update_check_patch.start()
        self.addCleanup(create_check_patch.stop)
        self.addCleanup(update_check_patch.stop)

    def _default_create_review_check_run(
        self, request: Any, *, subject: Any, external_identity: Any = None
    ) -> dict[str, Any]:
        return {
            "id": 501,
            "name": request.name,
            "status": request.status or "in_progress",
            "head_sha": request.head_sha,
            "external_id": request.external_id,
        }

    def _default_update_review_check_run(
        self, request: Any, *, subject: Any, external_identity: Any = None
    ) -> dict[str, Any]:
        return {
            "id": request.check_run_id,
            "name": request.name or "Gestalt Review",
            "status": "completed",
            "conclusion": request.conclusion,
        }

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
        self.assertEqual(
            installation["legacy_subject"],
            "service_account:github_app_installation:99:repo:acme/widgets",
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

    def test_comment_operations_are_declared_in_catalog_and_policy_schema(
        self,
    ) -> None:
        plugin_root = pathlib.Path(__file__).resolve().parents[1]
        catalog = yaml.safe_load((plugin_root / "catalog.yaml").read_text())
        operations = {operation["id"]: operation for operation in catalog["operations"]}

        event = operations[provider_module.GITHUB_EVENT_OPERATION]
        review = operations[provider_module.REVIEW_PULL_REQUEST_OPERATION]
        preference_targets = operations[
            provider_module.ACTION_PREFERENCES_LIST_TARGETS_OPERATION
        ]
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
        user_pr = operations[provider_module.USER_CREATE_PULL_REQUEST_OPERATION]
        issue_comment = operations[provider_module.BOT_CREATE_ISSUE_COMMENT_OPERATION]
        reaction = operations[provider_module.BOT_ADD_REACTION_OPERATION]
        add_labels = operations[provider_module.BOT_ADD_LABELS_OPERATION]
        remove_labels = operations[provider_module.BOT_REMOVE_LABELS_OPERATION]
        request_reviewers = operations[provider_module.BOT_REQUEST_REVIEWERS_OPERATION]
        suite_check_runs = operations[
            provider_module.BOT_LIST_CHECK_SUITE_CHECK_RUNS_OPERATION
        ]
        self.assertIn("workflow targets", event["description"])
        self.assertIn("pull_request workflow signal", review["description"])
        self.assertIn("repositories", preference_targets["description"])
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
        self.assertIn("OAuth token", user_pr["description"])
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
            "autoResolveStaleFindings",
            [parameter["name"] for parameter in review["parameters"]],
        )
        self.assertIn(
            "identity_kind",
            [parameter["name"] for parameter in preference_targets["parameters"]],
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
        user_pr_parameters = [parameter["name"] for parameter in user_pr["parameters"]]
        self.assertIn("files", user_pr_parameters)
        self.assertIn("include_bot_coauthor", user_pr_parameters)
        self.assertNotIn("installation_id", user_pr_parameters)
        self.assertNotIn("author_name", user_pr_parameters)
        self.assertNotIn("author_email", user_pr_parameters)
        self.assertNotIn("committer_name", user_pr_parameters)
        self.assertNotIn("committer_email", user_pr_parameters)

        schema = yaml.safe_load(
            (plugin_root / "schemas" / "config.schema.yaml").read_text()
        )
        enum = schema["properties"]["webhookPolicies"]["items"]["properties"]["action"][
            "properties"
        ]["allowedOperations"]["items"]["enum"]
        self.assertIn(provider_module.BOT_GET_REPOSITORY_OPERATION, enum)
        self.assertIn(provider_module.BOT_SEARCH_CODE_OPERATION, enum)
        self.assertIn(provider_module.BOT_GET_CONTENT_OPERATION, enum)
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
        self.assertIn(provider_module.BOT_CREATE_CHECK_RUN_OPERATION, enum)
        self.assertIn(provider_module.BOT_UPDATE_CHECK_RUN_OPERATION, enum)
        self.assertIn(provider_module.BOT_LIST_CHECK_SUITE_CHECK_RUNS_OPERATION, enum)
        self.assertNotIn(provider_module.USER_CREATE_PULL_REQUEST_OPERATION, enum)
        workflow_schema = schema["properties"]["webhookPolicies"]["items"][
            "properties"
        ]["workflow"]["properties"]
        plugin_target_schema = workflow_schema["target"]["properties"]["plugin"]
        self.assertEqual(plugin_target_schema["required"], ["plugin", "operation"])
        self.assertIn("plugin", plugin_target_schema["properties"])
        self.assertIn("operation", plugin_target_schema["properties"])
        self.assertIn("connection", plugin_target_schema["properties"])
        self.assertIn("instance", plugin_target_schema["properties"])
        self.assertEqual(
            plugin_target_schema["properties"]["credentialMode"]["enum"],
            ["none", "user"],
        )
        self.assertEqual(plugin_target_schema["properties"]["input"]["type"], "object")
        self.assertNotIn("pluginName", plugin_target_schema["properties"])
        self.assertNotIn("credential_mode", plugin_target_schema["properties"])
        policy_schema = schema["properties"]["webhookPolicies"]["items"]["properties"]
        self.assertEqual(policy_schema["displayName"]["type"], "string")
        self.assertEqual(policy_schema["description"]["type"], "string")
        self.assertEqual(
            policy_schema["trigger"]["properties"]["frequency"]["enum"],
            [
                "every_delivery",
                "once_per_pr",
                "once_per_head_sha",
                "once_per_ci_incident",
                "manual_only",
            ],
        )
        self.assertEqual(
            policy_schema["trigger"]["properties"]["manualCommandMatch"]["enum"],
            ["contains", "exact"],
        )
        self.assertEqual(
            policy_schema["trigger"]["properties"]["requireAppMention"]["type"],
            "boolean",
        )
        self.assertEqual(
            policy_schema["dedupe"]["properties"]["scope"]["enum"],
            ["delivery", "pull_request", "pr_head", "ci_incident"],
        )
        self.assertEqual(
            policy_schema["comments"]["properties"]["timelinePolicy"]["enum"],
            ["allow", "never", "actionable_only"],
        )
        self.assertEqual(
            policy_schema["comments"]["properties"]["inlinePolicy"]["enum"],
            ["allow", "never", "findings_only"],
        )
        self.assertEqual(
            policy_schema["comments"]["properties"]["suppressStaleHead"]["type"],
            "boolean",
        )
        self.assertEqual(
            policy_schema["action"]["properties"]["allowCodeReviewComments"]["type"],
            "boolean",
        )
        self.assertEqual(
            policy_schema["action"]["properties"]["allowSelfFix"]["type"],
            "boolean",
        )
        self.assertEqual(
            policy_schema["action"]["properties"]["selfFixMode"]["enum"],
            ["disabled", "suggest", "branch_commit", "pull_request"],
        )
        self.assertEqual(
            policy_schema["action"]["properties"]["preferenceSubject"]["enum"],
            ["pull_request_author", "comment_author", "sender"],
        )
        self.assertEqual(
            schema["properties"]["actionPreferences"]["properties"]["failureMode"][
                "enum"
            ],
            ["config_default"],
        )
        self.assertEqual(
            schema["properties"]["actionPreferences"]["properties"]["indexeddb"][
                "type"
            ],
            "string",
        )

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
        metadata = sdk_value_to_dict(agent.metadata)
        self.assertEqual(metadata["github"]["installation_id"], 99)
        self.assertEqual(metadata["github"]["repository"], "acme/widgets")
        self.assertEqual(metadata["github"]["number"], 7)

        self.assertEqual(request.signal.name, "github.app.webhook")
        data = sdk_value_to_dict(request.signal.payload)
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
                    sdk_value_to_dict(request.signal.payload),
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
                    sdk_value_to_dict(request.target.agent.metadata)["github"][
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

    def test_policy_trigger_and_dedupe_controls_workflow_keys(self) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {
                        "id": "once-pr",
                        "match": {
                            "events": ["pull_request"],
                            "actions": ["opened"],
                        },
                        "trigger": {"frequency": "once_per_pr"},
                        "dedupe": {"scope": "pull_request"},
                    },
                    {
                        "id": "once-head",
                        "match": {
                            "events": ["pull_request"],
                            "actions": ["synchronize"],
                        },
                        "trigger": {"frequency": "once_per_head_sha"},
                        "dedupe": {"scope": "pr_head"},
                    },
                    {
                        "id": "failed-ci",
                        "match": {
                            "events": ["check_run", "check_suite", "workflow_run"],
                            "actions": ["completed"],
                            "conclusions": ["failure"],
                        },
                        "trigger": {"frequency": "once_per_ci_incident"},
                        "dedupe": {"scope": "ci_incident"},
                        "action": {"mode": "comment"},
                    },
                ],
            },
        )

        pull_request_payload = {
            "installation": {"id": 99},
            "repository": {
                "full_name": "acme/widgets",
                "name": "widgets",
                "owner": {"login": "acme"},
            },
            "pull_request": {
                "number": 7,
                "title": "Fix widgets",
                "head": {"ref": "feature", "sha": "abc123"},
                "base": {"ref": "main", "sha": "def456"},
            },
            "sender": {"login": "octocat"},
        }

        once_pr = self._workflow_signal_request(
            {
                **pull_request_payload,
                "action": "opened",
                "headers": {"X-GitHub-Event": "pull_request"},
            }
        )
        self.assertEqual(
            once_pr.workflow_key,
            "github:99:acme/widgets:pr:7:policy:once-pr",
        )
        self.assertEqual(
            once_pr.idempotency_key,
            "github:trigger:once_per_pr:99:acme/widgets:pr:7:policy:once-pr",
        )

        once_head = self._workflow_signal_request(
            {
                **pull_request_payload,
                "action": "synchronize",
                "headers": {"X-GitHub-Event": "pull_request"},
            }
        )
        self.assertEqual(
            once_head.workflow_key,
            "github:99:acme/widgets:pr:7:head:abc123:policy:once-head",
        )
        self.assertEqual(
            once_head.idempotency_key,
            "github:trigger:once_per_head_sha:99:acme/widgets:pr:7:"
            "head:abc123:policy:once-head",
        )

        ci_base = {
            "action": "completed",
            "installation": {"id": 99},
            "repository": {
                "full_name": "acme/widgets",
                "name": "widgets",
                "owner": {"login": "acme"},
            },
            "sender": {"login": "octocat"},
        }
        for event_type, event_object in (
            (
                "check_run",
                {
                    "id": 123,
                    "status": "completed",
                    "conclusion": "failure",
                    "head_sha": "abc123",
                    "pull_requests": [{"number": 7}],
                },
            ),
            (
                "check_suite",
                {
                    "id": 456,
                    "status": "completed",
                    "conclusion": "failure",
                    "head_sha": "abc123",
                    "pull_requests": [{"number": 7}],
                },
            ),
            (
                "workflow_run",
                {
                    "id": 789,
                    "status": "completed",
                    "conclusion": "failure",
                    "head_sha": "abc123",
                    "pull_requests": [{"number": 7}],
                },
            ),
        ):
            with self.subTest(event_type=event_type):
                request = self._workflow_signal_request(
                    {
                        **ci_base,
                        event_type: event_object,
                        "headers": {"X-GitHub-Event": event_type},
                    }
                )
                self.assertEqual(
                    request.workflow_key,
                    "github:99:acme/widgets:ci:pr:7:head:abc123:policy:failed-ci",
                )
                self.assertEqual(
                    request.idempotency_key,
                    "github:trigger:once_per_ci_incident:99:acme/widgets:"
                    "ci:pr:7:head:abc123:policy:failed-ci",
                )
                self.assertEqual(
                    request.signal.idempotency_key, request.idempotency_key
                )
                data = cast(
                    dict[str, Any],
                    sdk_value_to_dict(request.signal.payload),
                )
                self.assertEqual(
                    data["webhook_policy"]["canonical"]["workflow_key"],
                    request.workflow_key,
                )
                self.assertEqual(
                    data["webhook_policy"]["canonical"]["idempotency_key"],
                    request.idempotency_key,
                )

    def test_policy_comment_controls_filter_effective_tools_and_guidance(
        self,
    ) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {
                        "id": "findings-inline-only",
                        "match": {
                            "events": ["pull_request"],
                            "actions": ["opened"],
                        },
                        "comments": {
                            "timelinePolicy": "never",
                            "inlinePolicy": "findings_only",
                        },
                        "action": {
                            "allowedOperations": [
                                provider_module.BOT_GET_PULL_REQUEST_OPERATION,
                                provider_module.BOT_LIST_PULL_REQUEST_FILES_OPERATION,
                                provider_module.BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
                                provider_module.BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
                                provider_module.BOT_CREATE_ISSUE_COMMENT_OPERATION,
                            ]
                        },
                    },
                    {
                        "id": "timeline-only",
                        "match": {
                            "events": ["pull_request"],
                            "actions": ["synchronize"],
                        },
                        "comments": {"inlinePolicy": "never"},
                        "action": {"mode": "comment"},
                    },
                ],
            },
        )

        base = {
            "installation": {"id": 99},
            "repository": {"full_name": "acme/widgets"},
            "pull_request": {
                "number": 7,
                "head": {"ref": "feature", "sha": "abc123"},
                "base": {"ref": "main", "sha": "def456"},
            },
            "headers": {"X-GitHub-Event": "pull_request"},
            "sender": {"login": "octocat"},
        }
        inline_only = self._workflow_signal_request({**base, "action": "opened"})
        inline_operations = [
            tool.operation for tool in inline_only.target.agent.tool_refs
        ]
        self.assertEqual(
            inline_operations,
            [
                provider_module.BOT_GET_PULL_REQUEST_OPERATION,
                provider_module.BOT_LIST_PULL_REQUEST_FILES_OPERATION,
                provider_module.BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
            ],
        )
        system_prompt = inline_only.target.agent.messages[0].text
        self.assertIn("line-anchored findings", system_prompt)
        self.assertNotIn("pull request timeline comments", system_prompt)
        data = cast(
            dict[str, Any],
            sdk_value_to_dict(inline_only.signal.payload),
        )
        self.assertEqual(
            data["webhook_policy"]["tool_refs"],
            inline_operations,
        )
        self.assertEqual(
            data["webhook_policy"]["comments"]["timeline_policy"],
            "never",
        )
        self.assertNotIn(
            provider_module.BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
            data["agent_request"]["user_prompt"],
        )

        timeline_only = self._workflow_signal_request({**base, "action": "synchronize"})
        timeline_operations = [
            tool.operation for tool in timeline_only.target.agent.tool_refs
        ]
        self.assertNotIn(
            provider_module.BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
            timeline_operations,
        )
        self.assertIn(
            provider_module.BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
            timeline_operations,
        )

    def test_policy_action_gates_filter_code_review_and_self_fix_tools(self) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {
                        "id": "no-review-no-fix",
                        "action": {
                            "mode": "pull_request",
                            "allowCodeReviewComments": False,
                            "allowSelfFix": False,
                            "allowedOperations": [
                                provider_module.BOT_GET_PULL_REQUEST_OPERATION,
                                provider_module.BOT_LIST_PULL_REQUEST_FILES_OPERATION,
                                provider_module.BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
                                provider_module.BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
                                provider_module.BOT_COMMIT_FILES_OPERATION,
                                provider_module.BOT_OPEN_PULL_REQUEST_OPERATION,
                                provider_module.BOT_CREATE_PULL_REQUEST_OPERATION,
                            ],
                        },
                    }
                ],
            },
        )

        request = self._workflow_signal_request(
            {
                "action": "opened",
                "installation": {"id": 99},
                "repository": {"full_name": "acme/widgets"},
                "pull_request": {"number": 7},
                "headers": {"X-GitHub-Event": "pull_request"},
                "sender": {"login": "octocat"},
            }
        )

        operations = [tool.operation for tool in request.target.agent.tool_refs]
        self.assertEqual(
            operations,
            [
                provider_module.BOT_GET_PULL_REQUEST_OPERATION,
                provider_module.BOT_LIST_PULL_REQUEST_FILES_OPERATION,
                provider_module.BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
            ],
        )
        system_prompt = request.target.agent.messages[0].text
        self.assertIn("disables code review comments", system_prompt)
        self.assertIn("disables self-fix tools", system_prompt)
        self.assertNotIn("bot.createPullRequestReview", system_prompt)
        self.assertNotIn("bot.commitFiles", system_prompt)
        data = cast(
            dict[str, Any],
            sdk_value_to_dict(request.signal.payload),
        )
        self.assertEqual(data["webhook_policy"]["tool_refs"], operations)
        self.assertEqual(
            data["webhook_policy"]["action"]["allow_code_review_comments"], False
        )
        self.assertEqual(data["webhook_policy"]["action"]["allow_self_fix"], False)
        self.assertIn(
            "allow_code_review_comments: False",
            data["agent_request"]["user_prompt"],
        )
        self.assertIn("allow_self_fix: False", data["agent_request"]["user_prompt"])

    def test_policy_self_fix_mode_must_explicitly_allow_write_tools(self) -> None:
        base_config = {
            "appId": "12345",
            "appPrivateKey": "unused-in-tests",
            "workflow": {"provider": "local"},
            "agent": {"provider": "simple", "model": "deep"},
        }
        payload = {
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
                **base_config,
                "webhookPolicies": [
                    {"id": "default-disabled", "action": {"mode": "pull_request"}}
                ],
            },
        )
        disabled = self._workflow_signal_request(payload)
        disabled_operations = [
            tool.operation for tool in disabled.target.agent.tool_refs
        ]
        self.assertNotIn(
            provider_module.BOT_COMMIT_FILES_OPERATION, disabled_operations
        )
        self.assertNotIn(
            provider_module.BOT_CREATE_PULL_REQUEST_OPERATION, disabled_operations
        )

        provider_module.configure(
            "github",
            {
                **base_config,
                "webhookPolicies": [
                    {
                        "id": "branch-only",
                        "action": {
                            "mode": "pull_request",
                            "selfFixMode": "branch_commit",
                        },
                    }
                ],
            },
        )
        branch_only = self._workflow_signal_request(payload)
        branch_operations = [
            tool.operation for tool in branch_only.target.agent.tool_refs
        ]
        self.assertIn(provider_module.BOT_COMMIT_FILES_OPERATION, branch_operations)
        self.assertNotIn(
            provider_module.BOT_CREATE_PULL_REQUEST_OPERATION, branch_operations
        )

    def test_branch_commit_policy_enriches_pr_comment_context(self) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {
                        "id": "same-pr-fix",
                        "match": {"events": ["issue_comment"]},
                        "action": {
                            "mode": "pull_request",
                            "allowCodeReviewComments": False,
                            "selfFixMode": "branch_commit",
                            "allowedOperations": [
                                "bot.getPullRequest",
                                "bot.listPullRequestFiles",
                                "bot.createPullRequestConversationComment",
                                "bot.commitFiles",
                                "bot.openPullRequest",
                                "bot.createPullRequest",
                            ],
                        },
                    }
                ],
            },
        )

        def fake_get_pull_request(
            request: Any,
            *,
            subject: Any,
            external_identity: Any = None,
            client: Any = None,
        ) -> dict[str, Any]:
            self.assertEqual(request.owner, "acme")
            self.assertEqual(request.repo, "widgets")
            self.assertEqual(request.pull_number, 7)
            self.assertEqual(request.installation_id, 99)
            return self._same_repo_pull_request()

        with mock.patch.object(
            provider_module, "get_pull_request", side_effect=fake_get_pull_request
        ) as get_pull_request:
            request = self._workflow_signal_request(self._pr_issue_comment_payload())

        get_pull_request.assert_called_once()
        operations = [tool.operation for tool in request.target.agent.tool_refs]
        self.assertEqual(
            operations,
            [
                provider_module.BOT_GET_PULL_REQUEST_OPERATION,
                provider_module.BOT_LIST_PULL_REQUEST_FILES_OPERATION,
                provider_module.BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
                provider_module.BOT_COMMIT_FILES_OPERATION,
            ],
        )
        system_prompt = request.target.agent.messages[0].text
        self.assertIn("Self-fix is same-PR mode", system_prompt)
        self.assertIn("pull_request.head_ref value as branch", system_prompt)
        self.assertNotIn("bot.createPullRequest ", system_prompt)

        data = cast(dict[str, Any], sdk_value_to_dict(request.signal.payload))
        pull_request = data["agent_request"]["pull_request"]
        self.assertEqual(pull_request["head_ref"], "feature")
        self.assertEqual(pull_request["base_ref"], "main")
        self.assertEqual(pull_request["head_repo"]["full_name"], "acme/widgets")
        self.assertEqual(pull_request["base_repo"]["full_name"], "acme/widgets")
        self.assertEqual(pull_request["head_repo_is_base_repo"], True)
        self.assertEqual(pull_request["maintainer_can_modify"], True)
        prompt = data["agent_request"]["user_prompt"]
        self.assertIn("head_repo.full_name: acme/widgets", prompt)
        self.assertIn("head_repo_is_base_repo: True", prompt)
        self.assertEqual(data["webhook_policy"]["tool_refs"], operations)

    def test_branch_commit_context_fetch_waits_for_effective_preferences(self) -> None:
        config = self._action_preferences_config(
            match={"events": ["issue_comment"]},
            action={
                "mode": "pull_request",
                "selfFixMode": "pull_request",
                "preferenceSubject": "comment_author",
            },
        )
        provider_module.configure("github", config)
        identity = identity_module.GitHubPreferenceIdentity(
            preference_subject="comment_author",
            repository="acme/widgets",
            external_identity_type=provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE,
            external_subject_id="user:303",
        )
        record_id = preferences_module.preference_record_id(
            repository="acme/widgets",
            policy_id="review-policy",
            identity=identity,
            identity_kind="external_subject_id",
        )
        fake_db = FakeIndexedDB(
            {
                record_id: {
                    "id": record_id,
                    "schema_version": 2,
                    "repository": "acme/widgets",
                    "policy_id": "review-policy",
                    "identity_kind": "external_subject_id",
                    "external_identity_type": provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE,
                    "external_subject_id": "user:303",
                    "self_fix_mode": "branch_commit",
                }
            }
        )

        with (
            mock.patch.object(gestalt, "IndexedDB", return_value=fake_db),
            mock.patch.object(
                gestalt.Request,
                "authorization",
                return_value=FakeAuthorization(),
            ),
            mock.patch.object(
                provider_module,
                "get_pull_request",
                return_value=self._same_repo_pull_request(),
            ) as get_pull_request,
        ):
            request = self._workflow_signal_request(self._pr_issue_comment_payload())

        get_pull_request.assert_called_once()
        operations = [tool.operation for tool in request.target.agent.tool_refs]
        self.assertIn(provider_module.BOT_COMMIT_FILES_OPERATION, operations)
        self.assertNotIn(provider_module.BOT_OPEN_PULL_REQUEST_OPERATION, operations)
        self.assertNotIn(provider_module.BOT_CREATE_PULL_REQUEST_OPERATION, operations)
        data = cast(dict[str, Any], sdk_value_to_dict(request.signal.payload))
        self.assertEqual(
            data["webhook_policy"]["action_preferences"]["effective"]["self_fix_mode"],
            "branch_commit",
        )
        self.assertEqual(
            data["agent_request"]["pull_request"]["head_repo"]["full_name"],
            "acme/widgets",
        )

    def test_branch_commit_context_fetch_after_preferences_maps_github_errors(
        self,
    ) -> None:
        config = self._action_preferences_config(
            match={"events": ["issue_comment"]},
            action={
                "mode": "pull_request",
                "selfFixMode": "pull_request",
                "preferenceSubject": "comment_author",
            },
        )
        provider_module.configure("github", config)
        identity = identity_module.GitHubPreferenceIdentity(
            preference_subject="comment_author",
            repository="acme/widgets",
            external_identity_type=provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE,
            external_subject_id="user:303",
        )
        record_id = preferences_module.preference_record_id(
            repository="acme/widgets",
            policy_id="review-policy",
            identity=identity,
            identity_kind="external_subject_id",
        )
        fake_db = FakeIndexedDB(
            {
                record_id: {
                    "id": record_id,
                    "schema_version": 2,
                    "repository": "acme/widgets",
                    "policy_id": "review-policy",
                    "identity_kind": "external_subject_id",
                    "external_identity_type": provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE,
                    "external_subject_id": "user:303",
                    "self_fix_mode": "branch_commit",
                }
            }
        )

        with (
            mock.patch.object(gestalt, "IndexedDB", return_value=fake_db),
            mock.patch.object(
                gestalt.Request,
                "authorization",
                return_value=FakeAuthorization(),
            ),
            mock.patch.object(
                provider_module,
                "get_pull_request",
                side_effect=GitHubAPIError(502, "GitHub unavailable"),
            ),
        ):
            result = provider_module.github_events_handle(
                self._pr_issue_comment_payload(), gestalt.Request()
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, Any]], result)
        self.assertEqual(response.status, 502)
        self.assertIn("GitHub unavailable", response.body["error"])

    def test_branch_commit_context_fetch_is_narrow(self) -> None:
        base_config = {
            "appId": "12345",
            "appPrivateKey": "unused-in-tests",
            "workflow": {"provider": "local"},
            "agent": {"provider": "simple", "model": "deep"},
        }

        provider_module.configure(
            "github",
            {
                **base_config,
                "webhookPolicies": [
                    {
                        "id": "follow-up-pr",
                        "match": {"events": ["issue_comment"]},
                        "action": {
                            "mode": "pull_request",
                            "selfFixMode": "pull_request",
                        },
                    }
                ],
            },
        )
        with mock.patch.object(provider_module, "get_pull_request") as get_pull_request:
            self._workflow_signal_request(self._pr_issue_comment_payload())
        get_pull_request.assert_not_called()

        provider_module.configure(
            "github",
            {
                **base_config,
                "webhookPolicies": [
                    {
                        "id": "plain-issue",
                        "match": {"events": ["issue_comment"]},
                        "action": {
                            "mode": "pull_request",
                            "selfFixMode": "branch_commit",
                        },
                    }
                ],
            },
        )
        with mock.patch.object(provider_module, "get_pull_request") as get_pull_request:
            self._workflow_signal_request(self._plain_issue_comment_payload())
        get_pull_request.assert_not_called()

        provider_module.configure(
            "github",
            {
                **base_config,
                "webhookPolicies": [
                    {
                        "id": "direct-pr",
                        "match": {"events": ["pull_request"]},
                        "action": {
                            "mode": "pull_request",
                            "selfFixMode": "branch_commit",
                        },
                    }
                ],
            },
        )
        with mock.patch.object(provider_module, "get_pull_request") as get_pull_request:
            request = self._workflow_signal_request(self._full_pull_request_payload())
        get_pull_request.assert_not_called()
        data = cast(dict[str, Any], sdk_value_to_dict(request.signal.payload))
        self.assertEqual(
            data["agent_request"]["pull_request"]["head_repo"]["full_name"],
            "acme/widgets",
        )

    def test_action_preferences_absent_preserves_static_dispatch_without_lookup(
        self,
    ) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {
                        "id": "static-comment",
                        "match": {"events": ["pull_request"]},
                        "action": {"mode": "comment"},
                    }
                ],
            },
        )

        with (
            mock.patch.object(
                gestalt, "IndexedDB", side_effect=AssertionError("no indexeddb")
            ),
            mock.patch.object(
                gestalt.Request,
                "authorization",
                side_effect=AssertionError("no authorization"),
            ),
        ):
            request = self._workflow_signal_request(
                {
                    "action": "opened",
                    "installation": {"id": 99},
                    "repository": {"full_name": "acme/widgets"},
                    "pull_request": {"number": 7, "user": {"id": 101}},
                    "headers": {"X-GitHub-Event": "pull_request"},
                    "sender": {"id": 202, "login": "octocat"},
                }
            )

        data = cast(dict[str, Any], sdk_value_to_dict(request.signal.payload))
        self.assertNotIn("action_preferences", data["webhook_policy"])
        self.assertIn(
            provider_module.BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
            [tool.operation for tool in request.target.agent.tool_refs],
        )

    def test_action_preferences_external_record_disables_inline_review_tool(
        self,
    ) -> None:
        provider_module.configure(
            "github",
            self._action_preferences_config(
                action={"mode": "comment", "preferenceSubject": "pull_request_author"}
            ),
        )
        identity = identity_module.GitHubPreferenceIdentity(
            preference_subject="pull_request_author",
            repository="acme/widgets",
            external_identity_type=provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE,
            external_subject_id="user:101",
            subject_id="user:ada",
        )
        record_id = preferences_module.preference_record_id(
            repository="acme/widgets",
            policy_id="review-policy",
            identity=identity,
            identity_kind="external_subject_id",
        )
        fake_db = FakeIndexedDB(
            {
                record_id: {
                    "id": record_id,
                    "schema_version": 1,
                    "repository": "acme/widgets",
                    "policy_id": "review-policy",
                    "identity_kind": "external_subject_id",
                    "external_identity_type": provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE,
                    "external_subject_id": "user:101",
                    "allow_code_review_comments": False,
                    "allow_self_fix": None,
                }
            }
        )
        authorization = FakeAuthorization(
            [SimpleNamespace(type="subject", id="user:ada")]
        )

        with (
            mock.patch.object(gestalt, "IndexedDB", return_value=fake_db),
            mock.patch.object(
                gestalt.Request, "authorization", return_value=authorization
            ),
        ):
            request = self._workflow_signal_request(self._preference_pr_payload())

        operations = [tool.operation for tool in request.target.agent.tool_refs]
        self.assertNotIn(
            provider_module.BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION, operations
        )
        self.assertIn(
            provider_module.BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
            operations,
        )
        data = cast(dict[str, Any], sdk_value_to_dict(request.signal.payload))
        preferences = data["webhook_policy"]["action_preferences"]
        self.assertEqual(preferences["source"], "external_subject_id")
        self.assertEqual(preferences["record_id"], record_id)
        self.assertEqual(preferences["effective"]["allow_code_review_comments"], False)
        self.assertIn(
            "action_preferences_source: external_subject_id",
            data["agent_request"]["user_prompt"],
        )

    def test_action_preferences_subject_record_fallback_disables_self_fix(
        self,
    ) -> None:
        provider_module.configure(
            "github",
            self._action_preferences_config(
                action={"mode": "pull_request", "selfFixMode": "pull_request"}
            ),
        )
        identity = identity_module.GitHubPreferenceIdentity(
            preference_subject="pull_request_author",
            repository="acme/widgets",
            external_identity_type=provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE,
            external_subject_id="user:101",
            subject_id="user:ada",
        )
        record_id = preferences_module.preference_record_id(
            repository="acme/widgets",
            policy_id="review-policy",
            identity=identity,
            identity_kind="subject_id",
        )
        fake_db = FakeIndexedDB(
            {
                record_id: {
                    "id": record_id,
                    "schema_version": 1,
                    "repository": "acme/widgets",
                    "policy_id": "review-policy",
                    "identity_kind": "subject_id",
                    "subject_id": "user:ada",
                    "allow_code_review_comments": None,
                    "allow_self_fix": False,
                    "self_fix_mode": "pull_request",
                }
            }
        )

        with (
            mock.patch.object(gestalt, "IndexedDB", return_value=fake_db),
            mock.patch.object(
                gestalt.Request,
                "authorization",
                return_value=FakeAuthorization(
                    [SimpleNamespace(type="subject", id="user:ada")]
                ),
            ),
        ):
            request = self._workflow_signal_request(self._preference_pr_payload())

        operations = [tool.operation for tool in request.target.agent.tool_refs]
        self.assertNotIn(provider_module.BOT_COMMIT_FILES_OPERATION, operations)
        self.assertNotIn(provider_module.BOT_OPEN_PULL_REQUEST_OPERATION, operations)
        self.assertNotIn(provider_module.BOT_CREATE_PULL_REQUEST_OPERATION, operations)
        data = cast(dict[str, Any], sdk_value_to_dict(request.signal.payload))
        self.assertEqual(
            data["webhook_policy"]["action_preferences"]["source"], "subject_id"
        )
        self.assertEqual(
            data["webhook_policy"]["action_preferences"]["effective"]["self_fix_mode"],
            "disabled",
        )

    def test_action_preferences_true_cannot_exceed_static_policy_ceiling(
        self,
    ) -> None:
        provider_module.configure(
            "github",
            self._action_preferences_config(
                action={
                    "mode": "pull_request",
                    "allowCodeReviewComments": False,
                    "allowSelfFix": False,
                    "selfFixMode": "pull_request",
                }
            ),
        )
        identity = identity_module.GitHubPreferenceIdentity(
            preference_subject="pull_request_author",
            repository="acme/widgets",
            external_identity_type=provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE,
            external_subject_id="user:101",
        )
        record_id = preferences_module.preference_record_id(
            repository="acme/widgets",
            policy_id="review-policy",
            identity=identity,
            identity_kind="external_subject_id",
        )
        fake_db = FakeIndexedDB(
            {
                record_id: {
                    "id": record_id,
                    "schema_version": 1,
                    "repository": "acme/widgets",
                    "policy_id": "review-policy",
                    "identity_kind": "external_subject_id",
                    "external_identity_type": provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE,
                    "external_subject_id": "user:101",
                    "allow_code_review_comments": True,
                    "allow_self_fix": True,
                }
            }
        )

        with (
            mock.patch.object(gestalt, "IndexedDB", return_value=fake_db),
            mock.patch.object(
                gestalt.Request, "authorization", return_value=FakeAuthorization()
            ),
        ):
            request = self._workflow_signal_request(self._preference_pr_payload())

        operations = [tool.operation for tool in request.target.agent.tool_refs]
        self.assertNotIn(
            provider_module.BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION, operations
        )
        self.assertNotIn(provider_module.BOT_COMMIT_FILES_OPERATION, operations)

    def test_action_preferences_store_or_record_failure_falls_back_to_config(
        self,
    ) -> None:
        for record in (
            None,
            {
                "id": "bad",
                "schema_version": 999,
                "repository": "acme/widgets",
                "policy_id": "review-policy",
                "identity_kind": "external_subject_id",
                "external_identity_type": provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE,
                "external_subject_id": "user:101",
            },
        ):
            with self.subTest(record=record):
                provider_module.configure(
                    "github",
                    self._action_preferences_config(action={"mode": "comment"}),
                )
                records: dict[str, dict[str, Any]] = {}
                if record is not None:
                    identity = identity_module.GitHubPreferenceIdentity(
                        preference_subject="pull_request_author",
                        repository="acme/widgets",
                        external_identity_type=provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE,
                        external_subject_id="user:101",
                    )
                    record_id = preferences_module.preference_record_id(
                        repository="acme/widgets",
                        policy_id="review-policy",
                        identity=identity,
                        identity_kind="external_subject_id",
                    )
                    records[record_id] = {**record, "id": record_id}
                fake_db = FakeIndexedDB(records)
                if record is None:
                    fake_db.object_store_client.fail = True
                with (
                    mock.patch.object(gestalt, "IndexedDB", return_value=fake_db),
                    mock.patch.object(
                        gestalt.Request,
                        "authorization",
                        return_value=FakeAuthorization(),
                    ),
                ):
                    request = self._workflow_signal_request(
                        self._preference_pr_payload()
                    )
                self.assertIn(
                    provider_module.BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
                    [tool.operation for tool in request.target.agent.tool_refs],
                )

    def test_action_preferences_dynamic_false_ignores_builtin_review_target(
        self,
    ) -> None:
        provider_module.configure(
            "github",
            {
                **self._action_preferences_config(action={"mode": "comment"}),
                "webhookPolicies": [
                    {
                        "id": "review-policy",
                        "match": {"events": ["pull_request"]},
                        "workflow": {
                            "target": {
                                "plugin": {
                                    "plugin": "github",
                                    "operation": "reviewPullRequest",
                                }
                            }
                        },
                        "action": {"mode": "comment"},
                    }
                ],
            },
        )
        identity = identity_module.GitHubPreferenceIdentity(
            preference_subject="pull_request_author",
            repository="acme/widgets",
            external_identity_type=provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE,
            external_subject_id="user:101",
        )
        record_id = preferences_module.preference_record_id(
            repository="acme/widgets",
            policy_id="review-policy",
            identity=identity,
            identity_kind="external_subject_id",
        )
        fake_db = FakeIndexedDB(
            {
                record_id: {
                    "id": record_id,
                    "schema_version": 1,
                    "repository": "acme/widgets",
                    "policy_id": "review-policy",
                    "identity_kind": "external_subject_id",
                    "external_identity_type": provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE,
                    "external_subject_id": "user:101",
                    "allow_code_review_comments": False,
                    "allow_self_fix": None,
                }
            }
        )
        workflow_manager = FakeWorkflowManager()
        with (
            mock.patch.object(gestalt, "IndexedDB", return_value=fake_db),
            mock.patch.object(
                gestalt.Request, "authorization", return_value=FakeAuthorization()
            ),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            result = provider_module.github_events_handle(
                self._preference_pr_payload(), gestalt.Request()
            )
        self.assertEqual(
            result,
            {
                "ok": True,
                "ignored": "policy_preference_disabled:code_review_comments",
            },
        )
        self.assertEqual(workflow_manager.requests, [])

    def test_action_preferences_comment_author_and_sender_modes(self) -> None:
        cases = [
            (
                "comment_author",
                "issue_comment",
                {
                    "action": "created",
                    "installation": {"id": 99},
                    "repository": {"full_name": "acme/widgets"},
                    "issue": {"number": 7, "pull_request": {}},
                    "comment": {"id": 123, "user": {"id": 303, "login": "reviewer"}},
                    "headers": {"X-GitHub-Event": "issue_comment"},
                    "sender": {"id": 202, "login": "octocat"},
                },
                "user:303",
            ),
            (
                "sender",
                "issues",
                {
                    "action": "opened",
                    "installation": {"id": 99},
                    "repository": {"full_name": "acme/widgets"},
                    "issue": {"number": 8},
                    "headers": {"X-GitHub-Event": "issues"},
                    "sender": {"id": 202, "login": "octocat"},
                },
                "user:202",
            ),
        ]
        for preference_subject, event_name, payload, external_subject_id in cases:
            with self.subTest(preference_subject=preference_subject):
                provider_module.configure(
                    "github",
                    self._action_preferences_config(
                        match={"events": [event_name]},
                        action={
                            "mode": "comment",
                            "preferenceSubject": preference_subject,
                        },
                    ),
                )
                identity = identity_module.GitHubPreferenceIdentity(
                    preference_subject=preference_subject,
                    repository="acme/widgets",
                    external_identity_type=provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE,
                    external_subject_id=external_subject_id,
                )
                record_id = preferences_module.preference_record_id(
                    repository="acme/widgets",
                    policy_id="review-policy",
                    identity=identity,
                    identity_kind="external_subject_id",
                )
                fake_db = FakeIndexedDB(
                    {
                        record_id: {
                            "id": record_id,
                            "schema_version": 1,
                            "repository": "acme/widgets",
                            "policy_id": "review-policy",
                            "identity_kind": "external_subject_id",
                            "external_identity_type": provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE,
                            "external_subject_id": external_subject_id,
                            "allow_code_review_comments": False,
                            "allow_self_fix": None,
                        }
                    }
                )
                with (
                    mock.patch.object(gestalt, "IndexedDB", return_value=fake_db),
                    mock.patch.object(
                        gestalt.Request,
                        "authorization",
                        return_value=FakeAuthorization(),
                    ),
                ):
                    request = self._workflow_signal_request(payload)
                data = cast(
                    dict[str, Any], sdk_value_to_dict(request.signal.payload)
                )
                self.assertEqual(
                    data["webhook_policy"]["action_preferences"]["identity"][
                        "external_subject_id"
                    ],
                    external_subject_id,
                )

    def test_action_preference_operations_manage_callers_own_identity(self) -> None:
        provider_module.configure(
            "github",
            self._action_preferences_config(
                action={"mode": "pull_request", "selfFixMode": "pull_request"}
            ),
        )
        fake_db = FakeIndexedDB()
        req = gestalt.Request(
            agent_external_identity=gestalt.ExternalIdentity(
                type=provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE, id="user:101"
            ),
            agent_subject=gestalt.Subject(id="user:ada", kind="human"),
        )

        with mock.patch.object(gestalt, "IndexedDB", return_value=fake_db) as indexeddb:
            set_result = provider_module.github_action_preferences_set(
                provider_module.SetActionPreferenceInput(
                    repository="acme/widgets",
                    policy_id="review-policy",
                    allow_code_review_comments=False,
                    allow_self_fix=None,
                    self_fix_mode="branch_commit",
                ),
                req,
            )
            get_result = provider_module.github_action_preferences_get(
                provider_module.ActionPreferenceInput(
                    repository="acme/widgets", policy_id="review-policy"
                ),
                req,
            )
            delete_result = provider_module.github_action_preferences_delete(
                provider_module.ActionPreferenceInput(
                    repository="acme/widgets", policy_id="review-policy"
                ),
                req,
            )
            get_after_delete = provider_module.github_action_preferences_get(
                provider_module.ActionPreferenceInput(
                    repository="acme/widgets", policy_id="review-policy"
                ),
                req,
            )
            invalid_result = provider_module.github_action_preferences_set(
                provider_module.SetActionPreferenceInput(
                    repository="acme/widgets",
                    policy_id="review-policy",
                    self_fix_mode="invalid",
                ),
                req,
            )

        indexeddb.assert_called_once_with("github_prefs")
        preference = set_result["data"]["preference"]
        self.assertEqual(preference["identity_kind"], "external_subject_id")
        self.assertEqual(preference["external_subject_id"], "user:101")
        self.assertEqual(preference["allow_code_review_comments"], False)
        self.assertEqual(preference["self_fix_mode"], "branch_commit")
        self.assertEqual(get_result["data"]["preference"]["id"], preference["id"])
        self.assertEqual(delete_result["data"]["deleted"], True)
        self.assertEqual(get_after_delete["data"]["found"], False)
        self.assertEqual(invalid_result.status, HTTPStatus.BAD_REQUEST)
        self.assertIn("self_fix_mode must be", invalid_result.body["error"])

    def test_action_preference_targets_list_oauth_accessible_effective_controls(
        self,
    ) -> None:
        outer_self = self

        class FakePreferenceTargetsClient:
            def __init__(self) -> None:
                self.repository_access_checks: list[tuple[str, str | None]] = []

            def current_user_identity(self, access_token: str) -> GitHubUserIdentity:
                outer_self.assertEqual(access_token, "user-token")
                return GitHubUserIdentity(
                    name="Ada Lovelace",
                    login="ada",
                    user_id="101",
                    email="",
                )

            def app_installations(
                self, *, per_page: int = 100, page: int = 1
            ) -> list[dict[str, Any]]:
                outer_self.assertEqual(per_page, 100)
                return [{"id": 99}, {"id": 100}] if page == 1 else []

            def installation_token(
                self,
                installation_id: int,
                *,
                repositories: Sequence[str] | None = None,
                permissions: Mapping[str, str] | None = None,
            ) -> str:
                if installation_id == 99:
                    return "installation-token-99"
                if installation_id == 100:
                    raise GitHubAPIError(HTTPStatus.FORBIDDEN, "installation suspended")
                raise AssertionError(f"unexpected installation {installation_id}")

            def installation_repositories(
                self, access_token: str, *, per_page: int = 100, page: int = 1
            ) -> list[dict[str, Any]]:
                outer_self.assertEqual(access_token, "installation-token-99")
                outer_self.assertEqual(per_page, 100)
                if page == 1:
                    return [
                        {
                            "full_name": "acme/widgets",
                            "html_url": "https://github.com/acme/widgets",
                        },
                        {
                            "full_name": "acme/private",
                            "html_url": "https://github.com/acme/private",
                        },
                    ]
                return []

            def github_json(
                self,
                method: str,
                path: str,
                token: str | None,
                payload: Mapping[str, Any] | None = None,
            ) -> dict[str, Any]:
                outer_self.assertEqual(method, "GET")
                self.repository_access_checks.append((path, token))
                if path == "/repos/acme/widgets":
                    return {
                        "full_name": "acme/widgets",
                        "html_url": "https://github.com/acme/widgets",
                    }
                if path == "/repos/acme/private":
                    raise GitHubAPIError(HTTPStatus.NOT_FOUND, "Not Found")
                raise AssertionError(f"unexpected GitHub request {path}")

        config = self._action_preferences_config()
        config["webhookPolicies"] = [
            {
                "id": "review-policy",
                "displayName": "PR review",
                "description": "Controls inline review comments.",
                "match": {"repositories": ["acme/widgets"]},
                "action": {"mode": "comment"},
            },
            {
                "id": "self-fix-policy",
                "match": {},
                "action": {
                    "mode": "pull_request",
                    "allowCodeReviewComments": False,
                    "selfFixMode": "pull_request",
                },
            },
            {
                "id": "subject-policy",
                "match": {"repositories": ["acme/widgets"]},
                "action": {"mode": "comment"},
            },
            {
                "id": "suggest-policy",
                "match": {"repositories": ["acme/widgets"]},
                "action": {"mode": "comment", "selfFixMode": "suggest"},
            },
            {
                "id": "legacy-disabled-policy",
                "match": {"repositories": ["acme/widgets"]},
                "action": {"mode": "pull_request", "selfFixMode": "pull_request"},
            },
            {
                "id": "observe-policy",
                "match": {},
                "action": {"mode": "observe"},
            },
            {
                "id": "other-repo-policy",
                "match": {"repositories": ["acme/other"]},
                "action": {"mode": "comment"},
            },
        ]
        provider_module.configure("github", config)
        fake_db = FakeIndexedDB()
        fake_client = FakePreferenceTargetsClient()
        req = SimpleNamespace(
            token="user-token",
            subject=gestalt.Subject(id="user:ada", kind="human"),
        )

        with (
            mock.patch.object(gestalt, "IndexedDB", return_value=fake_db),
            mock.patch.object(provider_module, "DEFAULT_GITHUB_CLIENT", fake_client),
            mock.patch.object(identity_module, "DEFAULT_GITHUB_CLIENT", fake_client),
        ):
            provider_module.github_action_preferences_set(
                provider_module.SetActionPreferenceInput(
                    repository="acme/widgets",
                    policy_id="review-policy",
                    allow_code_review_comments=False,
                ),
                req,
            )
            provider_module.github_action_preferences_set(
                provider_module.SetActionPreferenceInput(
                    repository="acme/widgets",
                    policy_id="self-fix-policy",
                    allow_self_fix=True,
                ),
                req,
            )
            provider_module.github_action_preferences_set(
                provider_module.SetActionPreferenceInput(
                    repository="acme/widgets",
                    policy_id="subject-policy",
                    identity_kind="subject_id",
                    allow_code_review_comments=False,
                ),
                req,
            )
            provider_module.github_action_preferences_set(
                provider_module.SetActionPreferenceInput(
                    repository="acme/widgets",
                    policy_id="legacy-disabled-policy",
                    allow_self_fix=False,
                ),
                req,
            )
            for record in fake_db.records.values():
                if record.get("policy_id") == "self-fix-policy":
                    record["allow_self_fix"] = "invalid"
            result = provider_module.github_action_preferences_list_targets(
                provider_module.ActionPreferenceTargetsInput(),
                req,
            )

        self.assertEqual(
            fake_client.repository_access_checks,
            [
                ("/repos/acme/widgets", "user-token"),
                ("/repos/acme/private", "user-token"),
            ],
        )
        data = result["data"]
        self.assertEqual(data["identity"]["identity_kind"], "external_subject_id")
        self.assertEqual(data["identity"]["external_subject_id"], "user:101")
        self.assertEqual(len(data["repositories"]), 1)
        repository = data["repositories"][0]
        self.assertEqual(repository["repository"], "acme/widgets")
        self.assertEqual(repository["installation_id"], 99)
        controls = {
            (control["policy_id"], control["field"]): control
            for control in repository["controls"]
        }
        self.assertEqual(
            controls[("review-policy", "allow_code_review_comments")]["label"],
            "PR review",
        )
        self.assertEqual(
            controls[("review-policy", "allow_code_review_comments")]["stored"],
            False,
        )
        self.assertEqual(
            controls[("review-policy", "allow_code_review_comments")]["effective"],
            False,
        )
        self.assertEqual(
            controls[("review-policy", "allow_code_review_comments")]["identity_kind"],
            "external_subject_id",
        )
        self.assertEqual(controls[("self-fix-policy", "self_fix_mode")]["stored"], None)
        self.assertEqual(
            controls[("self-fix-policy", "self_fix_mode")]["effective"], "pull_request"
        )
        self.assertEqual(
            controls[("suggest-policy", "self_fix_mode")]["effective"], "suggest"
        )
        self.assertEqual(
            [
                option["value"]
                for option in controls[("suggest-policy", "self_fix_mode")]["options"]
            ],
            ["disabled", "suggest"],
        )
        self.assertEqual(
            controls[("legacy-disabled-policy", "self_fix_mode")]["stored"],
            "disabled",
        )
        self.assertEqual(
            controls[("legacy-disabled-policy", "self_fix_mode")]["effective"],
            "disabled",
        )
        self.assertEqual(
            controls[("subject-policy", "allow_code_review_comments")]["stored"],
            False,
        )
        self.assertEqual(
            controls[("subject-policy", "allow_code_review_comments")]["identity_kind"],
            "subject_id",
        )
        self.assertNotIn(("observe-policy", "allow_code_review_comments"), controls)
        self.assertNotIn(("other-repo-policy", "allow_code_review_comments"), controls)

    def test_action_preference_targets_do_not_return_session_unauthorized_for_github_oauth(
        self,
    ) -> None:
        class RejectedOAuthClient:
            def current_user_identity(self, _access_token: str) -> GitHubUserIdentity:
                raise GitHubAPIError(HTTPStatus.UNAUTHORIZED, "Bad credentials")

        provider_module.configure(
            "github",
            self._action_preferences_config(action={"mode": "comment"}),
        )

        with mock.patch.object(
            identity_module, "DEFAULT_GITHUB_CLIENT", RejectedOAuthClient()
        ):
            result = provider_module.github_action_preferences_list_targets(
                provider_module.ActionPreferenceTargetsInput(),
                SimpleNamespace(
                    token="revoked-token",
                    subject=gestalt.Subject(id="user:ada", kind="human"),
                ),
            )

        self.assertEqual(result.status, HTTPStatus.PRECONDITION_FAILED)
        self.assertIn("reconnect GitHub", result.body["error"])

    def test_action_preference_operations_reject_missing_store_or_identity(
        self,
    ) -> None:
        result = provider_module.github_action_preferences_get(
            provider_module.ActionPreferenceInput(
                repository="acme/widgets", policy_id="review-policy"
            ),
            gestalt.Request(),
        )
        self.assertEqual(result.status, HTTPStatus.PRECONDITION_FAILED)

        provider_module.configure(
            "github",
            self._action_preferences_config(action={"mode": "comment"}),
        )
        fake_db = FakeIndexedDB()
        with mock.patch.object(gestalt, "IndexedDB", return_value=fake_db):
            service_account_result = provider_module.github_action_preferences_set(
                provider_module.SetActionPreferenceInput(
                    repository="acme/widgets",
                    policy_id="review-policy",
                    allow_code_review_comments=False,
                ),
                gestalt.Request(
                    subject=gestalt.Subject(
                        id="service_account:github_app_installation:99",
                        kind="service_account",
                    )
                ),
            )
            unavailable_external_result = provider_module.github_action_preferences_get(
                provider_module.ActionPreferenceInput(
                    repository="acme/widgets",
                    policy_id="review-policy",
                    identity_kind="external_subject_id",
                ),
                gestalt.Request(
                    agent_subject=gestalt.Subject(id="user:ada", kind="human")
                ),
            )

        self.assertEqual(service_account_result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(unavailable_external_result.status, HTTPStatus.BAD_REQUEST)

    def test_policy_manual_commands_and_drafts_fall_through(self) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {
                        "id": "manual-review",
                        "match": {
                            "events": ["issue_comment"],
                            "actions": ["created"],
                        },
                        "trigger": {
                            "frequency": "manual_only",
                            "manualCommands": ["@gestalt review"],
                        },
                    },
                    {
                        "id": "issue-comment-fallback",
                        "match": {
                            "events": ["issue_comment"],
                            "actions": ["created"],
                        },
                    },
                    {
                        "id": "non-draft-pr",
                        "match": {
                            "events": ["pull_request"],
                            "actions": ["opened"],
                        },
                        "trigger": {"includeDrafts": False},
                    },
                    {
                        "id": "draft-fallback",
                        "match": {
                            "events": ["pull_request"],
                            "actions": ["opened"],
                        },
                    },
                ],
            },
        )

        issue_comment_base = {
            "action": "created",
            "installation": {"id": 99},
            "repository": {"full_name": "acme/widgets"},
            "issue": {
                "number": 7,
                "title": "Fix widgets",
                "pull_request": {"html_url": "https://github.com/acme/widgets/pull/7"},
            },
            "headers": {"X-GitHub-Event": "issue_comment"},
            "sender": {"login": "octocat"},
        }
        fallback = self._workflow_signal_request(
            {
                **issue_comment_base,
                "comment": {"id": 10, "body": "looks good"},
            }
        )
        fallback_data = cast(
            dict[str, Any],
            sdk_value_to_dict(fallback.signal.payload),
        )
        self.assertEqual(
            fallback_data["webhook_policy"]["id"],
            "issue-comment-fallback",
        )

        manual = self._workflow_signal_request(
            {
                **issue_comment_base,
                "comment": {"id": 11, "body": "please @gestalt review"},
            }
        )
        manual_data = cast(
            dict[str, Any],
            sdk_value_to_dict(manual.signal.payload),
        )
        self.assertEqual(manual_data["webhook_policy"]["id"], "manual-review")

        pull_request_base = {
            "action": "opened",
            "installation": {"id": 99},
            "repository": {"full_name": "acme/widgets"},
            "pull_request": {
                "number": 7,
                "head": {"ref": "feature", "sha": "abc123"},
                "base": {"ref": "main", "sha": "def456"},
            },
            "headers": {"X-GitHub-Event": "pull_request"},
            "sender": {"login": "octocat"},
        }
        draft = self._workflow_signal_request(
            {
                **pull_request_base,
                "pull_request": {
                    **pull_request_base["pull_request"],
                    "draft": True,
                },
            }
        )
        draft_data = cast(
            dict[str, Any],
            sdk_value_to_dict(draft.signal.payload),
        )
        self.assertEqual(draft_data["webhook_policy"]["id"], "draft-fallback")

        ready = self._workflow_signal_request(
            {
                **pull_request_base,
                "pull_request": {
                    **pull_request_base["pull_request"],
                    "draft": False,
                },
            }
        )
        ready_data = cast(
            dict[str, Any],
            sdk_value_to_dict(ready.signal.payload),
        )
        self.assertEqual(ready_data["webhook_policy"]["id"], "non-draft-pr")

    def test_policy_app_mention_manual_trigger_uses_github_app_identity(self) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {
                        "id": "app-mention-review",
                        "match": {
                            "events": ["issue_comment"],
                            "actions": ["created"],
                        },
                        "trigger": {
                            "frequency": "manual_only",
                            "requireAppMention": True,
                        },
                    },
                    {
                        "id": "fallback",
                        "match": {"events": ["issue_comment"], "actions": ["created"]},
                    },
                ],
            },
        )
        base = {
            "action": "created",
            "installation": {"id": 99},
            "repository": {"full_name": "acme/widgets"},
            "issue": {
                "number": 7,
                "pull_request": {"html_url": "https://github.com/acme/widgets/pull/7"},
            },
            "headers": {"X-GitHub-Event": "issue_comment"},
            "sender": {"login": "octocat"},
        }
        identity = GitHubBotIdentity(
            name="Example App Bot",
            login="example-app[bot]",
            user_id="12345678",
            email="12345678+example-app[bot]@users.noreply.github.com",
            slug="example-app",
        )

        with mock.patch(
            "internals.manual_trigger.bot_identity_or_none", return_value=identity
        ):
            mention = self._workflow_signal_payload(
                {**base, "comment": {"id": 11, "body": "please @example-app"}}
            )
            slash = self._workflow_signal_payload(
                {**base, "comment": {"id": 12, "body": "/codex fix this"}}
            )
            other = self._workflow_signal_payload(
                {**base, "comment": {"id": 13, "body": "please @not-the-app"}}
            )
            substring = self._workflow_signal_payload(
                {**base, "comment": {"id": 14, "body": "mail@example-app.test"}}
            )

        self.assertEqual(mention["webhook_policy"]["id"], "app-mention-review")
        self.assertEqual(
            mention["webhook_policy"]["trigger"]["require_app_mention"], True
        )
        self.assertEqual(slash["webhook_policy"]["id"], "fallback")
        self.assertEqual(other["webhook_policy"]["id"], "fallback")
        self.assertEqual(substring["webhook_policy"]["id"], "fallback")

    def test_policy_exact_manual_command_rejects_verbose_suffix(self) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {
                        "id": "exact-review",
                        "match": {"events": ["issue_comment"], "actions": ["created"]},
                        "trigger": {
                            "frequency": "manual_only",
                            "manualCommands": ["gestalt review"],
                            "manualCommandMatch": "exact",
                        },
                    },
                    {
                        "id": "fallback",
                        "match": {"events": ["issue_comment"], "actions": ["created"]},
                    },
                ],
            },
        )
        base = {
            "action": "created",
            "installation": {"id": 99},
            "repository": {"full_name": "acme/widgets"},
            "issue": {
                "number": 7,
                "pull_request": {"html_url": "https://github.com/acme/widgets/pull/7"},
            },
            "headers": {"X-GitHub-Event": "issue_comment"},
            "sender": {"login": "octocat"},
        }

        exact = self._workflow_signal_payload(
            {**base, "comment": {"id": 11, "body": "  Gestalt   Review  "}}
        )
        verbose = self._workflow_signal_payload(
            {**base, "comment": {"id": 12, "body": "gestalt review verbose=true"}}
        )

        self.assertEqual(exact["webhook_policy"]["id"], "exact-review")
        self.assertEqual(verbose["webhook_policy"]["id"], "fallback")

    def test_pr_scoped_policy_does_not_treat_plain_issue_as_pr(self) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {
                        "id": "issue-comment-pr-scope",
                        "match": {
                            "events": ["issue_comment"],
                            "actions": ["created"],
                        },
                        "trigger": {"frequency": "once_per_pr"},
                        "dedupe": {"scope": "pull_request"},
                    }
                ],
            },
        )

        plain_issue = self._workflow_signal_request(
            {
                "action": "created",
                "installation": {"id": 99},
                "repository": {"full_name": "acme/widgets"},
                "issue": {
                    "number": 12,
                    "title": "Plain issue",
                    "html_url": "https://github.com/acme/widgets/issues/12",
                },
                "comment": {"id": 10, "body": "please review"},
                "headers": {"X-GitHub-Event": "issue_comment"},
                "sender": {"login": "octocat"},
            }
        )
        self.assertEqual(
            plain_issue.workflow_key,
            "github:99:acme/widgets:12:policy:issue-comment-pr-scope",
        )
        self.assertNotIn(":pr:12:", plain_issue.workflow_key)
        self.assertTrue(
            plain_issue.idempotency_key.startswith(
                "github:event:acme/widgets:policy:issue-comment-pr-scope:"
                "issue_comment:created:"
            )
        )

        pr_issue_comment = self._workflow_signal_request(
            {
                "action": "created",
                "installation": {"id": 99},
                "repository": {"full_name": "acme/widgets"},
                "issue": {
                    "number": 7,
                    "title": "PR issue",
                    "html_url": "https://github.com/acme/widgets/pull/7",
                    "pull_request": {
                        "html_url": "https://github.com/acme/widgets/pull/7"
                    },
                },
                "comment": {"id": 11, "body": "please review"},
                "headers": {"X-GitHub-Event": "issue_comment"},
                "sender": {"login": "octocat"},
            }
        )
        self.assertEqual(
            pr_issue_comment.workflow_key,
            "github:99:acme/widgets:pr:7:policy:issue-comment-pr-scope",
        )
        self.assertEqual(
            pr_issue_comment.idempotency_key,
            "github:trigger:once_per_pr:99:acme/widgets:pr:7:"
            "policy:issue-comment-pr-scope",
        )

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
                provider_module.BOT_GET_REPOSITORY_OPERATION,
                provider_module.BOT_SEARCH_CODE_OPERATION,
                provider_module.BOT_GET_CONTENT_OPERATION,
                provider_module.BOT_GET_PULL_REQUEST_OPERATION,
                provider_module.BOT_LIST_PULL_REQUEST_FILES_OPERATION,
                provider_module.BOT_LIST_PULL_REQUEST_REVIEW_THREADS_OPERATION,
                provider_module.BOT_GET_CHECK_RUN_OPERATION,
                provider_module.BOT_LIST_CHECK_SUITE_CHECK_RUNS_OPERATION,
                provider_module.BOT_LIST_CHECK_RUN_ANNOTATIONS_OPERATION,
                provider_module.BOT_GET_WORKFLOW_RUN_OPERATION,
                provider_module.BOT_LIST_WORKFLOW_RUN_JOBS_OPERATION,
                provider_module.BOT_CREATE_PULL_REQUEST_REVIEW_OPERATION,
                provider_module.BOT_CREATE_PULL_REQUEST_CONVERSATION_COMMENT_OPERATION,
                provider_module.BOT_CREATE_ISSUE_COMMENT_OPERATION,
            ],
        )
        metadata = sdk_value_to_dict(agent.metadata)
        self.assertEqual(metadata["github"]["policy"]["id"], "failed-ci-comment")
        self.assertEqual(metadata["github"]["policy"]["mode"], "comment")

        data = cast(
            dict[str, Any],
            sdk_value_to_dict(request.signal.payload),
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

    def test_ci_policy_suppresses_stale_head_before_dispatch(self) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "agent": {"provider": "simple", "model": "deep"},
                "webhookPolicies": [
                    {
                        "id": "failed-ci-comment",
                        "match": {
                            "events": ["check_suite"],
                            "actions": ["completed"],
                            "conclusions": ["failure"],
                        },
                        "comments": {"suppressStaleHead": True},
                        "action": {"mode": "comment"},
                    }
                ],
            },
        )
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
                self.assertEqual(body["permissions"], {"pull_requests": "read"})
                return FakeHTTPResponse({"token": "pull-token"})
            if path == "/repos/acme/widgets/pulls/7":
                self.assertEqual(auth_header(request), "Bearer pull-token")
                return FakeHTTPResponse({"number": 7, "head": {"sha": "current-sha"}})
            self.fail(f"unexpected request {method} {path}")

        workflow_manager = FakeWorkflowManager()
        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            result = provider_module.github_events_handle(
                {
                    "action": "completed",
                    "installation": {"id": 99},
                    "repository": {
                        "full_name": "acme/widgets",
                        "name": "widgets",
                        "owner": {"login": "acme"},
                    },
                    "check_suite": {
                        "id": 321,
                        "status": "completed",
                        "conclusion": "failure",
                        "head_sha": "stale-sha",
                        "pull_requests": [{"number": 7}],
                    },
                    "headers": {"X-GitHub-Event": "check_suite"},
                    "sender": {"login": "octocat"},
                },
                github_request(),
            )

        self.assertEqual(
            result, {"ok": True, "ignored": "stale_head:stale-sha:current-sha"}
        )
        self.assertEqual(workflow_manager.requests, [])
        self.assertEqual(len(calls), 2)

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
                        "credentialMode": "none",
                        "input": {
                            "maxComments": 10,
                            "changedLinesOnly": True,
                            "dryRun": True,
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
            sdk_value_to_dict(plugin_request.signal.payload),
            sdk_value_to_dict(agent_request.signal.payload),
        )
        self.assertEqual(
            sdk_value_to_dict(plugin_request.signal.metadata),
            sdk_value_to_dict(agent_request.signal.metadata),
        )
        self.assertIsNotNone(agent_request.target.agent)
        self.assertIsNotNone(plugin_request.target.plugin)

        plugin = plugin_request.target.plugin
        self.assertEqual(plugin.plugin_name, "github")
        self.assertEqual(plugin.operation, "reviewPullRequest")
        self.assertEqual(plugin.connection, "review-bot")
        self.assertEqual(plugin.instance, "prod")
        self.assertEqual(plugin.credential_mode, "none")

        target_input = sdk_value_to_dict(plugin.input)
        self.assertEqual(target_input["maxComments"], 10)
        self.assertEqual(target_input["changedLinesOnly"], True)
        self.assertEqual(target_input["dryRun"], True)
        self.assertNotIn("pull_request", target_input)
        self.assertNotIn("repository", target_input)

        signal_payload = sdk_value_to_dict(plugin_request.signal.payload)
        self.assertEqual(signal_payload["repository"]["full_name"], "acme/widgets")
        self.assertEqual(signal_payload["agent_request"]["pull_request"]["number"], 7)

    def test_builtin_review_policy_creates_check_run_before_workflow_enqueue(
        self,
    ) -> None:
        events: list[str] = []

        class RecordingWorkflowManager(FakeWorkflowManager):
            def signal_or_start_run(self, request: Any) -> Any:
                events.append("workflow")
                return super().signal_or_start_run(request)

        workflow_manager = RecordingWorkflowManager()
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "webhookPolicies": [
                    {
                        "id": "pr-review",
                        "match": {
                            "events": ["pull_request"],
                            "actions": ["synchronize"],
                        },
                        "workflow": {
                            "target": {
                                "plugin": {
                                    "plugin": "github",
                                    "operation": "reviewPullRequest",
                                    "input": {"checkRunName": "Bugbot Review"},
                                }
                            }
                        },
                    }
                ],
            },
        )

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            if path == "/app/installations/99/access_tokens":
                if body["permissions"] == {"checks": "read"}:
                    return FakeHTTPResponse({"token": "checks-read-token"})
                if body["permissions"] == {"checks": "write"}:
                    return FakeHTTPResponse({"token": "checks-write-token"})
            if path == "/repos/acme/widgets/commits/abc123/check-runs":
                events.append("check-list")
                self.assertEqual(method, "GET")
                self.assertEqual(auth_header(request), "Bearer checks-read-token")
                self.assertEqual(
                    urllib.parse.parse_qs(
                        urllib.parse.urlparse(request.full_url).query
                    ),
                    {
                        "check_name": ["Bugbot Review"],
                        "filter": ["all"],
                        "per_page": ["100"],
                        "page": ["1"],
                    },
                )
                return FakeHTTPResponse({"total_count": 0, "check_runs": []})
            if path == "/repos/acme/widgets/check-runs":
                events.append("check-create")
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer checks-write-token")
                self.assertEqual(body["name"], "Bugbot Review")
                self.assertEqual(body["head_sha"], "abc123")
                self.assertEqual(body["status"], "in_progress")
                self.assertTrue(body["external_id"].startswith("gestalt-review:"))
                return FakeHTTPResponse(
                    {
                        "id": 456,
                        "name": body["name"],
                        "status": body["status"],
                        "external_id": body["external_id"],
                        "head_sha": body["head_sha"],
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
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            result = provider_module.github_events_handle(
                self._review_pull_request_payload(), github_request()
            )

        self.assertEqual(result["ok"], True)
        self.assertEqual(events, ["check-list", "check-create", "workflow"])
        self.assertEqual(len(workflow_manager.requests), 1)
        signal_payload = sdk_value_to_dict(
            workflow_manager.requests[0].signal.payload
        )
        self.assertEqual(signal_payload["review_check_run"]["id"], 456)
        self.assertEqual(signal_payload["review_check_run"]["name"], "Bugbot Review")

    def test_builtin_review_policy_check_run_failure_prevents_workflow_enqueue(
        self,
    ) -> None:
        workflow_manager = FakeWorkflowManager()
        self._configure_builtin_review_policy()

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            path = request_path(request)
            body = request_json(request)
            if path == "/app/installations/99/access_tokens":
                if body["permissions"] == {"checks": "read"}:
                    return FakeHTTPResponse({"token": "checks-read-token"})
                if body["permissions"] == {"checks": "write"}:
                    return FakeHTTPResponse({"token": "checks-write-token"})
            if path == "/repos/acme/widgets/commits/abc123/check-runs":
                return FakeHTTPResponse({"total_count": 0, "check_runs": []})
            if path == "/repos/acme/widgets/check-runs":
                raise http_error(
                    request.full_url,
                    HTTPStatus.FORBIDDEN,
                    '{"message":"Checks write denied"}',
                )
            self.fail(f"unexpected request {request.get_method()} {path}")

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            result = provider_module.github_events_handle(
                self._review_pull_request_payload(), github_request()
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, Any]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertEqual(workflow_manager.requests, [])

    def test_builtin_review_policy_marks_check_run_failed_when_enqueue_fails(
        self,
    ) -> None:
        events: list[str] = []
        workflow_manager = FakeWorkflowManager(fail=True)
        self._configure_builtin_review_policy()

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            if path == "/app/installations/99/access_tokens":
                if body["permissions"] == {"checks": "read"}:
                    return FakeHTTPResponse({"token": "checks-read-token"})
                if body["permissions"] == {"checks": "write"}:
                    return FakeHTTPResponse({"token": "checks-write-token"})
            if path == "/repos/acme/widgets/commits/abc123/check-runs":
                return FakeHTTPResponse({"total_count": 0, "check_runs": []})
            if path == "/repos/acme/widgets/check-runs" and method == "POST":
                events.append("check-create")
                return FakeHTTPResponse(
                    {
                        "id": 456,
                        "name": body["name"],
                        "status": body["status"],
                        "external_id": body["external_id"],
                        "head_sha": body["head_sha"],
                    }
                )
            if path == "/repos/acme/widgets/check-runs/456" and method == "PATCH":
                events.append("check-failure")
                self.assertEqual(body["conclusion"], "failure")
                self.assertEqual(body["output"]["title"], "Review dispatch failed")
                return FakeHTTPResponse(
                    {
                        "id": 456,
                        "name": "Gestalt Review",
                        "status": "completed",
                        "conclusion": "failure",
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
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            result = provider_module.github_events_handle(
                self._review_pull_request_payload(), github_request()
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, Any]], result)
        self.assertEqual(response.status, HTTPStatus.SERVICE_UNAVAILABLE)
        self.assertEqual(events, ["check-create", "check-failure"])

    def test_builtin_review_policy_reuses_matching_active_check_run(self) -> None:
        events: list[str] = []
        workflow_manager = FakeWorkflowManager()
        self._configure_builtin_review_policy()

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            if path == "/app/installations/99/access_tokens":
                if body["permissions"] == {"checks": "read"}:
                    return FakeHTTPResponse({"token": "checks-read-token"})
                if body["permissions"] == {"checks": "write"}:
                    return FakeHTTPResponse({"token": "checks-write-token"})
            if path == "/repos/acme/widgets/commits/abc123/check-runs":
                external_id = "gestalt-review:" + "a" * 64
                return FakeHTTPResponse(
                    {
                        "total_count": 1,
                        "check_runs": [
                            {
                                "id": 456,
                                "name": "Gestalt Review",
                                "status": "in_progress",
                                "external_id": external_id,
                                "head_sha": "abc123",
                            }
                        ],
                    }
                )
            if path == "/repos/acme/widgets/check-runs/456" and method == "PATCH":
                events.append("check-update")
                self.assertEqual(body["status"], "in_progress")
                return FakeHTTPResponse(
                    {
                        "id": 456,
                        "name": "Gestalt Review",
                        "status": "in_progress",
                        "external_id": "gestalt-review:" + "a" * 64,
                        "head_sha": "abc123",
                    }
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch(
                "provider._review_check_run_external_id",
                return_value="gestalt-review:" + "a" * 64,
            ),
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            result = provider_module.github_events_handle(
                self._review_pull_request_payload(), github_request()
            )

        self.assertEqual(result["ok"], True)
        self.assertEqual(events, ["check-update"])
        payload = sdk_value_to_dict(workflow_manager.requests[0].signal.payload)
        self.assertEqual(payload["review_check_run"]["id"], 456)

    def test_builtin_review_policy_reuses_matching_failed_completed_check_run(
        self,
    ) -> None:
        events: list[str] = []
        workflow_manager = FakeWorkflowManager()
        self._configure_builtin_review_policy()

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            if path == "/app/installations/99/access_tokens":
                if body["permissions"] == {"checks": "read"}:
                    return FakeHTTPResponse({"token": "checks-read-token"})
                if body["permissions"] == {"checks": "write"}:
                    raise AssertionError("completed check should not be updated")
            if path == "/repos/acme/widgets/commits/abc123/check-runs":
                events.append("check-list")
                external_id = "gestalt-review:" + "a" * 64
                return FakeHTTPResponse(
                    {
                        "total_count": 1,
                        "check_runs": [
                            {
                                "id": 456,
                                "name": "Gestalt Review",
                                "status": "completed",
                                "conclusion": "failure",
                                "external_id": external_id,
                                "head_sha": "abc123",
                            }
                        ],
                    }
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch(
                "provider._review_check_run_external_id",
                return_value="gestalt-review:" + "a" * 64,
            ),
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            result = provider_module.github_events_handle(
                self._review_pull_request_payload(), github_request()
            )

        self.assertEqual(result["ok"], True)
        self.assertEqual(events, ["check-list"])
        payload = sdk_value_to_dict(workflow_manager.requests[0].signal.payload)
        self.assertEqual(payload["review_check_run"]["id"], 456)
        self.assertEqual(payload["review_check_run"]["status"], "completed")
        self.assertEqual(payload["review_check_run"]["conclusion"], "failure")

    def test_builtin_review_policy_restarts_matching_dispatch_failed_check_run(
        self,
    ) -> None:
        events: list[str] = []
        workflow_manager = FakeWorkflowManager()
        self._configure_builtin_review_policy()

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            if path == "/app/installations/99/access_tokens":
                if body["permissions"] == {"checks": "read"}:
                    return FakeHTTPResponse({"token": "checks-read-token"})
                if body["permissions"] == {"checks": "write"}:
                    return FakeHTTPResponse({"token": "checks-write-token"})
            if path == "/repos/acme/widgets/commits/abc123/check-runs":
                events.append("check-list")
                external_id = "gestalt-review:" + "a" * 64
                return FakeHTTPResponse(
                    {
                        "total_count": 1,
                        "check_runs": [
                            {
                                "id": 456,
                                "name": "Gestalt Review",
                                "status": "completed",
                                "conclusion": "failure",
                                "external_id": external_id,
                                "head_sha": "abc123",
                                "output": {"title": "Review dispatch failed"},
                            }
                        ],
                    }
                )
            if path == "/repos/acme/widgets/check-runs/456" and method == "PATCH":
                events.append("check-update")
                self.assertEqual(body["status"], "in_progress")
                self.assertEqual(body["output"]["title"], "Review running")
                return FakeHTTPResponse(
                    {
                        "id": 456,
                        "name": "Gestalt Review",
                        "status": "in_progress",
                        "external_id": "gestalt-review:" + "a" * 64,
                        "head_sha": "abc123",
                    }
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch(
                "provider._review_check_run_external_id",
                return_value="gestalt-review:" + "a" * 64,
            ),
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            result = provider_module.github_events_handle(
                self._review_pull_request_payload(), github_request()
            )

        self.assertEqual(result["ok"], True)
        self.assertEqual(events, ["check-list", "check-update"])
        payload = sdk_value_to_dict(workflow_manager.requests[0].signal.payload)
        self.assertEqual(payload["review_check_run"]["id"], 456)
        self.assertEqual(payload["review_check_run"]["status"], "in_progress")

    def test_manual_review_comment_fetches_pr_head_before_check_run_creation(
        self,
    ) -> None:
        events: list[str] = []
        workflow_manager = FakeWorkflowManager()
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "webhookPolicies": [
                    {
                        "id": "manual-review",
                        "match": {
                            "events": ["issue_comment"],
                            "actions": ["created"],
                        },
                        "trigger": {
                            "frequency": "manual_only",
                            "manualCommands": ["gestalt review"],
                        },
                        "workflow": {
                            "target": {
                                "plugin": {
                                    "plugin": "github",
                                    "operation": "reviewPullRequest",
                                }
                            }
                        },
                    }
                ],
            },
        )

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            if path == "/app/installations/99/access_tokens":
                if body["permissions"] == {"pull_requests": "read"}:
                    return FakeHTTPResponse({"token": "pull-token"})
                if body["permissions"] == {"checks": "read"}:
                    return FakeHTTPResponse({"token": "checks-read-token"})
                if body["permissions"] == {"checks": "write"}:
                    return FakeHTTPResponse({"token": "checks-write-token"})
            if path == "/repos/acme/widgets/pulls/7":
                events.append("pull")
                return FakeHTTPResponse(
                    {
                        "number": 7,
                        "head": {"ref": "feature", "sha": "abc123"},
                        "base": {"ref": "main", "sha": "def456"},
                    }
                )
            if path == "/repos/acme/widgets/commits/abc123/check-runs":
                events.append("check-list")
                return FakeHTTPResponse({"total_count": 0, "check_runs": []})
            if path == "/repos/acme/widgets/check-runs" and method == "POST":
                events.append("check-create")
                self.assertEqual(body["head_sha"], "abc123")
                return FakeHTTPResponse(
                    {
                        "id": 456,
                        "name": body["name"],
                        "status": body["status"],
                        "external_id": body["external_id"],
                        "head_sha": body["head_sha"],
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
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            payload = self._manual_review_comment_payload()
            payload["comment"]["body"] = "please gestalt review"
            result = provider_module.github_events_handle(payload, github_request())

        self.assertEqual(result["ok"], True)
        self.assertEqual(events, ["pull", "check-list", "check-create"])
        payload = sdk_value_to_dict(workflow_manager.requests[0].signal.payload)
        self.assertEqual(payload["review_check_run"]["head_sha"], "abc123")

    def test_app_mention_manual_review_creates_check_run(self) -> None:
        events: list[str] = []
        workflow_manager = FakeWorkflowManager()
        identity = GitHubBotIdentity(
            name="Example App Bot",
            login="example-app[bot]",
            user_id="12345678",
            email="12345678+example-app[bot]@users.noreply.github.com",
            slug="example-app",
        )
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "webhookPolicies": [
                    {
                        "id": "manual-review",
                        "match": {
                            "events": ["issue_comment"],
                            "actions": ["created"],
                        },
                        "trigger": {
                            "frequency": "manual_only",
                            "requireAppMention": True,
                        },
                        "workflow": {
                            "target": {
                                "plugin": {
                                    "plugin": "github",
                                    "operation": "reviewPullRequest",
                                }
                            }
                        },
                    }
                ],
            },
        )

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)
            if path == "/app/installations/99/access_tokens":
                if body["permissions"] == {"pull_requests": "read"}:
                    return FakeHTTPResponse({"token": "pull-token"})
                if body["permissions"] == {"checks": "read"}:
                    return FakeHTTPResponse({"token": "checks-read-token"})
                if body["permissions"] == {"checks": "write"}:
                    return FakeHTTPResponse({"token": "checks-write-token"})
            if path == "/repos/acme/widgets/pulls/7":
                events.append("pull")
                return FakeHTTPResponse(
                    {
                        "number": 7,
                        "head": {"ref": "feature", "sha": "abc123"},
                        "base": {"ref": "main", "sha": "def456"},
                    }
                )
            if path == "/repos/acme/widgets/commits/abc123/check-runs":
                events.append("check-list")
                return FakeHTTPResponse({"total_count": 0, "check_runs": []})
            if path == "/repos/acme/widgets/check-runs" and method == "POST":
                events.append("check-create")
                self.assertEqual(body["head_sha"], "abc123")
                return FakeHTTPResponse(
                    {
                        "id": 456,
                        "name": body["name"],
                        "status": body["status"],
                        "external_id": body["external_id"],
                        "head_sha": body["head_sha"],
                    }
                )
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch(
                "internals.manual_trigger.bot_identity_or_none", return_value=identity
            ),
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
            ),
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
        ):
            payload = self._manual_review_comment_payload()
            payload["comment"]["body"] = "please @example-app"
            result = provider_module.github_events_handle(payload, github_request())

        self.assertEqual(result["ok"], True)
        self.assertEqual(events, ["pull", "check-list", "check-create"])
        payload = sdk_value_to_dict(workflow_manager.requests[0].signal.payload)
        self.assertEqual(
            payload["webhook_policy"]["trigger"]["require_app_mention"], True
        )
        self.assertEqual(payload["review_check_run"]["head_sha"], "abc123")

    def test_review_comment_event_does_not_create_check_run(self) -> None:
        workflow_manager = FakeWorkflowManager()
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "webhookPolicies": [
                    {
                        "id": "review-comment",
                        "match": {
                            "events": ["pull_request_review_comment"],
                            "actions": ["created"],
                        },
                        "workflow": {
                            "target": {
                                "plugin": {
                                    "plugin": "github",
                                    "operation": "reviewPullRequest",
                                }
                            }
                        },
                    }
                ],
            },
        )

        with (
            mock.patch.object(
                gestalt.Request,
                "workflow_manager",
                return_value=workflow_manager,
                create=True,
            ),
            mock.patch(
                "internals.client.urllib.request.urlopen",
                side_effect=AssertionError("GitHub should not be called"),
            ),
        ):
            result = provider_module.github_events_handle(
                {
                    "action": "created",
                    "installation": {"id": 99},
                    "repository": {
                        "full_name": "acme/widgets",
                        "name": "widgets",
                        "owner": {"login": "acme"},
                    },
                    "pull_request": {
                        "number": 7,
                        "head": {"ref": "feature", "sha": "abc123"},
                        "base": {"ref": "main", "sha": "def456"},
                    },
                    "comment": {"id": 10, "body": "gestalt review"},
                    "headers": {"X-GitHub-Event": "pull_request_review_comment"},
                    "sender": {"login": "octocat"},
                },
                github_request(),
            )

        self.assertEqual(result["ok"], True)
        payload = sdk_value_to_dict(workflow_manager.requests[0].signal.payload)
        self.assertNotIn("review_check_run", payload)

    def test_review_pull_request_posts_validated_inline_comments(self) -> None:
        agent_manager = FakeAgentManager(
            findings=[],
            output_text=json.dumps(
                {
                    "findings": [
                        {
                            "path": "src/widget.py",
                            "line": 2,
                            "body": "This can throw when config is missing.",
                            "severity": "high",
                        },
                        {
                            "path": "src/widget.py",
                            "line": 1,
                            "body": (
                                "This is context and cannot receive a RIGHT-side "
                                "comment."
                            ),
                        },
                    ]
                }
            ),
            require_no_response_schema=True,
        )
        created_reviews: list[Any] = []

        def fake_create_pull_request_review(
            request: Any, *, subject: Any, external_identity: Any = None
        ) -> Any:
            self.assertIsNone(external_identity)
            created_reviews.append((request, subject))
            return {
                "id": 80,
                "state": "COMMENTED",
                "html_url": "https://github.com/acme/widgets/pull/7#pullrequestreview-80",
                "commit_id": "abc123",
                "body": "Automated review found 1 concrete issue.",
                "user": {"login": "example-app[bot]"},
            }

        request = request_with_tool_refs(
            github_request(),
            [
                gestalt.AgentToolRef(plugin="linear", operation="issue.get"),
                gestalt.AgentToolRef(plugin="github", operation="bot.getPullRequest"),
            ],
        )
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
                        "webhook_policy": {
                            "tool_refs": [
                                "bot.getPullRequest",
                                "bot.listPullRequestFiles",
                                "bot.commitFiles",
                            ],
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
        prompt_data = json.loads(prompt)
        self.assertEqual(
            prompt_data["output_contract"]["contract"],
            "github.pull_request_review.findings.v1",
        )
        self.assertEqual(
            prompt_data["output_contract"]["empty_response"], {"findings": []}
        )
        self.assertIn(
            "Use only added RIGHT-side lines",
            prompt_data["output_contract"]["line_policy"],
        )
        self.assertFalse(getattr(agent_manager.turns[0], "response_schema", None))
        self.assertEqual(
            [(ref.plugin, ref.operation) for ref in agent_manager.turns[0].tool_refs],
            [
                ("linear", "issue.get"),
                ("github", "bot.getPullRequest"),
            ],
        )
        self.assertEqual(
            agent_manager.turns[0].tool_source,
            gestalt.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG,
        )

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
            r"<!-- gestalt:github-review-finding v2 "
            r"fingerprint=[0-9a-f]{64} stable_fingerprint=[0-9a-f]{64} "
            r"source=github\.reviewPullRequest -->$",
        )

    def test_review_pull_request_commits_same_pr_self_fix(self) -> None:
        agent_manager = FakeAgentManager(
            findings=[],
            output_texts=[
                json.dumps(
                    {
                        "findings": [
                            {
                                "path": "src/widget.py",
                                "line": 2,
                                "body": "This can throw when config is missing.",
                            }
                        ]
                    }
                ),
                json.dumps(
                    {
                        "commit_message": "Fix widget config handling",
                        "files": [
                            {
                                "path": "src/widget.py",
                                "content": "bad = False\n",
                            }
                        ],
                    }
                ),
            ],
            require_no_response_schema=True,
        )
        commits: list[Any] = []
        created_checks: list[Any] = []
        updated_checks: list[Any] = []
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
                        "review_check_run": {
                            "id": 777,
                            "name": "Gestalt Review",
                            "status": "in_progress",
                            "head_sha": "abc123",
                        },
                        "webhook_policy": {
                            "tool_refs": [
                                "bot.getPullRequest",
                                "bot.listPullRequestFiles",
                                "bot.commitFiles",
                            ],
                            "action": {
                                "allow_self_fix": True,
                                "self_fix_mode": "branch_commit",
                            },
                        },
                    }
                }
            ]
        }

        def fake_commit_files(
            request: Any,
            *,
            subject: Any,
            pull_request_permissions: bool,
            external_identity: Any = None,
        ) -> operations_module.CommitResult:
            commits.append(
                (request, subject, pull_request_permissions, external_identity)
            )
            return operations_module.CommitResult(
                owner=request.owner,
                repo=request.repo,
                branch=request.branch,
                base_branch=request.base_branch,
                installation_id=request.installation_id,
                commit_sha="new-commit",
                commit_url="https://github.example/acme/widgets/commit/new-commit",
                tree_sha="new-tree",
                branch_created=False,
                files_changed=len(request.files),
            )

        def fake_create_check_run(request: Any, *, subject: Any) -> dict[str, Any]:
            created_checks.append((request, subject))
            return {
                "id": 888,
                "name": request.name,
                "status": "completed",
                "conclusion": request.conclusion,
                "head_sha": request.head_sha,
            }

        def fake_update_check_run(request: Any, *, subject: Any) -> dict[str, Any]:
            updated_checks.append((request, subject))
            return {
                "id": request.check_run_id,
                "name": "Gestalt Review",
                "status": "completed",
                "conclusion": request.conclusion,
                "head_sha": "abc123",
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
                    "head": {
                        "ref": "feature",
                        "sha": "abc123",
                        "repo": {"full_name": "acme/widgets"},
                    },
                    "base": {
                        "ref": "main",
                        "sha": "def456",
                        "repo": {"full_name": "acme/widgets"},
                    },
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
                "internals.review.get_file_text_at_ref",
                return_value="bad = True\n",
            ),
            mock.patch("internals.review.commit_files", side_effect=fake_commit_files),
            mock.patch(
                "internals.review.create_check_run", side_effect=fake_create_check_run
            ),
            mock.patch(
                "internals.review.update_check_run", side_effect=fake_update_check_run
            ),
            mock.patch(
                "internals.review.create_pull_request_review",
                side_effect=AssertionError("self-fix should not post stale comments"),
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(autoResolveStaleFindings=False),
                request,
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["posted"], False)
        self.assertEqual(data["comments"], 0)
        self.assertEqual(data["selfFix"]["status"], "committed")
        self.assertEqual(data["selfFix"]["commit"]["sha"], "new-commit")
        self.assertEqual(data["checkRun"]["head_sha"], "new-commit")
        self.assertEqual(len(agent_manager.turns), 2)
        expected_tool_refs = [
            ("github", "bot.getPullRequest"),
            ("github", "bot.listPullRequestFiles"),
            ("github", "bot.commitFiles"),
        ]
        self.assertEqual(
            [(ref.plugin, ref.operation) for ref in agent_manager.turns[0].tool_refs],
            expected_tool_refs,
        )
        self.assertEqual(
            [(ref.plugin, ref.operation) for ref in agent_manager.turns[1].tool_refs],
            expected_tool_refs,
        )
        self.assertEqual(
            agent_manager.turns[0].tool_source,
            gestalt.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG,
        )
        self.assertEqual(
            agent_manager.turns[1].tool_source,
            gestalt.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG,
        )
        self.assertIn("Fix concrete", agent_manager.turns[1].messages[0].text)
        self.assertNotIn("findings array", agent_manager.turns[1].messages[0].text)
        fix_prompt = json.loads(agent_manager.turns[1].messages[1].text)
        self.assertEqual(
            fix_prompt["output_contract"]["contract"],
            "github.pull_request_review.self_fix.v1",
        )
        self.assertEqual(len(commits), 1)
        commit_request = commits[0][0]
        self.assertEqual(commit_request.branch, "feature")
        self.assertEqual(commit_request.base_branch, "main")
        self.assertEqual(commit_request.expected_head_sha, "abc123")
        self.assertEqual(commit_request.files[0].path, "src/widget.py")
        self.assertEqual(commit_request.files[0].content, "bad = False\n")
        self.assertEqual(created_checks[0][0].head_sha, "new-commit")
        self.assertEqual(created_checks[0][0].conclusion, "success")
        self.assertEqual(updated_checks[0][0].check_run_id, 777)

    def test_review_pull_request_self_fix_requires_commit_tool_ref(self) -> None:
        agent_manager = FakeAgentManager(
            findings=[],
            output_text=json.dumps(
                {
                    "findings": [
                        {
                            "path": "src/widget.py",
                            "line": 2,
                            "body": "This can throw when config is missing.",
                        }
                    ]
                }
            ),
            require_no_response_schema=True,
        )
        created_reviews: list[Any] = []
        request = request_with_tool_refs(github_request(), [])
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
                        "webhook_policy": {
                            "tool_refs": [provider_module.BOT_COMMIT_FILES_OPERATION],
                            "action": {
                                "allow_self_fix": True,
                                "self_fix_mode": "branch_commit",
                            },
                        },
                    }
                }
            ]
        }

        def fake_create_pull_request_review(
            request: Any, *, subject: Any, external_identity: Any = None
        ) -> Any:
            created_reviews.append((request, subject))
            return {"id": 80, "state": "COMMENTED", "commit_id": "abc123"}

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
                    "head": {
                        "ref": "feature",
                        "sha": "abc123",
                        "repo": {"full_name": "acme/widgets"},
                    },
                    "base": {
                        "ref": "main",
                        "sha": "def456",
                        "repo": {"full_name": "acme/widgets"},
                    },
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
                "internals.review.commit_files",
                side_effect=AssertionError("commitFiles is not an effective tool"),
            ),
            mock.patch(
                "internals.review.create_pull_request_review",
                side_effect=fake_create_pull_request_review,
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(autoResolveStaleFindings=False),
                request,
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["posted"], True)
        self.assertNotIn("selfFix", data)
        self.assertEqual(len(agent_manager.turns), 1)
        self.assertEqual(agent_manager.turns[0].tool_refs, [])
        self.assertEqual(
            agent_manager.turns[0].tool_source,
            gestalt.AGENT_TOOL_SOURCE_MODE_MCP_CATALOG,
        )
        self.assertEqual(len(created_reviews), 1)

    def test_review_pull_request_self_fix_falls_back_when_head_is_stale(self) -> None:
        agent_manager = FakeAgentManager(
            findings=[],
            output_texts=[
                json.dumps(
                    {
                        "findings": [
                            {
                                "path": "src/widget.py",
                                "line": 2,
                                "body": "This can throw when config is missing.",
                            }
                        ]
                    }
                ),
                json.dumps(
                    {
                        "files": [
                            {
                                "path": "src/widget.py",
                                "content": "bad = False\n",
                            }
                        ]
                    }
                ),
            ],
            require_no_response_schema=True,
        )
        created_reviews: list[Any] = []
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
                        "webhook_policy": {
                            "tool_refs": ["bot.commitFiles"],
                            "action": {
                                "allow_self_fix": True,
                                "self_fix_mode": "branch_commit",
                            },
                        },
                    }
                }
            ]
        }

        def fake_create_pull_request_review(
            request: Any, *, subject: Any, external_identity: Any = None
        ) -> Any:
            created_reviews.append((request, subject))
            return {
                "id": 80,
                "state": "COMMENTED",
                "commit_id": "abc123",
                "body": "Automated review found 1 concrete issue.",
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
                    "head": {
                        "ref": "feature",
                        "sha": "abc123",
                        "repo": {"full_name": "acme/widgets"},
                    },
                    "base": {
                        "ref": "main",
                        "sha": "def456",
                        "repo": {"full_name": "acme/widgets"},
                    },
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
                "internals.review.get_file_text_at_ref",
                return_value="bad = True\n",
            ),
            mock.patch(
                "internals.review.commit_files",
                side_effect=ValueError("branch head changed since expected_head_sha"),
            ),
            mock.patch(
                "internals.review.create_pull_request_review",
                side_effect=fake_create_pull_request_review,
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(autoResolveStaleFindings=False),
                request,
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["posted"], True)
        self.assertEqual(data["comments"], 1)
        self.assertEqual(data["selfFix"]["status"], "skipped")
        self.assertIn("branch head changed", data["selfFix"]["reason"])
        self.assertEqual(len(created_reviews), 1)

    def test_review_pull_request_self_fix_falls_back_for_unsafe_file(self) -> None:
        agent_manager = FakeAgentManager(
            findings=[],
            output_text=json.dumps(
                {
                    "findings": [
                        {
                            "path": "src/widget.py",
                            "line": 2,
                            "body": "This can throw when config is missing.",
                        }
                    ]
                }
            ),
            require_no_response_schema=True,
        )
        created_reviews: list[Any] = []
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
                        "webhook_policy": {
                            "tool_refs": ["bot.commitFiles"],
                            "action": {
                                "allow_self_fix": True,
                                "self_fix_mode": "branch_commit",
                            },
                        },
                    }
                }
            ]
        }

        def fake_create_pull_request_review(
            request: Any, *, subject: Any, external_identity: Any = None
        ) -> Any:
            created_reviews.append((request, subject))
            return {"id": 80, "state": "COMMENTED", "commit_id": "abc123"}

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
                    "head": {
                        "ref": "feature",
                        "sha": "abc123",
                        "repo": {"full_name": "acme/widgets"},
                    },
                    "base": {
                        "ref": "main",
                        "sha": "def456",
                        "repo": {"full_name": "acme/widgets"},
                    },
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
                "internals.review.get_file_text_at_ref",
                side_effect=ValueError("src/widget.py: content is not UTF-8 text"),
            ),
            mock.patch(
                "internals.review.commit_files",
                side_effect=AssertionError("unsafe content should not commit"),
            ),
            mock.patch(
                "internals.review.create_pull_request_review",
                side_effect=fake_create_pull_request_review,
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(autoResolveStaleFindings=False),
                request,
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["posted"], True)
        self.assertEqual(data["selfFix"]["status"], "skipped")
        self.assertIn("not UTF-8 text", data["selfFix"]["reason"])
        self.assertEqual(len(agent_manager.turns), 1)
        self.assertEqual(len(created_reviews), 1)

    def test_review_pull_request_accepts_contains_issue_comment_and_updates_check(
        self,
    ) -> None:
        agent_manager = FakeAgentManager(findings=[])
        created_checks: list[Any] = []
        updated_checks: list[Any] = []
        request = github_request()
        request.workflow = {
            "signals": [
                {
                    "payload": {
                        "github_event": "issue_comment",
                        "github_action": "created",
                        "installation": {"id": 99},
                        "repository": {"full_name": "acme/widgets"},
                        "summary": {"repository": "acme/widgets", "number": 7},
                        "webhook_policy": {
                            "trigger": {
                                "manual_commands": ["gestalt review"],
                            }
                        },
                        "agent_request": {
                            "issue": {"number": 7, "is_pull_request": True},
                            "comment": {"id": 10, "body": "please gestalt review"},
                        },
                    }
                }
            ]
        }

        def fake_create_check(request: Any, *, subject: Any) -> dict[str, Any]:
            created_checks.append((request, subject))
            return {
                "id": 501,
                "name": request.name,
                "status": "in_progress",
                "head_sha": request.head_sha,
            }

        def fake_update_check(request: Any, *, subject: Any) -> dict[str, Any]:
            updated_checks.append((request, subject))
            return {
                "id": request.check_run_id,
                "name": "Gestalt Review",
                "status": "completed",
                "conclusion": request.conclusion,
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
                    "draft": False,
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
                        "patch": "@@ -1 +1,2 @@\n context\n+ok = True",
                    }
                ],
            ),
            mock.patch(
                "internals.review.create_check_run", side_effect=fake_create_check
            ),
            mock.patch(
                "internals.review.update_check_run", side_effect=fake_update_check
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(
                    autoResolveStaleFindings=False,
                ),
                request,
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["reason"], "no_valid_findings")
        self.assertEqual(data["checkRun"]["conclusion"], "success")
        self.assertEqual(created_checks[0][0].head_sha, "abc123")
        self.assertEqual(updated_checks[0][0].conclusion, "success")

    def test_review_pull_request_accepts_app_mention_issue_comment(self) -> None:
        agent_manager = FakeAgentManager(findings=[])
        identity = GitHubBotIdentity(
            name="Example App Bot",
            login="example-app[bot]",
            user_id="12345678",
            email="12345678+example-app[bot]@users.noreply.github.com",
            slug="example-app",
        )
        request = github_request()
        request.workflow = {
            "signals": [
                {
                    "payload": {
                        "github_event": "issue_comment",
                        "github_action": "created",
                        "installation": {"id": 99},
                        "repository": {"full_name": "acme/widgets"},
                        "summary": {"repository": "acme/widgets", "number": 7},
                        "webhook_policy": {"trigger": {"require_app_mention": True}},
                        "agent_request": {
                            "issue": {"number": 7, "is_pull_request": True},
                            "comment": {"id": 10, "body": "please @example-app"},
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
                "internals.manual_trigger.bot_identity_or_none", return_value=identity
            ),
            mock.patch(
                "internals.review.get_pull_request",
                return_value={
                    "number": 7,
                    "draft": False,
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
                        "patch": "@@ -1 +1,2 @@\n context\n+ok = True",
                    }
                ],
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(
                    autoResolveStaleFindings=False,
                ),
                request,
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["reason"], "no_valid_findings")
        self.assertEqual(data["checkRun"]["conclusion"], "success")

    def test_review_pull_request_adopts_signal_check_run(self) -> None:
        agent_manager = FakeAgentManager(findings=[])
        updated_checks: list[Any] = []
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
                        "review_check_run": {
                            "id": 777,
                            "name": "Gestalt Review",
                            "status": "in_progress",
                            "head_sha": "abc123",
                        },
                    }
                }
            ]
        }

        def fake_update_check(request: Any, *, subject: Any) -> dict[str, Any]:
            updated_checks.append((request, subject))
            return {
                "id": request.check_run_id,
                "name": "Gestalt Review",
                "status": "completed",
                "conclusion": request.conclusion,
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
                    "draft": False,
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
                        "patch": "@@ -1 +1,2 @@\n context\n+ok = True",
                    }
                ],
            ),
            mock.patch(
                "internals.review.create_check_run",
                side_effect=AssertionError("signal check run should be adopted"),
            ),
            mock.patch(
                "internals.review.update_check_run", side_effect=fake_update_check
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(autoResolveStaleFindings=False),
                request,
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["checkRun"]["id"], 777)
        self.assertEqual(data["checkRun"]["conclusion"], "success")
        self.assertEqual(updated_checks[0][0].check_run_id, 777)

    def test_review_pull_request_marks_signal_check_failed_when_pull_fetch_fails(
        self,
    ) -> None:
        updated_checks: list[Any] = []
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
                        "review_check_run": {
                            "id": 777,
                            "name": "Gestalt Review",
                            "status": "in_progress",
                            "head_sha": "abc123",
                        },
                    }
                }
            ]
        }

        def fake_update_check(request: Any, *, subject: Any) -> dict[str, Any]:
            updated_checks.append((request, subject))
            return {
                "id": request.check_run_id,
                "name": "Gestalt Review",
                "status": "completed",
                "conclusion": request.conclusion,
            }

        with (
            mock.patch(
                "internals.review.get_pull_request",
                side_effect=GitHubAPIError(502, "GitHub unavailable"),
            ),
            mock.patch(
                "internals.review.update_check_run", side_effect=fake_update_check
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(autoResolveStaleFindings=False),
                request,
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, Any]], result)
        self.assertEqual(response.status, 502)
        self.assertEqual(updated_checks[0][0].check_run_id, 777)
        self.assertEqual(updated_checks[0][0].conclusion, "failure")

    def test_review_pull_request_marks_signal_check_failed_when_agent_fails(
        self,
    ) -> None:
        agent_manager = FakeAgentManager(
            findings=[], turn_error=Exception("agent rejected request")
        )
        updated_checks: list[Any] = []
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
                        "review_check_run": {
                            "id": 777,
                            "name": "Gestalt Review",
                            "status": "in_progress",
                            "head_sha": "abc123",
                        },
                    }
                }
            ]
        }

        def fake_update_check(request: Any, *, subject: Any) -> dict[str, Any]:
            updated_checks.append((request, subject))
            return {
                "id": request.check_run_id,
                "name": "Gestalt Review",
                "status": "completed",
                "conclusion": request.conclusion,
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
                    "draft": False,
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
                        "patch": "@@ -1 +1,2 @@\n context\n+ok = True",
                    }
                ],
            ),
            mock.patch(
                "internals.review.update_check_run", side_effect=fake_update_check
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(autoResolveStaleFindings=False),
                request,
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, Any]], result)
        self.assertEqual(response.status, 503)
        self.assertIn("review agent turn request failed", response.body["error"])
        self.assertEqual(updated_checks[0][0].check_run_id, 777)
        self.assertEqual(updated_checks[0][0].conclusion, "failure")

    def test_review_pull_request_marks_signal_check_failed_for_invalid_agent_output(
        self,
    ) -> None:
        cases: list[tuple[str, str, dict[str, Any] | None]] = [
            ("empty", "", None),
            ("invalid_json", "not json", None),
            ("non_object_json", '[{"findings":[]}]', None),
            ("missing_findings", "{}", None),
            ("non_array_findings", '{"findings":{}}', None),
            ("extra_top_level_key", '{"findings":[],"summary":"ok"}', None),
            ("multiple_objects", '{"findings":[]}{"findings":[]}', None),
            ("markdown_fence", '```json\n{"findings":[]}\n```', None),
            ("prose_wrapped", 'Here are findings: {"findings":[]}', None),
            (
                "structured_output_ignored",
                "not json",
                {
                    "findings": [
                        {
                            "path": "src/widget.py",
                            "line": 2,
                            "body": "This structured finding must be ignored.",
                        }
                    ]
                },
            ),
        ]
        for name, output_text, structured_output in cases:
            with self.subTest(name=name):
                agent_manager = FakeAgentManager(
                    findings=[],
                    output_text=output_text,
                    structured_output=structured_output,
                )
                updated_checks: list[Any] = []
                request = github_request()
                request.workflow = {
                    "signals": [
                        {
                            "payload": {
                                "github_event": "pull_request",
                                "github_action": "synchronize",
                                "installation": {"id": 99},
                                "repository": {"full_name": "acme/widgets"},
                                "summary": {
                                    "repository": "acme/widgets",
                                    "number": 7,
                                },
                                "review_check_run": {
                                    "id": 777,
                                    "name": "Gestalt Review",
                                    "status": "in_progress",
                                    "head_sha": "abc123",
                                },
                            }
                        }
                    ]
                }

                def fake_update_check(request: Any, *, subject: Any) -> dict[str, Any]:
                    updated_checks.append((request, subject))
                    return {
                        "id": request.check_run_id,
                        "name": "Gestalt Review",
                        "status": "completed",
                        "conclusion": request.conclusion,
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
                            "draft": False,
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
                                "patch": "@@ -1 +1,2 @@\n context\n+ok = True",
                            }
                        ],
                    ),
                    mock.patch(
                        "internals.review.create_pull_request_review",
                        side_effect=AssertionError("review should not be posted"),
                    ),
                    mock.patch(
                        "internals.review.list_pull_request_review_threads",
                        side_effect=AssertionError(
                            "stale threads should not be listed"
                        ),
                    ),
                    mock.patch(
                        "internals.review.resolve_pull_request_review_thread",
                        side_effect=AssertionError("thread should not be resolved"),
                    ),
                    mock.patch(
                        "internals.review.update_check_run",
                        side_effect=fake_update_check,
                    ),
                ):
                    result = provider_module.github_review_pull_request(
                        provider_module.ReviewPullRequestInput(
                            autoResolveStaleFindings=True
                        ),
                        request,
                    )

                self.assertIsInstance(result, gestalt.Response)
                response = cast(gestalt.Response[dict[str, Any]], result)
                self.assertEqual(response.status, 503)
                self.assertIn("agent", response.body["error"])
                self.assertEqual(updated_checks[0][0].check_run_id, 777)
                self.assertEqual(updated_checks[0][0].conclusion, "failure")

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
                                    review_module.ReviewFindingFingerprints(
                                        fingerprint="a" * 64,
                                        stable_fingerprint="a" * 64,
                                    ),
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
        self.assertEqual(list_thread_cursors, ["", "cursor-2", "", "cursor-2"])
        self.assertEqual(len(resolved_requests), 1)
        resolve_request, resolve_subject = resolved_requests[0]
        self.assertEqual(resolve_request.thread_id, "thread-stale")
        self.assertEqual(resolve_request.installation_id, 99)
        self.assertEqual(resolve_subject.id, request.subject.id)

    def test_review_pull_request_suppresses_materially_identical_existing_finding(
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
        existing_fingerprints = review_module.review_finding_fingerprints(
            subject,
            review_module.ValidatedFinding(
                path="src/widget.py",
                line=3,
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

        def fake_list_threads(request: Any, *, subject: Any) -> dict[str, Any]:
            return {
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
                                    existing_fingerprints,
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
                        "patch": "@@ -1 +1,3 @@\n context\n+bad = True\n+more = True",
                    }
                ],
            ),
            mock.patch(
                "internals.review.create_pull_request_review",
                side_effect=AssertionError("duplicate finding should not be posted"),
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
                side_effect=AssertionError("current finding should not be resolved"),
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(), request
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["posted"], False)
        self.assertEqual(data["comments"], 0)
        self.assertEqual(data["suppressedFindings"], 1)
        self.assertEqual(data["reason"], "duplicate_findings")
        self.assertEqual(
            data["skippedResolutionReasons"],
            [{"threadId": "thread-current", "reason": "current_finding"}],
        )

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
                                        review_module.ReviewFindingFingerprints(
                                            fingerprint=current_fingerprint,
                                            stable_fingerprint=current_fingerprint,
                                        ),
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
                                        review_module.ReviewFindingFingerprints(
                                            fingerprint="b" * 64,
                                            stable_fingerprint="b" * 64,
                                        ),
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
                                        review_module.ReviewFindingFingerprints(
                                            fingerprint="c" * 64,
                                            stable_fingerprint="c" * 64,
                                        ),
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

    def test_review_pull_request_allows_context_line_when_changed_lines_only_false(
        self,
    ) -> None:
        agent_manager = FakeAgentManager(
            findings=[
                {
                    "path": "src/widget.py",
                    "line": 1,
                    "body": "This context line can receive a RIGHT-side comment.",
                }
            ]
        )
        created_reviews: list[Any] = []
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

        def fake_create_pull_request_review(
            request: Any, *, subject: Any, external_identity: Any = None
        ) -> Any:
            created_reviews.append((request, subject))
            return {
                "id": 81,
                "state": "COMMENTED",
                "html_url": "https://github.com/acme/widgets/pull/7#pullrequestreview-81",
                "commit_id": "abc123",
                "body": "Automated review found 1 concrete issue.",
                "user": {"login": "example-app[bot]"},
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
                        "patch": "@@ -1,2 +1,3 @@\n context\n+added = True\n more",
                    }
                ],
            ),
            mock.patch(
                "internals.review.create_pull_request_review",
                side_effect=fake_create_pull_request_review,
            ),
        ):
            result = provider_module.github_review_pull_request(
                provider_module.ReviewPullRequestInput(changedLinesOnly=False),
                request,
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["posted"], True)
        self.assertEqual(data["comments"], 1)
        self.assertNotIn("droppedFindings", data)
        prompt_data = json.loads(agent_manager.turns[0].messages[1].text)
        self.assertIn(
            "Use RIGHT-side lines that are present",
            prompt_data["output_contract"]["line_policy"],
        )
        self.assertIn(
            "allowed by output_contract.line_policy",
            prompt_data["task"],
        )
        self.assertNotIn("added RIGHT-side diff lines only", prompt_data["task"])
        self.assertIn(
            "RIGHT-side lines allowed",
            agent_manager.turns[0].messages[0].text,
        )
        self.assertEqual(len(created_reviews), 1)
        review_request, _ = created_reviews[0]
        self.assertEqual(review_request.comments[0].path, "src/widget.py")
        self.assertEqual(review_request.comments[0].line, 1)
        self.assertEqual(review_request.comments[0].side, "RIGHT")

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
                provider_module.BOT_GET_REPOSITORY_OPERATION,
                provider_module.BOT_SEARCH_CODE_OPERATION,
                provider_module.BOT_GET_CONTENT_OPERATION,
                provider_module.BOT_GET_PULL_REQUEST_OPERATION,
                provider_module.BOT_LIST_PULL_REQUEST_FILES_OPERATION,
                provider_module.BOT_LIST_PULL_REQUEST_REVIEW_THREADS_OPERATION,
                provider_module.BOT_GET_CHECK_RUN_OPERATION,
                provider_module.BOT_LIST_CHECK_SUITE_CHECK_RUNS_OPERATION,
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
                {"webhookPolicies": [{"id": "bad-trigger", "trigger": "nope"}]},
                "trigger must be an object",
            ),
            (
                {
                    "webhookPolicies": [
                        {
                            "id": "bad-frequency",
                            "trigger": {"frequency": "hourly"},
                        }
                    ]
                },
                "trigger.frequency must be one of",
            ),
            (
                {
                    "webhookPolicies": [
                        {
                            "id": "manual-no-command",
                            "trigger": {"frequency": "manual_only"},
                        }
                    ]
                },
                "trigger.manualCommands or trigger.requireAppMention is required",
            ),
            (
                {
                    "webhookPolicies": [
                        {
                            "id": "manual-ambiguous",
                            "trigger": {
                                "frequency": "manual_only",
                                "manualCommands": ["gestalt review"],
                                "requireAppMention": True,
                            },
                        }
                    ]
                },
                "requireAppMention cannot be combined",
            ),
            (
                {
                    "webhookPolicies": [
                        {
                            "id": "bad-drafts",
                            "trigger": {"includeDrafts": "false"},
                        }
                    ]
                },
                "trigger.includeDrafts must be a boolean",
            ),
            (
                {
                    "webhookPolicies": [
                        {
                            "id": "bad-dedupe",
                            "dedupe": {"scope": "branch"},
                        }
                    ]
                },
                "dedupe.scope must be one of",
            ),
            (
                {
                    "webhookPolicies": [
                        {
                            "id": "bad-comments",
                            "comments": {"inlinePolicy": "summary_only"},
                        }
                    ]
                },
                "comments.inlinePolicy must be one of",
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
            (
                {
                    "webhookPolicies": [
                        {
                            "id": "bad-plugin-credential-mode",
                            "workflow": {
                                "target": {
                                    "plugin": {
                                        "plugin": "github",
                                        "operation": "reviewPullRequest",
                                        "credentialMode": "platform",
                                    }
                                }
                            },
                        }
                    ]
                },
                'workflow.target.plugin.credentialMode "platform" is not supported',
            ),
            (
                {
                    "webhookPolicies": [
                        {
                            "id": "snake-plugin-credential-mode",
                            "workflow": {
                                "target": {
                                    "plugin": {
                                        "plugin": "github",
                                        "operation": "reviewPullRequest",
                                        "credential_mode": "none",
                                    }
                                }
                            },
                        }
                    ]
                },
                "workflow.target.plugin.credential_mode is not supported",
            ),
            (
                {
                    "webhookPolicies": [
                        {
                            "id": "contradictory-review-target",
                            "comments": {"inlinePolicy": "never"},
                            "workflow": {
                                "target": {
                                    "plugin": {
                                        "plugin": "github",
                                        "operation": "reviewPullRequest",
                                    }
                                }
                            },
                        }
                    ]
                },
                "comments.inlinePolicy cannot be never",
            ),
            (
                {
                    "webhookPolicies": [
                        {
                            "id": "gated-review-target",
                            "action": {"allowCodeReviewComments": False},
                            "workflow": {
                                "target": {
                                    "plugin": {
                                        "plugin": "github",
                                        "operation": "reviewPullRequest",
                                    }
                                }
                            },
                        }
                    ]
                },
                "action.allowCodeReviewComments cannot be false",
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
            sdk_value_to_dict(request.signal.payload),
        )

    def _configure_builtin_review_policy(self) -> None:
        provider_module.configure(
            "github",
            {
                "appId": "12345",
                "appPrivateKey": "unused-in-tests",
                "workflow": {"provider": "local"},
                "webhookPolicies": [
                    {
                        "id": "pr-review",
                        "match": {
                            "events": ["pull_request"],
                            "actions": ["synchronize"],
                        },
                        "workflow": {
                            "target": {
                                "plugin": {
                                    "plugin": "github",
                                    "operation": "reviewPullRequest",
                                }
                            }
                        },
                    }
                ],
            },
        )

    def _review_pull_request_payload(self) -> dict[str, Any]:
        return {
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

    def _manual_review_comment_payload(self) -> dict[str, Any]:
        return {
            "action": "created",
            "installation": {"id": 99},
            "repository": {
                "full_name": "acme/widgets",
                "name": "widgets",
                "owner": {"login": "acme"},
            },
            "issue": {
                "number": 7,
                "title": "Fix widgets",
                "state": "open",
                "html_url": "https://github.com/acme/widgets/issues/7",
                "pull_request": {"html_url": "https://github.com/acme/widgets/pull/7"},
            },
            "comment": {"id": 10, "body": "gestalt review"},
            "headers": {
                "X-GitHub-Event": "issue_comment",
                "X-GitHub-Delivery": "delivery-manual-review",
            },
            "sender": {"login": "octocat"},
        }

    def _action_preferences_config(
        self,
        *,
        match: dict[str, Any] | None = None,
        action: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return {
            "appId": "12345",
            "appPrivateKey": "unused-in-tests",
            "workflow": {"provider": "local"},
            "agent": {"provider": "simple", "model": "deep"},
            "actionPreferences": {
                "indexeddb": "github_prefs",
                "store": "github_action_preferences",
                "failureMode": "config_default",
            },
            "webhookPolicies": [
                {
                    "id": "review-policy",
                    "match": match or {"events": ["pull_request"]},
                    "action": action or {"mode": "comment"},
                }
            ],
        }

    def _preference_pr_payload(self) -> dict[str, Any]:
        return {
            "action": "opened",
            "installation": {"id": 99},
            "repository": {"full_name": "acme/widgets"},
            "pull_request": {
                "number": 7,
                "user": {"id": 101, "login": "ada"},
                "head": {"ref": "feature", "sha": "abc123"},
                "base": {"ref": "main", "sha": "def456"},
            },
            "headers": {"X-GitHub-Event": "pull_request"},
            "sender": {"id": 202, "login": "octocat"},
        }

    def _same_repo_pull_request(self) -> dict[str, Any]:
        return {
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
            "maintainer_can_modify": True,
            "user": {"id": 101, "login": "ada"},
        }

    def _pr_issue_comment_payload(self) -> dict[str, Any]:
        return {
            "action": "created",
            "installation": {"id": 99},
            "repository": {
                "full_name": "acme/widgets",
                "name": "widgets",
                "owner": {"login": "acme"},
            },
            "issue": {
                "number": 7,
                "title": "Fix widgets",
                "state": "open",
                "html_url": "https://github.com/acme/widgets/issues/7",
                "pull_request": {"html_url": "https://github.com/acme/widgets/pull/7"},
            },
            "comment": {
                "id": 123,
                "body": "@valon-gestalt fix this",
                "user": {"id": 303, "login": "reviewer"},
            },
            "headers": {"X-GitHub-Event": "issue_comment"},
            "sender": {"id": 202, "login": "octocat"},
        }

    def _plain_issue_comment_payload(self) -> dict[str, Any]:
        return {
            "action": "created",
            "installation": {"id": 99},
            "repository": {
                "full_name": "acme/widgets",
                "name": "widgets",
                "owner": {"login": "acme"},
            },
            "issue": {
                "number": 8,
                "title": "Broken widget",
                "state": "open",
                "html_url": "https://github.com/acme/widgets/issues/8",
            },
            "comment": {
                "id": 124,
                "body": "@valon-gestalt fix this",
                "user": {"id": 303, "login": "reviewer"},
            },
            "headers": {"X-GitHub-Event": "issue_comment"},
            "sender": {"id": 202, "login": "octocat"},
        }

    def _full_pull_request_payload(self) -> dict[str, Any]:
        return {
            "action": "synchronize",
            "installation": {"id": 99},
            "repository": {
                "full_name": "acme/widgets",
                "name": "widgets",
                "owner": {"login": "acme"},
            },
            "pull_request": self._same_repo_pull_request(),
            "headers": {"X-GitHub-Event": "pull_request"},
            "sender": {"id": 202, "login": "octocat"},
        }

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
            sdk_value_to_dict(issue_comment_request.signal.payload),
        )
        self.assertEqual(issue_comment["github_event"], "issue_comment")
        self.assertNotIn("payload", issue_comment)
        self.assertNotIn("_gestalt_payload_preview_json", json.dumps(issue_comment))
        self.assertNotIn("comment_body", issue_comment["summary"])
        self.assertNotIn(
            "please update this workflow", json.dumps(issue_comment["summary"])
        )
        issue_metadata = sdk_value_to_dict(
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
            sdk_value_to_dict(review_request.signal.payload),
        )
        self.assertEqual(review["github_event"], "pull_request_review")
        self.assertNotIn("review_body", review["summary"])
        self.assertNotIn("please update this workflow", json.dumps(review["summary"]))
        review_metadata = sdk_value_to_dict(review_request.signal.metadata)
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
                    installation_id=99,
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
                    installation_id=99,
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

    def test_user_create_pull_request_uses_user_token_and_noreply_author(
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

            if path == "/user":
                self.assertEqual(method, "GET")
                self.assertEqual(auth_header(request), "Bearer user-token")
                return FakeHTTPResponse(
                    {"id": 222, "login": "ghopper", "name": "Grace Hopper"}
                )
            if path == "/app":
                self.assertEqual(method, "GET")
                self.assertEqual(auth_header(request), "Bearer app-jwt")
                return FakeHTTPResponse(
                    {"name": "Example App Bot", "slug": "example-app"}
                )
            if path == "/users/example-app%5Bbot%5D":
                self.assertEqual(method, "GET")
                self.assertEqual(auth_header(request), "")
                return FakeHTTPResponse({"id": 12345678, "login": "example-app[bot]"})
            if "/access_tokens" in path:
                self.fail(
                    f"user-auth operation must not mint installation token {path}"
                )
            if path == "/repos/acme/widgets/git/ref/heads/feature":
                self.assertEqual(auth_header(request), "Bearer user-token")
                raise http_error(request.full_url, HTTPStatus.NOT_FOUND)
            if path == "/repos/acme/widgets/git/ref/heads/main":
                self.assertEqual(auth_header(request), "Bearer user-token")
                return FakeHTTPResponse({"object": {"sha": "base-commit"}})
            if path == "/repos/acme/widgets/git/commits/base-commit":
                self.assertEqual(auth_header(request), "Bearer user-token")
                return FakeHTTPResponse({"tree": {"sha": "base-tree"}})
            if path == "/repos/acme/widgets/git/trees":
                self.assertEqual(auth_header(request), "Bearer user-token")
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
                self.assertEqual(auth_header(request), "Bearer user-token")
                self.assertEqual(
                    body["author"],
                    {
                        "name": "Grace Hopper",
                        "email": "222+ghopper@users.noreply.github.com",
                    },
                )
                self.assertEqual(body["author"], body["committer"])
                self.assertIn(
                    "Co-authored-by: Example App Bot "
                    "<12345678+example-app[bot]@users.noreply.github.com>",
                    body["message"],
                )
                return FakeHTTPResponse({"sha": "new-commit"})
            if path == "/repos/acme/widgets/git/refs":
                self.assertEqual(auth_header(request), "Bearer user-token")
                self.assertEqual(
                    body, {"ref": "refs/heads/feature", "sha": "new-commit"}
                )
                return FakeHTTPResponse({})
            if path == "/repos/acme/widgets/pulls":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer user-token")
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
            result = provider_module.user_create_pull_request(
                provider_module.UserCreatePullRequestInput(
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
                gestalt.Request(
                    token="user-token",
                    agent_external_identity=gestalt.ExternalIdentity(
                        type=provider_module.GITHUB_EXTERNAL_IDENTITY_TYPE,
                        id="user:999",
                    ),
                ),
            )

        data = cast(dict[str, Any], result)["data"]
        self.assertEqual(data["commit"]["sha"], "new-commit")
        self.assertEqual(data["commit"]["installation_id"], 0)
        self.assertEqual(data["pull_request"]["number"], 42)
        self.assertFalse(any("/access_tokens" in call[1] for call in calls))

    def test_user_create_pull_request_uses_public_email_when_available(self) -> None:
        commit_payloads: list[dict[str, Any]] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            method = request.get_method()
            path = request_path(request)
            body = request_json(request)

            if path == "/user":
                return FakeHTTPResponse(
                    {
                        "id": 222,
                        "login": "ghopper",
                        "name": "Grace Hopper",
                        "email": "grace@example.com",
                    }
                )
            if path == "/app":
                self.fail(
                    "bot identity should not be loaded when bot coauthor is disabled"
                )
            if "/access_tokens" in path:
                self.fail(
                    f"user-auth operation must not mint installation token {path}"
                )
            if path == "/repos/acme/widgets/git/ref/heads/feature":
                raise http_error(request.full_url, HTTPStatus.NOT_FOUND)
            if path == "/repos/acme/widgets/git/ref/heads/main":
                return FakeHTTPResponse({"object": {"sha": "base-commit"}})
            if path == "/repos/acme/widgets/git/commits/base-commit":
                return FakeHTTPResponse({"tree": {"sha": "base-tree"}})
            if path == "/repos/acme/widgets/git/trees":
                return FakeHTTPResponse({"sha": "new-tree"})
            if path == "/repos/acme/widgets/git/commits":
                commit_payloads.append(body)
                return FakeHTTPResponse({"sha": "new-commit"})
            if path == "/repos/acme/widgets/git/refs":
                return FakeHTTPResponse({})
            if path == "/repos/acme/widgets/pulls":
                self.assertEqual(method, "POST")
                self.assertEqual(auth_header(request), "Bearer user-token")
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

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            result = provider_module.user_create_pull_request(
                provider_module.UserCreatePullRequestInput(
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
                    include_bot_coauthor=False,
                ),
                gestalt.Request(token="user-token"),
            )

        self.assertIsInstance(result, dict)
        self.assertEqual(
            commit_payloads[0]["author"],
            {"name": "Grace Hopper", "email": "grace@example.com"},
        )
        self.assertEqual(commit_payloads[0]["author"], commit_payloads[0]["committer"])

    def test_user_create_pull_request_rejects_missing_token(self) -> None:
        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.user_create_pull_request(
                provider_module.UserCreatePullRequestInput(
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
                gestalt.Request(),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(response.body["error"], "token is required")
        urlopen.assert_not_called()

    def test_user_create_pull_request_rejects_blank_title_before_writes(self) -> None:
        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.user_create_pull_request(
                provider_module.UserCreatePullRequestInput(
                    owner="acme",
                    repo="widgets",
                    title="  ",
                    message="Update README",
                    files=[
                        provider_module.FileChangeInput(
                            path="README.md", content="hello"
                        )
                    ],
                    branch="feature",
                    base="main",
                ),
                gestalt.Request(token="user-token"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(response.body["error"], "title is required")
        urlopen.assert_not_called()

    def test_user_create_pull_request_rejects_missing_github_login_before_writes(
        self,
    ) -> None:
        calls: list[str] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            path = request_path(request)
            calls.append(path)
            if path == "/user":
                self.assertEqual(auth_header(request), "Bearer user-token")
                return FakeHTTPResponse({"id": 222, "name": "Grace Hopper"})
            self.fail(f"unexpected request {path}")

        with mock.patch(
            "internals.client.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            result = provider_module.user_create_pull_request(
                provider_module.UserCreatePullRequestInput(
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
                gestalt.Request(token="user-token"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_GATEWAY)
        self.assertIn("login", response.body["error"])
        self.assertEqual(calls, ["/user"])

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

    def test_resolve_installation_returns_external_identity_and_legacy_subject(
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
            data["legacy_subject"],
            "service_account:github_app_installation:99:repo:acme/widgets",
        )
        self.assertEqual(
            calls,
            [
                ("GET", "/repos/acme/widgets/installation", "Bearer app-jwt"),
                ("POST", "/app/installations/99/access_tokens", "Bearer app-jwt"),
            ],
        )

    def test_bot_operations_prefer_external_identity_over_legacy_subject(self) -> None:
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
                    installation_id=99,
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
        calls: list[str] = []

        def fake_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            path = request_path(request)
            calls.append(path)
            if path == "/app/installations/99/access_tokens":
                self.assertEqual(request.get_method(), "POST")
                return FakeHTTPResponse({"token": "issue-token"})
            if path == "/repos/acme/widgets/issues/7/comments":
                self.assertEqual(request.get_method(), "POST")
                self.assertEqual(auth_header(request), "Bearer issue-token")
                return FakeHTTPResponse(
                    {
                        "id": 11,
                        "body": "Looks good.",
                        "html_url": "https://github.example/acme/widgets/issues/7#issuecomment-11",
                    }
                )
            self.fail(f"unexpected request {request.get_method()} {path}")

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
                    body="Looks good.",
                    installation_id=99,
                ),
                GitHubExternalIdentityRequest(
                    external_identity={"type": "", "id": ""},
                    subject_installation_id=99,
                    subject_repo="acme/widgets",
                ),
            )

        self.assertIsInstance(result, dict)
        self.assertEqual(
            calls,
            [
                "/app/installations/99/access_tokens",
                "/repos/acme/widgets/issues/7/comments",
            ],
        )

    def test_external_identity_invalid_mismatch_and_unresolvable_fail_closed(
        self,
    ) -> None:
        valid_legacy_request = github_external_identity_request(
            subject_installation_id=99, subject_repo="acme/widgets"
        )

        with mock.patch("internals.client.urllib.request.urlopen") as urlopen:
            result = provider_module.bot_create_issue_comment(
                provider_module.CreateIssueCommentInput(
                    owner="acme",
                    repo="widgets",
                    issue_number=7,
                    body="Looks broken.",
                    installation_id=99,
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
                    installation_id=99,
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
                    installation_id=99,
                ),
                valid_legacy_request,
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertIn("could not be resolved", response.body["error"])
        self.assertEqual(calls, ["/repos/acme/widgets/installation"])

        calls = []

        def explicit_mismatch_urlopen(
            request: urllib.request.Request, timeout: float = 30
        ) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            calls.append(request_path(request))
            if request_path(request) == "/repos/acme/widgets/installation":
                return FakeHTTPResponse({"id": 99})
            self.fail(
                f"unexpected request {request.get_method()} {request_path(request)}"
            )

        with (
            mock.patch("internals.client.create_app_jwt", return_value="app-jwt"),
            mock.patch(
                "internals.client.urllib.request.urlopen",
                side_effect=explicit_mismatch_urlopen,
            ),
        ):
            result = provider_module.bot_open_pull_request(
                provider_module.OpenPullRequestInput(
                    owner="acme",
                    repo="widgets",
                    title="Update README",
                    head="feature",
                    base="main",
                    installation_id=100,
                ),
                valid_legacy_request,
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertIn("installation_id", response.body["error"])
        self.assertEqual(calls, ["/repos/acme/widgets/installation"])

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
