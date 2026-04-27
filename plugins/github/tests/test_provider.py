from __future__ import annotations

import io
import json
import unittest
import urllib.error
import urllib.parse
import urllib.request
from email.message import Message
from http import HTTPStatus
from typing import Any, cast
from unittest import mock

import gestalt
from gestalt.gen.v1 import agent_pb2 as _agent_pb2

import provider as provider_module

agent_pb2: Any = _agent_pb2


class FakeHTTPResponse:
    def __init__(self, body: dict[str, Any] | None = None) -> None:
        self._body = json.dumps(body or {}).encode("utf-8")

    def __enter__(self) -> FakeHTTPResponse:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


class FakeAgentManager:
    def __init__(self) -> None:
        self.requests: list[Any] = []

    def __enter__(self) -> FakeAgentManager:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def run(self, request: Any) -> Any:
        self.requests.append(request)
        return agent_pb2.ManagedAgentRun(
            provider_name=request.provider_name or "simple",
            run=agent_pb2.BoundAgentRun(
                id="run-123", status=agent_pb2.AGENT_RUN_STATUS_RUNNING
            ),
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


def github_request(installation_id: int = 99, repo: str = "acme/widgets") -> gestalt.Request:
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
                "botName": "Example App Bot",
                "botEmail": "12345678+example-app[bot]@users.noreply.github.com",
                "agentProvider": "simple",
                "agentModel": "deep",
            },
        )
        self.addCleanup(provider_module.configure, "github", {})

    def test_resolve_http_subject_maps_installation_to_workload(self) -> None:
        subject = provider_module.resolve_http_subject(
            gestalt.HTTPSubjectRequest(
                params={
                    "installation": {"id": 99},
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

    def test_webhook_handler_starts_agent_with_bot_tools(self) -> None:
        agent_manager = FakeAgentManager()
        payload = {
            "action": "opened",
            "installation": {"id": 99},
            "repository": {
                "full_name": "acme/widgets",
                "name": "widgets",
                "owner": {"login": "acme"},
            },
            "pull_request": {
                "number": 7,
                "head": {"ref": "feature"},
                "base": {"ref": "main"},
            },
            "sender": {"login": "octocat"},
        }

        with mock.patch.object(
            gestalt.Request, "agent_manager", return_value=agent_manager
        ):
            result = provider_module.github_events_handle(payload, gestalt.Request())

        self.assertEqual(result["ok"], True)
        self.assertEqual(result["agent_run_id"], "run-123")
        self.assertEqual(len(agent_manager.requests), 1)

        request = agent_manager.requests[0]
        self.assertEqual(request.provider_name, "simple")
        self.assertEqual(request.model, "deep")
        self.assertEqual(
            request.tool_source, agent_pb2.AGENT_TOOL_SOURCE_MODE_EXPLICIT
        )
        self.assertEqual(
            [tool.operation for tool in request.tool_refs],
            [
                provider_module.BOT_COMMIT_FILES_OPERATION,
                provider_module.BOT_OPEN_PULL_REQUEST_OPERATION,
                provider_module.BOT_CREATE_PULL_REQUEST_OPERATION,
            ],
        )
        self.assertIn("github:99:acme/widgets:7", request.session_ref)

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
                self.assertIn(
                    "Co-authored-by: Ada <ada@example.com>", body["message"]
                )
                self.assertIn(
                    "Co-authored-by: Example App Bot <12345678+example-app[bot]@users.noreply.github.com>",
                    body["message"],
                )
                return FakeHTTPResponse({"sha": "new-commit"})
            if path == "/repos/acme/widgets/git/refs":
                self.assertEqual(body, {"ref": "refs/heads/feature", "sha": "new-commit"})
                return FakeHTTPResponse({})
            self.fail(f"unexpected request {method} {path}")

        with (
            mock.patch("provider._create_app_jwt", return_value="app-jwt"),
            mock.patch("provider.urllib.request.urlopen", side_effect=fake_urlopen),
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
            mock.patch("provider._create_app_jwt", return_value="app-jwt"),
            mock.patch("provider.urllib.request.urlopen", side_effect=fake_urlopen),
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
        with mock.patch("provider.urllib.request.urlopen") as urlopen:
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

        with mock.patch("provider.urllib.request.urlopen") as urlopen:
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
        with mock.patch("provider.urllib.request.urlopen") as urlopen:
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

        with mock.patch("provider.urllib.request.urlopen") as urlopen:
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

    def test_webhook_handler_filters_unsupported_and_configured_bot_events(self) -> None:
        agent_manager = FakeAgentManager()
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

        with mock.patch.object(
            gestalt.Request, "agent_manager", return_value=agent_manager
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
        self.assertEqual(agent_manager.requests, [])


if __name__ == "__main__":
    unittest.main()
