from __future__ import annotations

import unittest
from types import SimpleNamespace
from typing import Any
from unittest import mock

import gestalt

from internals import cache_ingest
from internals.config import GitHubBotIdentity, configure_from_mapping
from internals.constants import DEFAULT_WEBHOOK_EVENTS
from internals.webhook import github_event_type
import provider as provider_module


class FakeWorkflows:
    def __init__(self) -> None:
        self.requests: list[Any] = []

    def __enter__(self) -> FakeWorkflows:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def deliver_event(self, request: Any) -> None:
        self.requests.append(request)


class CacheIngestTests(unittest.TestCase):
    def setUp(self) -> None:
        configure_from_mapping(
            {
                "appId": "123",
                "cacheEnabled": True,
                "workflow": {"provider": "local"},
            }
        )
        self.addCleanup(configure_from_mapping, {})

    def test_projects_each_supported_event_family(self) -> None:
        cases = [
            ("pull_request", "pull_request", {"id": 1, "updated_at": "2026-01-01"}),
            (
                "pull_request_review",
                "review",
                {"id": 2, "submitted_at": "2026-01-01"},
            ),
            (
                "pull_request_review_comment",
                "comment",
                {"id": 3, "updated_at": "2026-01-01"},
            ),
            ("check_run", "check_run", {"id": 4, "completed_at": "2026-01-01"}),
            (
                "check_suite",
                "check_suite",
                {"id": 5, "updated_at": "2026-01-01"},
            ),
            (
                "workflow_run",
                "workflow_run",
                {"id": 6, "updated_at": "2026-01-01"},
            ),
            ("issues", "issue", {"id": 7, "updated_at": "2026-01-01"}),
            (
                "issue_comment",
                "comment",
                {"id": 8, "updated_at": "2026-01-01"},
            ),
            (
                "deployment",
                "deployment",
                {"id": 9, "updated_at": "2026-01-01"},
            ),
            (
                "deployment_status",
                "deployment_status",
                {"id": 10, "created_at": "2026-01-01"},
            ),
        ]

        for event_type, object_key, entity in cases:
            payload = {
                "action": "created",
                object_key: entity,
                "deployment": {"id": 9},
            }
            summary = {"repository": "acme/widgets", "installation_id": 99}
            with (
                self.subTest(event_type=event_type),
                mock.patch.object(
                    cache_ingest.cache_store,
                    "put_entity_and_increment",
                    return_value=(True, {}),
                ) as put,
            ):
                self.assertTrue(
                    cache_ingest.ingest_webhook_event(
                        event_type, payload, summary
                    )
                )
                put.assert_called_once()
                self.assertEqual(put.call_args.args[1], "acme/widgets")

    def test_deleted_action_writes_tombstone(self) -> None:
        with mock.patch.object(
            cache_ingest.cache_store,
            "put_entity_and_increment",
            return_value=(True, {}),
        ) as put:
            cache_ingest.ingest_webhook_event(
                "issue_comment",
                {
                    "action": "deleted",
                    "comment": {"id": 8, "updated_at": "2026-01-01"},
                },
                {"repository": "acme/widgets", "installation_id": 99},
            )

        self.assertTrue(put.call_args.kwargs["deleted"])

    def test_disabled_cache_does_not_touch_storage(self) -> None:
        configure_from_mapping({"cacheEnabled": False})
        with mock.patch.object(
            cache_ingest.cache_store, "put_entity_and_increment"
        ) as put:
            ingested = cache_ingest.ingest_webhook_event(
                "pull_request",
                {"pull_request": {"id": 1}},
                {"repository": "acme/widgets", "installation_id": 99},
            )

        self.assertFalse(ingested)
        put.assert_not_called()

    def test_bot_sender_is_ingested_before_delivery_is_suppressed(self) -> None:
        payload = {
            "action": "opened",
            "installation": {"id": 99},
            "repository": {"full_name": "acme/widgets"},
            "pull_request": {"id": 1, "number": 7},
            "sender": {"login": "example-app[bot]"},
        }
        identity = GitHubBotIdentity(
            name="Example",
            login="example-app[bot]",
            user_id="1",
            email="example@example.com",
        )
        with (
            mock.patch.object(
                provider_module, "ingest_webhook_event", return_value=True
            ) as ingest,
            mock.patch("internals.webhook.bot_identity", return_value=identity),
            mock.patch.object(
                gestalt.Request,
                "workflows",
                side_effect=AssertionError("workflow delivery must be suppressed"),
                create=True,
            ),
        ):
            result = provider_module.github_events_handle(payload, gestalt.Request())

        self.assertEqual(result, {"ok": True, "ignored": "configured_bot_sender"})
        ingest.assert_called_once()

    def test_ingest_failure_does_not_block_workflow_delivery(self) -> None:
        payload = {
            "action": "opened",
            "installation": {"id": 99},
            "repository": {"full_name": "acme/widgets"},
            "pull_request": {"id": 1, "number": 7},
            "sender": {"login": "octocat"},
        }
        workflows = FakeWorkflows()
        workflow_request = SimpleNamespace(
            provider="local",
            event=SimpleNamespace(id="github:delivery"),
        )
        with (
            mock.patch.object(
                provider_module,
                "ingest_webhook_event",
                side_effect=RuntimeError("cache unavailable"),
            ),
            mock.patch.object(
                provider_module,
                "_build_workflow_deliver_event_request",
                return_value=workflow_request,
            ),
            mock.patch.object(
                gestalt.Request,
                "workflows",
                return_value=workflows,
                create=True,
            ),
        ):
            result = provider_module.github_events_handle(payload, gestalt.Request())

        self.assertTrue(result["delivered"])
        self.assertEqual(workflows.requests, [workflow_request])

    def test_deployment_events_are_defaulted_and_structurally_discriminated(
        self,
    ) -> None:
        self.assertIn("deployment", DEFAULT_WEBHOOK_EVENTS)
        self.assertIn("deployment_status", DEFAULT_WEBHOOK_EVENTS)
        self.assertEqual(
            github_event_type(
                {
                    "deployment_status": {"id": 2},
                    "deployment": {"id": 1},
                    "repository": {},
                    "action": "created",
                }
            ),
            "deployment_status",
        )
        self.assertEqual(
            github_event_type(
                {
                    "deployment": {"id": 1},
                    "repository": {},
                    "action": "created",
                }
            ),
            "deployment",
        )


if __name__ == "__main__":
    unittest.main()
