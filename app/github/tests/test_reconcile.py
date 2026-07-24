from __future__ import annotations

import pathlib
import unittest
from types import SimpleNamespace
from typing import Any
from unittest import mock

import gestalt
import yaml

from internals import cache_store, reconcile
from internals.config import configure_from_mapping
from internals.errors import GitHubAPIError


class AllowedAuthorization:
    def check_access(self, _request: Any) -> Any:
        return SimpleNamespace(allowed=True)


class ReconcileTests(unittest.TestCase):
    def setUp(self) -> None:
        configure_from_mapping(
            {"appId": "123", "cacheEnabled": True},
            provider_name="github",
        )
        self.addCleanup(configure_from_mapping, {})
        self.client = mock.Mock()
        self.client.repository_installation_id.return_value = 99
        self.client.installation_token.return_value = "token"
        self.client.github_json.return_value = {"number": 7, "state": "closed"}
        self.subject = gestalt.Subject(id="service_account:test")
        self.authorization = AllowedAuthorization()
        self.record = cache_store.CachedResponse(
            id="response-id",
            operation="bot.getPullRequest",
            repository="acme/widgets",
            domain="pull_request",
            request={
                "kind": "rest",
                "method": "GET",
                "path": "/repos/acme/widgets/pulls/7",
                "response_kind": "object",
                "permissions": {"pull_requests": "read"},
            },
            body={"number": 7, "state": "open"},
            generation=0,
            fetched_at=1,
            expires_at=2,
        )

    def test_reconcile_refreshes_drift_and_releases_lease(self) -> None:
        with (
            mock.patch.object(
                reconcile.cache_store,
                "claim_reconcile_lease",
                return_value=True,
            ),
            mock.patch.object(
                reconcile.cache_store,
                "list_expired_responses",
                return_value=[self.record],
            ),
            mock.patch.object(
                reconcile.cache_store, "get_generation", return_value=2
            ),
            mock.patch.object(
                reconcile.cache_store,
                "put_cached_response_if_generation",
                return_value=True,
            ) as put,
            mock.patch.object(
                reconcile.cache_store, "prune_responses", return_value=3
            ),
            mock.patch.object(
                reconcile.cache_store, "prune_entities", return_value=0
            ),
            mock.patch.object(
                reconcile.cache_store, "release_reconcile_lease", return_value=True
            ) as release,
        ):
            report = reconcile.reconcile_cache(
                "acme",
                "widgets",
                25,
                subject=self.subject,
                authorization=self.authorization,
                client=self.client,
            )

        self.assertEqual(report.checked, 1)
        self.assertEqual(report.drifted, 1)
        self.assertEqual(report.refreshed, 1)
        self.assertEqual(report.pruned, 3)
        put.assert_called_once()
        release.assert_called_once()

    def test_reconcile_removes_confirmed_not_found_response(self) -> None:
        self.client.github_json.side_effect = GitHubAPIError(404, "not found")
        with (
            mock.patch.object(
                reconcile.cache_store,
                "claim_reconcile_lease",
                return_value=True,
            ),
            mock.patch.object(
                reconcile.cache_store,
                "list_expired_responses",
                return_value=[self.record],
            ),
            mock.patch.object(
                reconcile.cache_store, "delete_cached_response"
            ) as delete,
            mock.patch.object(
                reconcile.cache_store, "prune_responses", return_value=0
            ),
            mock.patch.object(
                reconcile.cache_store, "prune_entities", return_value=0
            ),
            mock.patch.object(
                reconcile.cache_store, "release_reconcile_lease", return_value=True
            ),
        ):
            report = reconcile.reconcile_cache(
                "acme",
                "widgets",
                25,
                subject=self.subject,
                authorization=self.authorization,
                client=self.client,
            )

        self.assertEqual(report.deleted, 1)
        delete.assert_called_once_with("response-id")

    def test_invalidation_during_replay_prevents_stale_refresh(self) -> None:
        generation = 1

        def replay(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            nonlocal generation
            generation = 2
            return {"number": 7, "state": "closed"}

        self.client.github_json.side_effect = replay

        def conditional_put(*_args: Any, **kwargs: Any) -> bool:
            return kwargs["expected_generation"] == generation

        with (
            mock.patch.object(
                reconcile.cache_store,
                "claim_reconcile_lease",
                return_value=True,
            ),
            mock.patch.object(
                reconcile.cache_store,
                "list_expired_responses",
                return_value=[self.record],
            ),
            mock.patch.object(
                reconcile.cache_store,
                "get_generation",
                side_effect=lambda *_args: generation,
            ),
            mock.patch.object(
                reconcile.cache_store,
                "put_cached_response_if_generation",
                side_effect=conditional_put,
            ) as put,
            mock.patch.object(
                reconcile.cache_store, "prune_responses", return_value=0
            ),
            mock.patch.object(
                reconcile.cache_store, "prune_entities", return_value=0
            ),
            mock.patch.object(
                reconcile.cache_store, "release_reconcile_lease", return_value=True
            ),
        ):
            report = reconcile.reconcile_cache(
                "acme",
                "widgets",
                25,
                subject=self.subject,
                authorization=self.authorization,
                client=self.client,
            )

        self.assertEqual(report.refreshed, 0)
        self.assertEqual(put.call_args.kwargs["expected_generation"], 1)

    def test_reconcile_respects_lease_contention_and_request_cap(self) -> None:
        with mock.patch.object(
            reconcile.cache_store,
            "claim_reconcile_lease",
            return_value=False,
        ):
            report = reconcile.reconcile_cache(
                "acme",
                "widgets",
                1_000,
                subject=self.subject,
                authorization=self.authorization,
                client=self.client,
            )

        self.assertFalse(report.lease_acquired)
        self.client.github_json.assert_not_called()

    def test_disabled_reconcile_does_not_touch_storage(self) -> None:
        configure_from_mapping({"cacheEnabled": False})
        with mock.patch.object(
            reconcile.cache_store, "claim_reconcile_lease"
        ) as claim:
            report = reconcile.reconcile_cache(
                "acme",
                "widgets",
                25,
                subject=self.subject,
                authorization=self.authorization,
                client=self.client,
            )

        self.assertTrue(report.disabled)
        claim.assert_not_called()

    def test_catalog_declares_hidden_maintenance_operation(self) -> None:
        catalog_path = pathlib.Path(__file__).resolve().parents[1] / "catalog.yaml"
        catalog = yaml.safe_load(catalog_path.read_text())
        operations = {operation["id"]: operation for operation in catalog["operations"]}

        maintenance = operations["maintenance.reconcileCache"]
        self.assertFalse(maintenance["visible"])
        self.assertEqual(
            [parameter["name"] for parameter in maintenance["parameters"]],
            ["owner", "repo", "max_entries"],
        )


if __name__ == "__main__":
    unittest.main()
