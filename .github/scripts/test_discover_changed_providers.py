"""Unit tests for discover_changed_providers.compute_payload.

These tests run against the actual repository checkout (the script only reads
manifest and go.mod files), and inject synthetic changed-file lists so we can
assert exactly what would be selected for representative scenarios.

Run with: ``python3 -m unittest .github/scripts/test_discover_changed_providers.py``
"""

from __future__ import annotations

import unittest
from pathlib import Path

import discover_changed_providers as discover

REPO = Path(__file__).resolve().parents[2]


def names(payload: dict) -> set[str]:
    return {p["name"] for p in payload["plugins"]}


class ComputePayloadTests(unittest.TestCase):
    def call(self, files: list[str]) -> dict:
        return discover.compute_payload(REPO, "pull_request", files)

    # ----- baseline / no-op -----

    def test_no_changes_runs_nothing(self) -> None:
        payload = self.call([])
        self.assertEqual(payload["count"], 0)
        self.assertEqual(payload["scope"], "changed")
        self.assertEqual(payload["run_validate"], "false")
        self.assertEqual(payload["run_ui_lint"], "false")
        self.assertEqual(payload["run_ui_tests"], "false")
        self.assertEqual(payload["run_ui_e2e"], "false")

    def test_top_level_doc_change_is_noop(self) -> None:
        payload = self.call(["README.md", "LICENSE"])
        self.assertEqual(payload["count"], 0)
        self.assertEqual(payload["run_ui_e2e"], "false")

    # ----- plugin-dir-only changes -----

    def test_python_plugin_isolated(self) -> None:
        payload = self.call(["plugins/slack/main.py"])
        self.assertEqual(names(payload), {"plugins/slack"})
        self.assertEqual(payload["run_ui_lint"], "false")
        self.assertEqual(payload["run_ui_e2e"], "false")

    def test_runtime_python_plugin_isolated(self) -> None:
        payload = self.call(["runtime/modal/foo.go"])
        self.assertEqual(names(payload), {"runtime/modal"})
        self.assertEqual(payload["run_ui_e2e"], "false")

    def test_s3_plugin_isolated(self) -> None:
        payload = self.call(["s3/s3/foo.go"])
        self.assertEqual(names(payload), {"s3/s3"})
        self.assertEqual(payload["run_ui_e2e"], "false")

    # ----- self-contained per-provider modules (post Phase 3a) -----
    # After the multi-module root split, each provider has its own go.mod
    # and no shared internal/ packages. A change to a single provider must
    # never widen to its sibling providers.

    def test_auth_oidc_change_isolated(self) -> None:
        payload = self.call(["auth/oidc/provider.go"])
        self.assertEqual(names(payload), {"auth/oidc"})

    def test_secrets_aws_change_isolated(self) -> None:
        payload = self.call(["secrets/aws/provider.go"])
        self.assertEqual(names(payload), {"secrets/aws"})

    def test_secrets_vault_change_isolated(self) -> None:
        payload = self.call(["secrets/vault/provider.go"])
        self.assertEqual(names(payload), {"secrets/vault"})

    def test_cache_valkey_change_isolated(self) -> None:
        payload = self.call(["cache/valkey/provider.go"])
        self.assertEqual(names(payload), {"cache/valkey"})

    def test_authorization_indexeddb_change_isolated(self) -> None:
        payload = self.call(["authorization/indexeddb/provider.go"])
        self.assertEqual(names(payload), {"authorization/indexeddb"})

    # ----- indexeddb providers (post Phase 3b) -----
    # indexeddb/internal and indexeddb/contracttest moved to the gestalt SDK.
    # Each indexeddb backend is now self-contained.

    def test_indexeddb_relationaldb_still_widens_to_test_consumers(self) -> None:
        # workflow/indexeddb still imports indexeddb/relationaldb in its
        # *test* code, and external_credentials/default depends on it via the
        # in-repo replace directive. The integration test target also
        # depends on relationaldb. A change to relationaldb runs all of these.
        payload = self.call(["indexeddb/relationaldb/foo.go"])
        expected = {
            "indexeddb/relationaldb",
            "external_credentials/default",
            "workflow/indexeddb",
        }
        self.assertEqual(names(payload), expected)
        self.assertEqual(payload["run_ui_e2e"], "true")
        self.assertEqual(payload["run_integration"], "true")

    def test_integration_change_runs_integration_only(self) -> None:
        payload = self.call(["integration/external_credentials_default_test.go"])
        self.assertEqual(payload["count"], 0)
        self.assertEqual(payload["run_integration"], "true")
        self.assertEqual(payload["run_ui_lint"], "false")

    def test_indexeddb_mongodb_isolated(self) -> None:
        payload = self.call(["indexeddb/mongodb/foo.go"])
        self.assertEqual(names(payload), {"indexeddb/mongodb"})
        self.assertEqual(payload["run_ui_e2e"], "false")

    def test_indexeddb_dynamodb_isolated(self) -> None:
        payload = self.call(["indexeddb/dynamodb/foo.go"])
        self.assertEqual(names(payload), {"indexeddb/dynamodb"})
        self.assertEqual(payload["run_ui_e2e"], "false")

    def test_external_credentials_default_triggers_ui_e2e(self) -> None:
        payload = self.call(["external_credentials/default/foo.go"])
        self.assertEqual(names(payload), {"external_credentials/default"})
        self.assertEqual(payload["run_ui_e2e"], "true")

    # ----- UI -----

    def test_ui_default_change_runs_all_ui_jobs(self) -> None:
        payload = self.call(["ui/default/src/foo.tsx"])
        self.assertEqual(names(payload), {"ui/default"})
        self.assertEqual(payload["run_ui_lint"], "true")
        self.assertEqual(payload["run_ui_tests"], "true")
        self.assertEqual(payload["run_ui_e2e"], "true")

    # ----- workflow & script changes -----

    def test_release_workflow_change_is_noop(self) -> None:
        payload = self.call([".github/workflows/release-plugin.yml"])
        self.assertEqual(payload["count"], 0)
        self.assertEqual(payload["scope"], "changed")
        self.assertEqual(payload["run_ui_e2e"], "false")

    def test_release_only_script_change_is_noop(self) -> None:
        payload = self.call([".github/scripts/package_python_docker.sh"])
        self.assertEqual(payload["count"], 0)
        self.assertEqual(payload["run_ui_e2e"], "false")

    def test_ci_yml_change_runs_everything(self) -> None:
        payload = self.call([".github/workflows/ci.yml"])
        self.assertEqual(payload["scope"], "all")
        self.assertGreater(payload["count"], 10)
        self.assertEqual(payload["run_ui_lint"], "true")
        self.assertEqual(payload["run_ui_e2e"], "true")

    def test_ci_referenced_script_change_runs_everything(self) -> None:
        payload = self.call([".github/scripts/setup_test_env.sh"])
        self.assertEqual(payload["scope"], "all")
        self.assertGreater(payload["count"], 10)

    def test_dependabot_change_is_noop(self) -> None:
        payload = self.call([".github/dependabot.yml"])
        self.assertEqual(payload["count"], 0)

    # ----- mixed -----

    def test_mixed_python_and_ui_change(self) -> None:
        payload = self.call(["plugins/slack/main.py", "ui/default/src/foo.tsx"])
        self.assertEqual(names(payload), {"plugins/slack", "ui/default"})
        self.assertEqual(payload["run_ui_lint"], "true")
        self.assertEqual(payload["run_ui_e2e"], "true")

    def test_push_event_runs_everything(self) -> None:
        payload = discover.compute_payload(REPO, "push", None)
        self.assertEqual(payload["scope"], "all")
        self.assertGreater(payload["count"], 10)
        self.assertEqual(payload["run_ui_lint"], "true")
        self.assertEqual(payload["run_ui_tests"], "true")
        self.assertEqual(payload["run_ui_e2e"], "true")

    # ----- catch-all guards -----

    def test_unknown_top_level_path_is_noop(self) -> None:
        payload = self.call(["docs/architecture.md"])
        self.assertEqual(payload["count"], 0)
        self.assertEqual(payload["run_ui_e2e"], "false")

    def test_per_plugin_python_change_does_not_widen(self) -> None:
        # A change in one Python plugin must not pull in any other plugin or UI.
        payload = self.call(["plugins/slack/some/nested/dir/file.py"])
        self.assertEqual(names(payload), {"plugins/slack"})
        self.assertEqual(payload["run_ui_e2e"], "false")

    # ----- needs_sibling_gestalt flag (runtime holdouts) -----

    def test_runtime_modal_needs_sibling_gestalt(self) -> None:
        payload = self.call(["runtime/modal/foo.go"])
        plugin = next(p for p in payload["plugins"] if p["name"] == "runtime/modal")
        self.assertTrue(plugin["needs_sibling_gestalt"])

    def test_runtime_nebius_needs_sibling_gestalt(self) -> None:
        payload = self.call(["runtime/nebius/foo.go"])
        plugin = next(p for p in payload["plugins"] if p["name"] == "runtime/nebius")
        self.assertTrue(plugin["needs_sibling_gestalt"])

    def test_runtime_gkeagentsandbox_needs_sibling_gestalt(self) -> None:
        payload = self.call(["runtime/gkeagentsandbox/foo.go"])
        plugin = next(p for p in payload["plugins"] if p["name"] == "runtime/gkeagentsandbox")
        self.assertTrue(plugin["needs_sibling_gestalt"])

    def test_migrated_provider_does_not_need_sibling_gestalt(self) -> None:
        payload = self.call(["indexeddb/relationaldb/foo.go"])
        plugin = next(p for p in payload["plugins"] if p["name"] == "indexeddb/relationaldb")
        self.assertFalse(plugin["needs_sibling_gestalt"])


if __name__ == "__main__":
    unittest.main()
