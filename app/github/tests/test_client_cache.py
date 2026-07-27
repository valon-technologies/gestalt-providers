from __future__ import annotations

import unittest
from unittest import mock

from internals.client import (
    _record_rate_limit_headers,
    clear_installation_token_cache_for_tests,
    installation_token,
    latest_rate_limit_snapshot,
)
import provider as provider_module


class InstallationTokenCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        clear_installation_token_cache_for_tests()

    def tearDown(self) -> None:
        clear_installation_token_cache_for_tests()

    @mock.patch("internals.client.github_json")
    @mock.patch("internals.client.create_app_jwt", return_value="app-jwt")
    def test_reuses_cached_installation_token(
        self,
        _create_app_jwt: mock.MagicMock,
        github_json: mock.MagicMock,
    ) -> None:
        github_json.return_value = {
            "token": "installation-token",
            "expires_at": "2099-01-01T00:00:00Z",
        }

        first = installation_token(42)
        second = installation_token(42)

        self.assertEqual(first, "installation-token")
        self.assertEqual(second, "installation-token")
        self.assertEqual(github_json.call_count, 1)

    @mock.patch("internals.client.github_json")
    @mock.patch("internals.client.create_app_jwt", return_value="app-jwt")
    def test_cache_key_includes_repositories(
        self,
        _create_app_jwt: mock.MagicMock,
        github_json: mock.MagicMock,
    ) -> None:
        github_json.return_value = {
            "token": "installation-token",
            "expires_at": "2099-01-01T00:00:00Z",
        }

        installation_token(42, repositories=["acme/widgets"])
        installation_token(42, repositories=["acme/widgets"])
        installation_token(42, repositories=["acme/other"])

        self.assertEqual(github_json.call_count, 2)


class RateLimitSnapshotTests(unittest.TestCase):
    def test_record_rate_limit_headers_updates_snapshot(self) -> None:
        _record_rate_limit_headers(
            {
                "x-ratelimit-limit": "5000",
                "x-ratelimit-remaining": "4999",
                "x-ratelimit-reset": "1700000000",
            }
        )

        self.assertEqual(
            latest_rate_limit_snapshot(),
            {
                "x-ratelimit-limit": "5000",
                "x-ratelimit-remaining": "4999",
                "x-ratelimit-reset": "1700000000",
            },
        )

    def test_runtime_rate_limit_operation_returns_snapshot(self) -> None:
        _record_rate_limit_headers({"x-ratelimit-remaining": "1234"})
        result = provider_module.runtime_rate_limit({}, mock.Mock())
        self.assertEqual(result["rate_limit"]["x-ratelimit-remaining"], "1234")


if __name__ == "__main__":
    unittest.main()
