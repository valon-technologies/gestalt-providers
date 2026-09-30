from __future__ import annotations

import io
import unittest
import urllib.error
import urllib.request
from http import HTTPStatus
from http.client import HTTPMessage
from unittest import mock

import internals.client as client_module
from internals.client import (
    SlackAPIError,
    SlackClientError,
    _SlackFileRedirectHandler,
    get_bytes,
    is_slack_file_download_url,
    is_slack_file_upload_url,
)


class IsSlackFileDownloadUrlTests(unittest.TestCase):
    def test_allows_files_slack_com_exact_host(self) -> None:
        self.assertTrue(
            is_slack_file_download_url(
                "https://files.slack.com/files-pri/T0/F0/whatever"
            )
        )

    def test_allows_tenant_subdomain_of_slack_files_com(self) -> None:
        self.assertTrue(
            is_slack_file_download_url(
                "https://valon.slack-files.com/files-pri/T0/F0/whatever"
            )
        )

    def test_allows_bare_slack_files_com_apex(self) -> None:
        # Regression: Slack's `url_private` sometimes uses the bare apex; the
        # legacy suffix-only check missed this and surfaced as a provider 5xx.
        self.assertTrue(
            is_slack_file_download_url(
                "https://slack-files.com/files-pri/T0/F0/whatever"
            )
        )

    def test_allows_slack_edge_cdn_subdomain(self) -> None:
        # Regression: `files.slack.com` 302s into `*.slack-edge.com` for many
        # tenants, which the redirect handler must permit.
        self.assertTrue(
            is_slack_file_download_url("https://x.slack-edge.com/files/abc")
        )

    def test_allows_files_slack_edge_subdomain(self) -> None:
        self.assertTrue(
            is_slack_file_download_url("https://files.slack-edge.com/files/abc")
        )

    def test_rejects_http_scheme(self) -> None:
        self.assertFalse(
            is_slack_file_download_url(
                "http://files.slack.com/files-pri/T0/F0/whatever"
            )
        )

    def test_rejects_unrelated_host(self) -> None:
        self.assertFalse(
            is_slack_file_download_url(
                "https://attacker.com/files-pri/T0/F0/whatever"
            )
        )

    def test_rejects_naive_suffix_bypass_with_trailing_attacker_host(self) -> None:
        # Locks in the leading-dot suffix guard. If a future refactor drops
        # the dots, `files.slack.com.attacker.com` would falsely match.
        self.assertFalse(
            is_slack_file_download_url(
                "https://files.slack.com.attacker.com/files-pri/T0/F0/whatever"
            )
        )

    def test_rejects_naive_suffix_bypass_against_slack_edge(self) -> None:
        self.assertFalse(
            is_slack_file_download_url("https://evilslack-edge.com/files/abc")
        )

    def test_rejects_empty_string(self) -> None:
        self.assertFalse(is_slack_file_download_url(""))

    def test_rejects_url_without_hostname(self) -> None:
        self.assertFalse(is_slack_file_download_url("https://"))


class IsSlackFileUploadUrlTests(unittest.TestCase):
    def test_allows_files_slack_com_upload_path(self) -> None:
        self.assertTrue(
            is_slack_file_upload_url("https://files.slack.com/upload/v1/abc")
        )

    def test_rejects_slack_edge_for_uploads(self) -> None:
        # Uploads stay strict to `files.slack.com` + `/upload/v1/`; widening
        # download hosts must not weaken the upload guard.
        self.assertFalse(
            is_slack_file_upload_url("https://x.slack-edge.com/upload/v1/abc")
        )

    def test_rejects_files_slack_com_non_upload_path(self) -> None:
        self.assertFalse(
            is_slack_file_upload_url(
                "https://files.slack.com/files-pri/T0/F0/whatever"
            )
        )


class GetBytesRejectionContractTests(unittest.TestCase):
    def test_get_bytes_rejects_non_slack_url_with_bad_request(self) -> None:
        # Locks in the 5xx -> 4xx reclassification so the
        # gestaltd.operation.error_count{...5xx} monitor cannot regress on a
        # caller-supplied bad URL.
        with self.assertRaises(SlackAPIError) as raised:
            get_bytes("https://attacker.com/whatever", "test-token", 1024)
        self.assertEqual(raised.exception.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            raised.exception.body,
            {"error": "slack file download URL must be a Slack HTTPS file URL"},
        )

    def test_get_bytes_rejects_without_opening_a_connection(self) -> None:
        with mock.patch(
            "internals.client.urllib.request.build_opener"
        ) as build_opener:
            with self.assertRaises(SlackAPIError):
                get_bytes("https://attacker.com/whatever", "test-token", 1024)
        build_opener.assert_not_called()

    def test_get_bytes_does_not_raise_slack_client_error_on_bad_url(self) -> None:
        # Belt-and-suspenders guard against a future revert: `SlackClientError`
        # is not a `SlackAPIError`, so Gestalt maps it to 5xx — which is
        # exactly the regression we are preventing.
        try:
            get_bytes("https://attacker.com/whatever", "test-token", 1024)
        except SlackAPIError:
            pass
        except SlackClientError:
            self.fail(
                "get_bytes raised SlackClientError on a non-Slack URL; "
                "expected SlackAPIError(HTTPStatus.BAD_REQUEST, ...)"
            )


class SlackFileRedirectHandlerTests(unittest.TestCase):
    def _make_request(self) -> urllib.request.Request:
        return urllib.request.Request(
            "https://files.slack.com/files-pri/T0/F0/whatever",
            headers={"Authorization": "Bearer test-token"},
        )

    def test_preserves_authorization_on_redirect_to_slack_edge_cdn(self) -> None:
        handler = _SlackFileRedirectHandler()
        request = self._make_request()

        redirected = handler.redirect_request(
            request,
            io.BytesIO(),
            302,
            "Found",
            HTTPMessage(),
            "https://x.slack-edge.com/files/abc",
        )

        self.assertIsNotNone(redirected)
        assert redirected is not None
        self.assertEqual(redirected.full_url, "https://x.slack-edge.com/files/abc")
        self.assertEqual(redirected.get_header("Authorization"), "Bearer test-token")

    def test_rejects_redirect_to_non_slack_host_with_bad_request(self) -> None:
        handler = _SlackFileRedirectHandler()
        request = self._make_request()

        with self.assertRaises(SlackAPIError) as raised:
            handler.redirect_request(
                request,
                io.BytesIO(),
                302,
                "Found",
                HTTPMessage(),
                "https://attacker.com/files/abc",
            )

        self.assertEqual(raised.exception.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            raised.exception.body,
            {"error": "slack file download redirected to a non-Slack URL"},
        )


if __name__ == "__main__":
    # `client_module` is imported to make the `internals.client.urllib...`
    # patch target in the rejection tests resolvable even when this module is
    # executed directly.
    assert client_module is not None
    unittest.main()
