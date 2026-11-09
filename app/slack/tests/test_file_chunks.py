from __future__ import annotations

import base64
import hashlib
import unittest
from unittest import mock
from typing import Any

import gestalt
import provider
import internals.operations as operations


class FileChunkTests(unittest.TestCase):
    def setUp(self) -> None:
        self.body = b"synthetic-file" * 100_000
        self.metadata = {
            "file": {
                "id": "FTEST",
                "name": "contract.txt",
                "mimetype": "text/plain",
                "size": len(self.body),
                "url_private_download": "https://files.slack.com/files-pri/file",
            }
        }

    def get(self, **changes: Any) -> Any:
        values: dict[str, Any] = {
            "file_id": "FTEST",
            "offset": 0,
            "length": 1048576,
            "expected_sha256": "",
        } | changes
        return provider.files_get_chunk(
            provider.GetFileChunkInput(**values), gestalt.Request(token="caller-token")
        )

    def test_chunks_reconstruct_exact_original_with_pinned_digest(self) -> None:
        with (
            mock.patch.object(
                operations, "slack_get", return_value=self.metadata
            ) as info,
            mock.patch.object(
                operations, "get_bytes", return_value=(self.body, False)
            ) as download,
        ):
            first = self.get()["data"]
            second = self.get(
                offset=first["next_offset"], expected_sha256=first["sha256"]
            )["data"]
        self.assertEqual(
            base64.b64decode(first["content_base64"])
            + base64.b64decode(second["content_base64"]),
            self.body,
        )
        self.assertEqual(first["sha256"], hashlib.sha256(self.body).hexdigest())
        self.assertFalse(first["done"])
        self.assertTrue(second["done"])
        self.assertEqual(first["size"], len(self.body))
        info.assert_called_with("files.info", {"file": "FTEST"}, "caller-token")
        download.assert_called_with(
            "https://files.slack.com/files-pri/file", "caller-token", 25 * 1024 * 1024
        )

    def test_missing_token_and_invalid_ranges_do_not_fetch_files(self) -> None:
        with mock.patch.object(operations, "slack_get") as info:
            denied = provider.files_get_chunk(
                provider.GetFileChunkInput(file_id="FTEST"), gestalt.Request()
            )
            self.assertEqual(denied.status, 401)
            for values in [
                {"file_id": "https://evil.example"},
                {"offset": -1},
                {"offset": True},
                {"length": False},
                {"length": 0},
                {"length": 1048577},
                {"offset": 1},
                {"expected_sha256": "bad"},
            ]:
                self.assertEqual(self.get(**values).status, 400)
            info.assert_not_called()

    def test_foreign_identity_oversized_and_non_slack_metadata_never_download(
        self,
    ) -> None:
        cases = [
            {"id": "FOTHER"},
            {"size": 25 * 1024 * 1024 + 1},
            {"size": True},
            {"size": 0},
            {"url_private_download": "https://evil.example/file"},
            {"url_private_download": "https://user:pass@files.slack.com/file"},
            {"url_private_download": "https://files.slack.com:8443/file"},
        ]
        for changes in cases:
            metadata = {"file": self.metadata["file"] | changes}
            with (
                mock.patch.object(operations, "slack_get", return_value=metadata),
                mock.patch.object(operations, "get_bytes") as download,
            ):
                self.assertEqual(self.get().status, 400)
                download.assert_not_called()

    def test_changed_or_incomplete_bytes_are_not_returned(self) -> None:
        digest = hashlib.sha256(self.body).hexdigest()
        with mock.patch.object(operations, "slack_get", return_value=self.metadata):
            for response in [(self.body[:-1], False), (self.body, True)]:
                with mock.patch.object(operations, "get_bytes", return_value=response):
                    self.assertEqual(self.get().status, 400)
            changed = b"X" + self.body[1:]
            with mock.patch.object(
                operations, "get_bytes", return_value=(changed, False)
            ):
                self.assertEqual(
                    self.get(offset=1048576, expected_sha256=digest).status, 409
                )

    def test_read_only_operation_catalog_is_registered(self) -> None:
        operation = next(
            item
            for item in provider.app.catalog_dict()["operations"]
            if item["id"] == "files.getChunk"
        )
        self.assertTrue(operation["read_only"])
        self.assertEqual(operation["method"], "POST")

    def test_download_client_enforces_full_file_sentinel_bound(self) -> None:
        class Response:
            def __enter__(self) -> Response:
                return self

            def __exit__(self, *_args: object) -> None:
                pass

            def read(self, size: int) -> bytes:
                self_test.assertEqual(size, 25 * 1024 * 1024 + 1)
                return b"x" * size

        self_test = self
        metadata = {"file": self.metadata["file"] | {"size": 25 * 1024 * 1024}}
        opener = mock.Mock()
        opener.open.return_value = Response()
        with (
            mock.patch.object(operations, "slack_get", return_value=metadata),
            mock.patch("urllib.request.build_opener", return_value=opener),
        ):
            result = self.get()
        self.assertEqual(result.status, 400)
        request = opener.open.call_args.args[0]
        self.assertEqual(request.get_header("Authorization"), "Bearer caller-token")
        self.assertEqual(request.full_url, "https://files.slack.com/files-pri/file")

    def test_malformed_dispatch_parameters_are_caller_errors(self) -> None:
        with mock.patch.object(operations, "slack_get") as info:
            for params in [
                {"file_id": None},
                {"file_id": []},
                {"file_id": "FTEST", "offset": None},
                {"file_id": "FTEST", "length": None},
                {"file_id": "FTEST", "expected_sha256": None},
            ]:
                result = provider.app.execute(
                    "files.getChunk", params, gestalt.Request(token="caller-token")
                )
                self.assertEqual(result.status, 400)
            info.assert_not_called()

    def test_redirects_never_forward_credentials_to_nonstandard_or_credentialed_urls(
        self,
    ) -> None:
        import urllib.request
        from http.client import HTTPMessage
        from internals.client import _SlackFileRedirectHandler, SlackClientError

        source = urllib.request.Request(
            "https://files.slack.com/private",
            headers={"Authorization": "Bearer secret"},
        )
        redirect = _SlackFileRedirectHandler()
        for destination in [
            "https://files.slack.com:8443/file",
            "https://user:pass@files.slack.com/file",
            "https://:@files.slack.com/file",
            "https://evil.example/file",
            "http://files.slack.com/file",
            "https://files.slack.com/file#fragment",
        ]:
            with self.assertRaises(SlackClientError):
                redirect.redirect_request(
                    source, mock.Mock(), 302, "Found", HTTPMessage(), destination
                )
        allowed = redirect.redirect_request(
            source,
            mock.Mock(),
            302,
            "Found",
            HTTPMessage(),
            "https://cdn.slack-files.com/file",
        )
        assert allowed is not None
        self.assertEqual(allowed.get_header("Authorization"), "Bearer secret")
