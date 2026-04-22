import pathlib
import sys
import unittest
import urllib.parse
import urllib.request
from http import HTTPStatus
from typing import cast
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

import gestalt

from internals import blob as blob_module
from internals import VercelBlobAPIError
import provider as provider_module


class FakeHTTPResponse:
    def __init__(self, body: str = "", headers: dict[str, str] | None = None, status: int = 200) -> None:
        self._body = body.encode("utf-8")
        self.headers = headers or {}
        self.status = status

    def __enter__(self) -> FakeHTTPResponse:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def authorization_header(request: urllib.request.Request) -> str | None:
    return request.get_header("Authorization") or dict(request.header_items()).get("Authorization")


def header(request: urllib.request.Request, name: str) -> str | None:
    items = {key.lower(): value for key, value in request.header_items()}
    return request.get_header(name) or items.get(name.lower())


class VercelProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        provider_module.configure("vercel", {"clientId": "client-id", "clientSecret": "client-secret"})

    def test_blob_put_requires_blob_token(self) -> None:
        result = provider_module.blob_put(
            provider_module.BlobPutInput(
                pathname="roadmaps/newrez.json",
                access="private",
                body='{"ok":true}',
            )
        )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(response.body, {"error": "blobReadWriteToken is not configured"})

    def test_blob_put_passes_configured_blob_token(self) -> None:
        provider_module.configure(
            "vercel",
            {
                "clientId": "client-id",
                "clientSecret": "client-secret",
                "blobReadWriteToken": "blob-rw-token",
            },
        )

        with mock.patch.object(provider_module, "put_blob", return_value={"data": {"blob": {"pathname": "roadmaps/newrez.json"}}}) as put_blob:
            result = provider_module.blob_put(
                provider_module.BlobPutInput(
                    pathname="roadmaps/newrez.json",
                    access="private",
                    body='{"ok":true}',
                    content_type="application/json",
                    overwrite=True,
                )
            )

        self.assertEqual(result["data"]["blob"]["pathname"], "roadmaps/newrez.json")
        self.assertEqual(put_blob.call_args.kwargs["pathname"], "roadmaps/newrez.json")
        self.assertEqual(put_blob.call_args.kwargs["access"], "private")
        self.assertEqual(put_blob.call_args.kwargs["body"], '{"ok":true}')
        self.assertEqual(put_blob.call_args.args[0].token, "blob-rw-token")

    def test_blob_put_rejects_invalid_access(self) -> None:
        result = provider_module.blob_put(
            provider_module.BlobPutInput(
                pathname="roadmaps/newrez.json",
                access="team-only",
                body="hello",
            )
        )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(response.body, {"error": "access must be either private or public"})

    def test_blob_head_maps_not_found_error(self) -> None:
        provider_module.configure(
            "vercel",
            {
                "clientId": "client-id",
                "clientSecret": "client-secret",
                "blobReadWriteToken": "blob-rw-token",
            },
        )

        with mock.patch.object(
            provider_module,
            "head_blob",
            side_effect=VercelBlobAPIError(404, "Vercel Blob: The requested blob does not exist"),
        ):
            result = provider_module.blob_head(provider_module.BlobHeadInput(url_or_path="roadmaps/missing.json"))

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.NOT_FOUND)
        self.assertEqual(response.body, {"error": "Vercel Blob: The requested blob does not exist"})

    def test_blob_get_maps_access_error(self) -> None:
        provider_module.configure(
            "vercel",
            {
                "clientId": "client-id",
                "clientSecret": "client-secret",
                "blobReadWriteToken": "blob-rw-token",
            },
        )

        with mock.patch.object(
            provider_module,
            "get_blob",
            side_effect=VercelBlobAPIError(
                403, "Vercel Blob: Access denied, please provide a valid token for this resource."
            ),
        ):
            result = provider_module.blob_get(
                provider_module.BlobGetInput(url_or_path="roadmaps/newrez.json", access="private")
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.FORBIDDEN)
        self.assertEqual(
            response.body,
            {"error": "Vercel Blob: Access denied, please provide a valid token for this resource."},
        )

    def test_blob_delete_requires_targets(self) -> None:
        result = provider_module.blob_delete(provider_module.BlobDeleteInput(targets=[]))

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(response.body, {"error": "targets must contain at least one non-empty value"})

    def test_blob_delete_calls_delete_blobs_with_trimmed_targets(self) -> None:
        provider_module.configure(
            "vercel",
            {
                "clientId": "client-id",
                "clientSecret": "client-secret",
                "blobReadWriteToken": "blob-rw-token",
            },
        )

        with mock.patch.object(provider_module, "delete_blobs", return_value={"data": {"deleted": 2}}) as delete_blobs:
            result = provider_module.blob_delete(
                provider_module.BlobDeleteInput(targets=[" roadmaps/a.json ", "", "roadmaps/b.json"])
            )

        self.assertEqual(result["data"]["deleted"], 2)
        self.assertEqual(delete_blobs.call_args.kwargs["targets"], ["roadmaps/a.json", "roadmaps/b.json"])
        self.assertEqual(delete_blobs.call_args.args[0].token, "blob-rw-token")

    def test_put_blob_uses_vercel_blob_put_contract(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30.0) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30.0)
            self.assertEqual(request.get_method(), "PUT")
            self.assertEqual(authorization_header(request), "Bearer blob_rw_x_store123_token")
            self.assertEqual(header(request, "x-api-version"), "11")
            self.assertEqual(header(request, "x-vercel-blob-access"), "private")
            self.assertEqual(header(request, "x-content-type"), "application/json")
            self.assertEqual(header(request, "x-allow-overwrite"), "1")
            self.assertEqual(header(request, "x-add-random-suffix"), "0")

            parsed = urllib.parse.urlsplit(request.full_url)
            self.assertEqual(parsed.scheme, "https")
            self.assertEqual(parsed.netloc, "vercel.com")
            self.assertEqual(parsed.path, "/api/blob")
            self.assertEqual(urllib.parse.parse_qs(parsed.query), {"pathname": ["roadmaps/newrez.json"]})
            self.assertEqual(request.data, b'{"ok":true}')

            return FakeHTTPResponse(
                '{"url":"https://store123.private.blob.vercel-storage.com/roadmaps/newrez.json","downloadUrl":"https://store123.private.blob.vercel-storage.com/roadmaps/newrez.json?download=1","pathname":"roadmaps/newrez.json","contentType":"application/json","contentDisposition":"attachment; filename=\\"newrez.json\\""}'
            )

        with mock.patch.object(blob_module.urllib.request, "urlopen", side_effect=fake_urlopen):
            result = blob_module.put_blob(
                blob_module.VercelBlobConfig(token="blob_rw_x_store123_token"),
                pathname="roadmaps/newrez.json",
                body='{"ok":true}',
                body_base64="",
                access="private",
                content_type="application/json",
                add_random_suffix=False,
                overwrite=True,
                cache_control_max_age=None,
            )

        self.assertEqual(result["data"]["blob"]["pathname"], "roadmaps/newrez.json")

    def test_get_blob_uses_private_blob_url_and_auth_header(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30.0) -> FakeHTTPResponse:
            self.assertEqual(timeout, 12.0)
            self.assertEqual(request.get_method(), "GET")
            self.assertEqual(authorization_header(request), "Bearer blob_rw_x_store123_token")
            self.assertEqual(
                request.full_url,
                "https://store123.private.blob.vercel-storage.com/roadmaps/newrez.json",
            )
            return FakeHTTPResponse(
                body='{"ok":true}',
                headers={
                    "content-type": "application/json",
                    "content-length": "11",
                    "cache-control": "max-age=60",
                    "content-disposition": 'attachment; filename="newrez.json"',
                    "last-modified": "Tue, 22 Apr 2026 12:00:00 GMT",
                    "etag": '"etag-1"',
                },
            )

        with mock.patch.object(blob_module.urllib.request, "urlopen", side_effect=fake_urlopen):
            result = blob_module.get_blob(
                blob_module.VercelBlobConfig(token="blob_rw_x_store123_token"),
                url_or_path="roadmaps/newrez.json",
                access="private",
                if_none_match="",
                timeout_seconds=12.0,
                use_cache=True,
            )

        blob = result["data"]["blob"]
        self.assertEqual(blob["pathname"], "roadmaps/newrez.json")
        self.assertEqual(blob["status_code"], 200)
        self.assertEqual(blob["content_text"], '{"ok":true}')

    def test_delete_blobs_uses_delete_contract(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30.0) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30.0)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(authorization_header(request), "Bearer blob_rw_x_store123_token")
            self.assertEqual(header(request, "content-type"), "application/json")
            self.assertEqual(request.full_url, "https://vercel.com/api/blob/delete")
            self.assertEqual(request.data, b'{"urls": ["roadmaps/a.json", "roadmaps/b.json"]}')
            return FakeHTTPResponse()

        with mock.patch.object(blob_module.urllib.request, "urlopen", side_effect=fake_urlopen):
            result = blob_module.delete_blobs(
                blob_module.VercelBlobConfig(token="blob_rw_x_store123_token"),
                targets=["roadmaps/a.json", "roadmaps/b.json"],
            )

        self.assertEqual(result["data"]["deleted"], 2)


if __name__ == "__main__":
    unittest.main()
