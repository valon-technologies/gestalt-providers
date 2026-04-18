from __future__ import annotations

import base64
import unittest
from contextlib import AbstractContextManager
from http import HTTPStatus
from typing import cast
from unittest import mock

import gestalt

import provider as provider_module


def make_request() -> gestalt.Request:
    return gestalt.Request(
        token="-----BEGIN RSA PRIVATE KEY-----\npretend\n-----END RSA PRIVATE KEY-----",
        connection_params={
            "host": "sftp.deluxe.example",
            "username": "valon",
            "host_key": "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCtestkey",
            "lockbox_number": "0043",
            "state_code": "TX",
            "inbound_directory": "/Inbox",
            "upload_directory": "/",
            "inbound_filename_prefix": "VAL",
            "outbound_filename_prefix": "VAL",
        },
    )


class FakeClient(AbstractContextManager["FakeClient"]):
    def __init__(self, *, files: list[str] | None = None, content: bytes = b"") -> None:
        self.files = files or []
        self.content = content
        self.writes: list[tuple[str, bytes]] = []

    def __enter__(self) -> FakeClient:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def list_files(self, folder_path: str) -> list[str]:
        return self.files

    def read_file(self, remote_path: str) -> bytes:
        self.last_read_path = remote_path
        return self.content

    def write_file(self, remote_path: str, data: bytes) -> None:
        self.writes.append((remote_path, data))


class DeluxeProviderTests(unittest.TestCase):
    def test_format_filename_uses_connection_defaults(self) -> None:
        result = provider_module.files_format_filename(
            provider_module.FormatFilenameInput(file_type="STOP", timestamp="2024-10-02T12:34:56+00:00"),
            make_request(),
        )

        self.assertEqual(result, {"filename": "VALSTPTX.X0043.20241002.123456.csv"})

    def test_list_inbound_ar_and_image_files_groups_matching_pairs(self) -> None:
        fake_client = FakeClient(
            files=[
                "/Inbox/VALARFTX.X0043.20241002.123456.csv",
                "/Inbox/VALIMGTX.X0043.20241002.123456.zip",
                "/Inbox/VALRETTX.X0043.20241002.123456.csv",
            ]
        )

        with mock.patch("internals.sftp_client.DeluxeSFTPClient.from_request", return_value=fake_client):
            result = provider_module.files_list_inbound_ar_and_image_files(provider_module.EmptyInput(), make_request())

        self.assertEqual(result["skipped_dates"], [])
        self.assertEqual(len(result["pairs"]), 1)
        pair = result["pairs"][0]
        self.assertEqual(pair["ar_file"]["filename"], "VALARFTX.X0043.20241002.123456.csv")
        self.assertEqual(pair["check_images_zip_file"]["filename"], "VALIMGTX.X0043.20241002.123456.zip")

    def test_list_inbound_return_files_skips_duplicate_dates(self) -> None:
        fake_client = FakeClient(
            files=[
                "/Inbox/VALRETTX.X0043.20241002.123456.csv",
                "/Inbox/VALRETTX.X0043.20241002.223456.csv",
            ]
        )

        with mock.patch("internals.sftp_client.DeluxeSFTPClient.from_request", return_value=fake_client):
            result = provider_module.files_list_inbound_return_files(provider_module.EmptyInput(), make_request())

        self.assertEqual(result["files"], [])
        self.assertEqual(result["skipped_dates"], ["2024-10-02"])

    def test_upload_validates_filename_and_writes_bytes(self) -> None:
        fake_client = FakeClient()
        with mock.patch("internals.sftp_client.DeluxeSFTPClient.from_request", return_value=fake_client):
            result = provider_module.files_upload(
                provider_module.UploadFileInput(
                    file_type="STOP",
                    filename="VALSTPTX.X0043.20241002.123456.csv",
                    text_content="loan_number\n10001\n",
                ),
                make_request(),
            )

        self.assertEqual(result["uploaded"], True)
        self.assertEqual(fake_client.writes, [("/VALSTPTX.X0043.20241002.123456.csv", b"loan_number\n10001\n")])

    def test_download_returns_base64_and_utf8_text(self) -> None:
        fake_client = FakeClient(content=b"hello,world\n")
        with mock.patch("internals.sftp_client.DeluxeSFTPClient.from_request", return_value=fake_client):
            result = provider_module.files_download(
                provider_module.DownloadFileInput(remote_path="/Inbox/VALRETTX.X0043.20241002.123456.csv"),
                make_request(),
            )

        self.assertEqual(result["size_bytes"], 12)
        self.assertEqual(result["text_content"], "hello,world\n")
        self.assertEqual(result["content_base64"], base64.b64encode(b"hello,world\n").decode("ascii"))

    def test_missing_private_key_is_rejected(self) -> None:
        result = provider_module.files_download(
            provider_module.DownloadFileInput(remote_path="/Inbox/file.txt"),
            gestalt.Request(connection_params={"host": "x", "username": "y", "host_key": "z", "lockbox_number": "0043"}),
        )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(response.body, {"error": "RSA private key is required"})


if __name__ == "__main__":
    unittest.main()
