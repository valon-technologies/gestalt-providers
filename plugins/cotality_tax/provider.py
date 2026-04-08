from __future__ import annotations

import base64
import fnmatch
import json
import posixpath
import stat
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from pathlib import Path
from typing import Any, Iterator

import gestalt
import paramiko

plugin = gestalt.Plugin.from_manifest(Path(__file__).resolve().parent / "plugin.yaml")

PORT_PARAM = "port"
HOST_KEY_PARAM = "host_key"
PATH_PREFIX_PARAM = "path_prefix"

LIST_OPS = [('outbound.listFiles', 'List files published by Cotality in the configured outbound_dir.', 'outbound_dir')]
DOWNLOAD_OPS = [('outbound.downloadFile', 'Download a file from the configured outbound_dir.', 'outbound_dir')]
UPLOAD_OPS = [('inbound.uploadFile', 'Upload a file to the configured inbound_dir used for submissions to Cotality.', 'inbound_dir')]
DELETE_OPS = []


class ListRemoteFilesInput(gestalt.Model):
    remote_dir: str = gestalt.field(description="Remote directory to list", default="", required=False)
    pattern: str = gestalt.field(description="Optional filename glob pattern", default="", required=False)
    recursive: bool = gestalt.field(description="List files recursively", default=False, required=False)
    include_dirs: bool = gestalt.field(description="Include directories in the result set", default=False, required=False)
    max_items: int = gestalt.field(description="Maximum number of entries to return", default=500, required=False)


class DownloadRemoteFileInput(gestalt.Model):
    remote_path: str = gestalt.field(description="Remote file path")
    decode_text: bool = gestalt.field(description="Decode the file as text instead of returning base64", default=False, required=False)
    encoding: str = gestalt.field(description="Text encoding used when decode_text is true", default="utf-8", required=False)


class UploadRemoteFileInput(gestalt.Model):
    remote_path: str = gestalt.field(description="Remote destination path")
    content_text: str = gestalt.field(description="Plain-text content to upload", default="", required=False)
    content_base64: str = gestalt.field(description="Base64-encoded file content to upload", default="", required=False)
    ensure_parent_dirs: bool = gestalt.field(description="Create missing parent directories before upload", default=False, required=False)
    overwrite: bool = gestalt.field(description="Allow replacing an existing remote file", default=True, required=False)


class MoveRemoteFileInput(gestalt.Model):
    source_path: str = gestalt.field(description="Existing remote path")
    destination_path: str = gestalt.field(description="New remote path")
    ensure_parent_dirs: bool = gestalt.field(description="Create missing parent directories before moving the file", default=False, required=False)
    overwrite: bool = gestalt.field(description="Allow replacing an existing remote file", default=False, required=False)


class DeleteRemoteFileInput(gestalt.Model):
    remote_path: str = gestalt.field(description="Remote file path to delete")


class ListConfiguredFilesInput(gestalt.Model):
    pattern: str = gestalt.field(description="Optional filename glob pattern", default="", required=False)
    recursive: bool = gestalt.field(description="List files recursively", default=False, required=False)
    include_dirs: bool = gestalt.field(description="Include directories in the result set", default=False, required=False)
    max_items: int = gestalt.field(description="Maximum number of entries to return", default=500, required=False)
    directory_override: str = gestalt.field(description="Optional explicit remote directory to use instead of the configured connection parameter", default="", required=False)


class DownloadConfiguredFileInput(gestalt.Model):
    file_name: str = gestalt.field(description="File name or relative path inside the configured directory")
    directory_override: str = gestalt.field(description="Optional explicit remote directory to use instead of the configured connection parameter", default="", required=False)
    decode_text: bool = gestalt.field(description="Decode the file as text instead of returning base64", default=False, required=False)
    encoding: str = gestalt.field(description="Text encoding used when decode_text is true", default="utf-8", required=False)


class UploadConfiguredFileInput(gestalt.Model):
    file_name: str = gestalt.field(description="Destination file name or relative path inside the configured directory")
    directory_override: str = gestalt.field(description="Optional explicit remote directory to use instead of the configured connection parameter", default="", required=False)
    content_text: str = gestalt.field(description="Plain-text content to upload", default="", required=False)
    content_base64: str = gestalt.field(description="Base64-encoded file content to upload", default="", required=False)
    ensure_parent_dirs: bool = gestalt.field(description="Create missing parent directories before upload", default=False, required=False)
    overwrite: bool = gestalt.field(description="Allow replacing an existing remote file", default=True, required=False)


class DeleteConfiguredFileInput(gestalt.Model):
    file_name: str = gestalt.field(description="File name or relative path inside the configured directory")
    directory_override: str = gestalt.field(description="Optional explicit remote directory to use instead of the configured connection parameter", default="", required=False)


@dataclass(slots=True)
class SFTPConnectionSettings:
    host: str
    username: str
    password: str
    port: int
    host_key: str | None
    path_prefix: str | None


@dataclass(slots=True)
class SFTPBundle:
    client: paramiko.SSHClient
    sftp: paramiko.SFTPClient
    settings: SFTPConnectionSettings

    def close(self) -> None:
        self.sftp.close()
        self.client.close()


@plugin.operation(id="connection.test", method="POST", description='Open an SFTP session to the configured Cotality (CoreLogic) endpoint.')
def connection_test(req: gestalt.Request) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    try:
        with _open_sftp(req) as bundle:
            return {
                "connected": True,
                "host": bundle.settings.host,
                "username": bundle.settings.username,
                "port": bundle.settings.port,
                "verified_host_key": bool(bundle.settings.host_key),
                "path_prefix": bundle.settings.path_prefix,
            }
    except ValueError as err:
        return _bad_request(str(err))
    except Exception as err:
        return _server_error(str(err))


@plugin.operation(id="remote.listFiles", method="POST", description='List files in any remote Cotality directory.', read_only=True)
def remote_list_files(input: ListRemoteFilesInput, req: gestalt.Request) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    return _list_remote_files(req, remote_dir=input.remote_dir, pattern=input.pattern, recursive=input.recursive, include_dirs=input.include_dirs, max_items=input.max_items)


@plugin.operation(id="remote.downloadFile", method="POST", description='Download a file from any remote Cotality path.', read_only=True)
def remote_download_file(input: DownloadRemoteFileInput, req: gestalt.Request) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    return _download_remote_file(req, remote_path=input.remote_path, decode_text=input.decode_text, encoding=input.encoding)


@plugin.operation(id="remote.uploadFile", method="POST", description='Upload a file to any remote Cotality path.')
def remote_upload_file(input: UploadRemoteFileInput, req: gestalt.Request) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    return _upload_remote_file(req, remote_path=input.remote_path, content_text=input.content_text, content_base64=input.content_base64, ensure_parent_dirs=input.ensure_parent_dirs, overwrite=input.overwrite)


@plugin.operation(id="remote.moveFile", method="POST", description='Move or rename a file on the remote Cotality endpoint.')
def remote_move_file(input: MoveRemoteFileInput, req: gestalt.Request) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    return _move_remote_file(req, source_path=input.source_path, destination_path=input.destination_path, ensure_parent_dirs=input.ensure_parent_dirs, overwrite=input.overwrite)


@plugin.operation(id="remote.deleteFile", method="POST", description='Delete a file from the remote Cotality endpoint.')
def remote_delete_file(input: DeleteRemoteFileInput, req: gestalt.Request) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    return _delete_remote_file(req, remote_path=input.remote_path)


def _register_configured_operations() -> None:
    for op_id, description, dir_param in LIST_OPS:
        plugin.operation(id=op_id, method="POST", description=description, read_only=True)(_make_list_handler(dir_param))
    for op_id, description, dir_param in DOWNLOAD_OPS:
        plugin.operation(id=op_id, method="POST", description=description, read_only=True)(_make_download_handler(dir_param))
    for op_id, description, dir_param in UPLOAD_OPS:
        plugin.operation(id=op_id, method="POST", description=description)(_make_upload_handler(dir_param))
    for op_id, description, dir_param in DELETE_OPS:
        plugin.operation(id=op_id, method="POST", description=description)(_make_delete_handler(dir_param))


def _make_list_handler(dir_param: str):
    def handler(input: ListConfiguredFilesInput, req: gestalt.Request) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
        directory = _resolve_named_directory(req, dir_param, input.directory_override)
        if isinstance(directory, gestalt.Response):
            return directory
        return _list_remote_files(req, remote_dir=directory, pattern=input.pattern, recursive=input.recursive, include_dirs=input.include_dirs, max_items=input.max_items)

    handler.__name__ = f"list_{dir_param}"
    return handler


def _make_download_handler(dir_param: str):
    def handler(input: DownloadConfiguredFileInput, req: gestalt.Request) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
        directory = _resolve_named_directory(req, dir_param, input.directory_override)
        if isinstance(directory, gestalt.Response):
            return directory
        remote_path = _join_remote(directory, input.file_name)
        return _download_remote_file(req, remote_path=remote_path, decode_text=input.decode_text, encoding=input.encoding)

    handler.__name__ = f"download_{dir_param}"
    return handler


def _make_upload_handler(dir_param: str):
    def handler(input: UploadConfiguredFileInput, req: gestalt.Request) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
        directory = _resolve_named_directory(req, dir_param, input.directory_override)
        if isinstance(directory, gestalt.Response):
            return directory
        remote_path = _join_remote(directory, input.file_name)
        return _upload_remote_file(req, remote_path=remote_path, content_text=input.content_text, content_base64=input.content_base64, ensure_parent_dirs=input.ensure_parent_dirs, overwrite=input.overwrite)

    handler.__name__ = f"upload_{dir_param}"
    return handler


def _make_delete_handler(dir_param: str):
    def handler(input: DeleteConfiguredFileInput, req: gestalt.Request) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
        directory = _resolve_named_directory(req, dir_param, input.directory_override)
        if isinstance(directory, gestalt.Response):
            return directory
        remote_path = _join_remote(directory, input.file_name)
        return _delete_remote_file(req, remote_path=remote_path)

    handler.__name__ = f"delete_{dir_param}"
    return handler


def _resolve_named_directory(req: gestalt.Request, dir_param: str, override: str) -> str | gestalt.Response[dict[str, str]]:
    directory = (override or req.connection_param(dir_param)).strip()
    if not directory:
        return _bad_request(f"{dir_param} connection parameter is required for this operation")
    return directory


def _list_remote_files(req: gestalt.Request, *, remote_dir: str, pattern: str, recursive: bool, include_dirs: bool, max_items: int) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    if max_items < 1:
        return _bad_request("max_items must be greater than 0")
    try:
        with _open_sftp(req) as bundle:
            directory = _resolve_remote_path(remote_dir, bundle.settings, empty_means_root=True)
            entries = _collect_entries(bundle.sftp, directory, recursive=recursive, include_dirs=include_dirs, pattern=pattern, max_items=max_items)
            return {"directory": directory, "count": len(entries), "files": entries}
    except FileNotFoundError as err:
        return _not_found(str(err))
    except ValueError as err:
        return _bad_request(str(err))
    except Exception as err:
        return _server_error(str(err))


def _download_remote_file(req: gestalt.Request, *, remote_path: str, decode_text: bool, encoding: str) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    try:
        with _open_sftp(req) as bundle:
            resolved_path = _resolve_remote_path(remote_path, bundle.settings)
            attrs = bundle.sftp.stat(resolved_path)
            if stat.S_ISDIR(attrs.st_mode):
                return _bad_request(f"{resolved_path} is a directory")
            with bundle.sftp.open(resolved_path, "rb") as remote_file:
                data = remote_file.read()
            body: dict[str, Any] = {
                "remote_path": resolved_path,
                "size": attrs.st_size,
                "modified_at": _isoformat_timestamp(getattr(attrs, "st_mtime", None)),
            }
            if decode_text:
                body["content_text"] = data.decode(encoding)
                body["encoding"] = encoding
            else:
                body["content_base64"] = base64.b64encode(data).decode("ascii")
            return body
    except FileNotFoundError as err:
        return _not_found(str(err))
    except UnicodeDecodeError as err:
        return _bad_request(f"failed to decode file as text: {err}")
    except ValueError as err:
        return _bad_request(str(err))
    except Exception as err:
        return _server_error(str(err))


def _upload_remote_file(req: gestalt.Request, *, remote_path: str, content_text: str, content_base64: str, ensure_parent_dirs: bool, overwrite: bool) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    try:
        payload = _resolve_upload_bytes(content_text=content_text, content_base64=content_base64)
        with _open_sftp(req) as bundle:
            resolved_path = _resolve_remote_path(remote_path, bundle.settings)
            if ensure_parent_dirs:
                _ensure_parent_directory(bundle.sftp, resolved_path)
            if not overwrite and _path_exists(bundle.sftp, resolved_path):
                return _conflict(f"remote file already exists: {resolved_path}")
            with bundle.sftp.open(resolved_path, "wb") as remote_file:
                remote_file.write(payload)
            attrs = bundle.sftp.stat(resolved_path)
            return {
                "remote_path": resolved_path,
                "size": attrs.st_size,
                "modified_at": _isoformat_timestamp(getattr(attrs, "st_mtime", None)),
            }
    except FileNotFoundError as err:
        return _not_found(str(err))
    except ValueError as err:
        return _bad_request(str(err))
    except Exception as err:
        return _server_error(str(err))


def _move_remote_file(req: gestalt.Request, *, source_path: str, destination_path: str, ensure_parent_dirs: bool, overwrite: bool) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    try:
        with _open_sftp(req) as bundle:
            resolved_source = _resolve_remote_path(source_path, bundle.settings)
            resolved_destination = _resolve_remote_path(destination_path, bundle.settings)
            if ensure_parent_dirs:
                _ensure_parent_directory(bundle.sftp, resolved_destination)
            if not overwrite and _path_exists(bundle.sftp, resolved_destination):
                return _conflict(f"remote file already exists: {resolved_destination}")
            bundle.sftp.rename(resolved_source, resolved_destination)
            return {"source_path": resolved_source, "destination_path": resolved_destination}
    except FileNotFoundError as err:
        return _not_found(str(err))
    except ValueError as err:
        return _bad_request(str(err))
    except Exception as err:
        return _server_error(str(err))


def _delete_remote_file(req: gestalt.Request, *, remote_path: str) -> dict[str, Any] | gestalt.Response[dict[str, str]]:
    try:
        with _open_sftp(req) as bundle:
            resolved_path = _resolve_remote_path(remote_path, bundle.settings)
            attrs = bundle.sftp.stat(resolved_path)
            if stat.S_ISDIR(attrs.st_mode):
                return _bad_request(f"{resolved_path} is a directory")
            bundle.sftp.remove(resolved_path)
            return {"deleted": True, "remote_path": resolved_path}
    except FileNotFoundError as err:
        return _not_found(str(err))
    except ValueError as err:
        return _bad_request(str(err))
    except Exception as err:
        return _server_error(str(err))


@contextmanager
def _open_sftp(req: gestalt.Request) -> Iterator[SFTPBundle]:
    settings = _parse_connection_settings(req)
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=settings.host,
        port=settings.port,
        username=settings.username,
        password=settings.password,
        look_for_keys=False,
        allow_agent=False,
        timeout=30,
        banner_timeout=30,
        auth_timeout=30,
    )
    if settings.host_key:
        remote_key = client.get_transport().get_remote_server_key() if client.get_transport() else None
        if remote_key is None:
            client.close()
            raise RuntimeError("failed to read remote host key")
        actual = f"{remote_key.get_name()} {remote_key.get_base64()}"
        if not _host_keys_match(settings.host_key, actual):
            client.close()
            raise RuntimeError("remote host key did not match the configured host_key connection parameter")
    sftp = client.open_sftp()
    bundle = SFTPBundle(client=client, sftp=sftp, settings=settings)
    try:
        yield bundle
    finally:
        bundle.close()


def _parse_connection_settings(req: gestalt.Request) -> SFTPConnectionSettings:
    raw_token = req.token.strip()
    if not raw_token:
        raise ValueError("manual connection credentials are required")
    try:
        creds = json.loads(raw_token)
    except json.JSONDecodeError as err:
        raise ValueError("manual connection credential payload must be valid JSON") from err
    if not isinstance(creds, dict):
        raise ValueError("manual connection credential payload must be a JSON object")

    host = str(creds.get("host", "")).strip()
    username = str(creds.get("username", "")).strip()
    password = str(creds.get("password", "")).strip()
    if not host:
        raise ValueError("host credential is required")
    if not username:
        raise ValueError("username credential is required")
    if not password:
        raise ValueError("password credential is required")

    port_value = req.connection_param(PORT_PARAM).strip()
    port = 22
    if port_value:
        try:
            port = int(port_value)
        except ValueError as err:
            raise ValueError("port connection parameter must be an integer") from err
        if port < 1 or port > 65535:
            raise ValueError("port connection parameter must be between 1 and 65535")

    host_key = req.connection_param(HOST_KEY_PARAM).strip() or None
    path_prefix = req.connection_param(PATH_PREFIX_PARAM).strip() or None
    return SFTPConnectionSettings(host=host, username=username, password=password, port=port, host_key=host_key, path_prefix=path_prefix)


def _resolve_remote_path(path: str, settings: SFTPConnectionSettings, *, empty_means_root: bool = False) -> str:
    cleaned = (path or "").strip()
    if not cleaned:
        if empty_means_root:
            return settings.path_prefix or "."
        raise ValueError("remote path is required")
    if cleaned.startswith("/"):
        return posixpath.normpath(cleaned)
    if settings.path_prefix:
        return posixpath.normpath(posixpath.join(settings.path_prefix, cleaned))
    return posixpath.normpath(cleaned)


def _collect_entries(sftp: paramiko.SFTPClient, remote_dir: str, *, recursive: bool, include_dirs: bool, pattern: str, max_items: int) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    stack = [remote_dir]
    while stack:
        current = stack.pop()
        for attr in sftp.listdir_attr(current):
            child_path = _join_remote(current, attr.filename)
            is_dir = stat.S_ISDIR(attr.st_mode)
            if is_dir and recursive:
                stack.append(child_path)
            if is_dir and not include_dirs:
                continue
            if pattern and not fnmatch.fnmatch(attr.filename, pattern):
                continue
            entries.append(
                {
                    "path": child_path,
                    "name": attr.filename,
                    "is_dir": is_dir,
                    "size": None if is_dir else attr.st_size,
                    "modified_at": _isoformat_timestamp(getattr(attr, "st_mtime", None)),
                }
            )
            if len(entries) >= max_items:
                return entries
    return entries


def _ensure_parent_directory(sftp: paramiko.SFTPClient, remote_path: str) -> None:
    parent = posixpath.dirname(remote_path)
    if not parent or parent in {".", "/"}:
        return
    segments = [segment for segment in parent.split("/") if segment]
    current = "/" if parent.startswith("/") else ""
    for segment in segments:
        current = _join_remote(current or ".", segment) if current not in {"", "/"} else ("/" + segment if current == "/" else segment)
        if not _path_exists(sftp, current):
            sftp.mkdir(current)


def _path_exists(sftp: paramiko.SFTPClient, remote_path: str) -> bool:
    try:
        sftp.stat(remote_path)
        return True
    except FileNotFoundError:
        return False


def _resolve_upload_bytes(*, content_text: str, content_base64: str) -> bytes:
    has_text = bool(content_text)
    has_base64 = bool(content_base64)
    if has_text == has_base64:
        raise ValueError("provide exactly one of content_text or content_base64")
    if has_text:
        return content_text.encode("utf-8")
    try:
        return base64.b64decode(content_base64.encode("ascii"), validate=True)
    except Exception as err:
        raise ValueError("content_base64 must be valid base64") from err


def _join_remote(left: str, right: str) -> str:
    if not left or left == ".":
        return posixpath.normpath(right)
    if left == "/":
        return posixpath.normpath("/" + right)
    return posixpath.normpath(posixpath.join(left, right))


def _host_keys_match(expected: str, actual: str) -> bool:
    def normalize(value: str) -> str:
        return " ".join(value.strip().split())
    return normalize(expected) == normalize(actual)


def _isoformat_timestamp(timestamp: int | float | None) -> str | None:
    if timestamp is None:
        return None
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _bad_request(message: str) -> gestalt.Response[dict[str, str]]:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})


def _not_found(message: str) -> gestalt.Response[dict[str, str]]:
    return gestalt.Response(status=HTTPStatus.NOT_FOUND, body={"error": message})


def _conflict(message: str) -> gestalt.Response[dict[str, str]]:
    return gestalt.Response(status=HTTPStatus.CONFLICT, body={"error": message})


def _server_error(message: str) -> gestalt.Response[dict[str, str]]:
    return gestalt.Response(status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": message})


_register_configured_operations()


if __name__ == "__main__":
    plugin.serve()
