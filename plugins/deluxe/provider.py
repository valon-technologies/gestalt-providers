from __future__ import annotations

import base64
import posixpath
from datetime import UTC, date, datetime
from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt

from internals import (
    DEFAULT_DATETIME_FORMAT,
    DEFAULT_INBOUND_DIRECTORY,
    DEFAULT_STATE_CODE,
    DEFAULT_UPLOAD_DIRECTORY,
    DeluxeSFTPClient,
    FileIdentifier,
    FileNamingError,
    format_filename,
    parse_filename,
    suffix_for_file_type,
)

ErrorResponse: TypeAlias = gestalt.Response[dict[str, Any]]
OperationResult: TypeAlias = dict[str, Any] | ErrorResponse


class EmptyInput(gestalt.Model):
    pass


class FormatFilenameInput(gestalt.Model):
    file_type: str = gestalt.field(description="Deluxe file type such as STOP, LOOKUP, AR, CHECK_IMAGES_ZIP, or RETURNS.")
    timestamp: str = gestalt.field(
        description="Optional ISO-8601 timestamp. Defaults to the current UTC time.",
        default="",
        required=False,
    )


class ParseFilenameInput(gestalt.Model):
    filename: str = gestalt.field(description="Filename to parse.")


class UploadFileInput(gestalt.Model):
    file_type: str = gestalt.field(description="Deluxe file type such as STOP or LOOKUP.")
    filename: str = gestalt.field(description="Filename to upload.")
    content_base64: str = gestalt.field(
        description="Base64-encoded file contents.",
        default="",
        required=False,
    )
    text_content: str = gestalt.field(
        description="UTF-8 text file contents. Use instead of content_base64 for plain text or CSV payloads.",
        default="",
        required=False,
    )
    remote_directory: str = gestalt.field(
        description="Optional remote upload directory override.",
        default="",
        required=False,
    )


class DownloadFileInput(gestalt.Model):
    remote_path: str = gestalt.field(description="Exact remote path to download.")


@gestalt.operation(
    id="files.formatFilename",
    method="POST",
    description="Format a Deluxe lockbox filename using the configured lockbox metadata.",
    read_only=True,
)
def files_format_filename(input: FormatFilenameInput, req: gestalt.Request) -> OperationResult:
    lockbox_number = _connection_param(req, "lockbox_number")
    if not lockbox_number:
        return _bad_request("lockbox_number connection parameter is required")

    try:
        timestamp = _parse_timestamp(input.timestamp)
        filename = format_filename(
            file_type=input.file_type.strip(),
            timestamp=timestamp,
            lockbox_number=lockbox_number,
            state_code=_connection_param(req, "state_code") or DEFAULT_STATE_CODE,
            filename_prefix=_connection_param(req, "outbound_filename_prefix") or "VAL",
            filename_suffix_override=_suffix_override(req, input.file_type.strip()),
            datetime_format=_connection_param(req, "datetime_format") or DEFAULT_DATETIME_FORMAT,
        )
    except FileNamingError as err:
        return _bad_request(str(err))
    except ValueError as err:
        return _bad_request(str(err))

    return {"filename": filename}


@gestalt.operation(
    id="files.parseFilename",
    method="POST",
    description="Parse a Deluxe lockbox filename into its structured components.",
    read_only=True,
)
def files_parse_filename(input: ParseFilenameInput, req: gestalt.Request) -> OperationResult:
    try:
        file_id = parse_filename(
            filename=input.filename.strip(),
            expected_lockbox_number=_connection_param(req, "lockbox_number") or None,
            state_code=_connection_param(req, "state_code") or None,
            filename_prefix=_connection_param(req, "inbound_filename_prefix") or None,
            inbound_directory=_connection_param(req, "inbound_directory") or DEFAULT_INBOUND_DIRECTORY,
        )
    except FileNamingError as err:
        return _bad_request(str(err))

    return _file_identifier_dict(file_id)


@gestalt.operation(
    id="files.listInboundArAndImageFiles",
    method="GET",
    description="List inbound Deluxe AR files and their paired image ZIP files.",
    read_only=True,
)
def files_list_inbound_ar_and_image_files(_input: EmptyInput, req: gestalt.Request) -> OperationResult:
    credentials_error = _validate_sftp_request(req)
    if credentials_error is not None:
        return credentials_error

    file_ids_by_date = _list_inbound_files_by_date(req, {"AR", "CHECK_IMAGES_ZIP"})
    pairs: list[dict[str, Any]] = []
    skipped_dates: list[str] = []
    for process_date in sorted(file_ids_by_date):
        file_ids = file_ids_by_date[process_date]
        ar_file = next((item for item in file_ids if item.file_type == "AR"), None)
        image_file = next((item for item in file_ids if item.file_type == "CHECK_IMAGES_ZIP"), None)
        if ar_file is None or image_file is None or len(file_ids) != 2:
            skipped_dates.append(process_date.isoformat())
            continue
        pairs.append(
            {
                "process_date": process_date.isoformat(),
                "ar_file": _file_identifier_dict(ar_file),
                "check_images_zip_file": _file_identifier_dict(image_file),
            }
        )

    return {"pairs": pairs, "skipped_dates": skipped_dates}


@gestalt.operation(
    id="files.listInboundReturnFiles",
    method="GET",
    description="List inbound Deluxe return files.",
    read_only=True,
)
def files_list_inbound_return_files(_input: EmptyInput, req: gestalt.Request) -> OperationResult:
    credentials_error = _validate_sftp_request(req)
    if credentials_error is not None:
        return credentials_error

    file_ids_by_date = _list_inbound_files_by_date(req, {"RETURNS"})
    files: list[dict[str, Any]] = []
    skipped_dates: list[str] = []
    for process_date in sorted(file_ids_by_date):
        matched = file_ids_by_date[process_date]
        if len(matched) != 1:
            skipped_dates.append(process_date.isoformat())
            continue
        files.append(_file_identifier_dict(matched[0]))

    return {"files": files, "skipped_dates": skipped_dates}


@gestalt.operation(
    id="files.upload",
    method="POST",
    description="Upload a Deluxe file to the configured SFTP server after validating its filename.",
)
def files_upload(input: UploadFileInput, req: gestalt.Request) -> OperationResult:
    credentials_error = _validate_sftp_request(req)
    if credentials_error is not None:
        return credentials_error

    file_type = input.file_type.strip()
    filename = input.filename.strip()
    if not file_type:
        return _bad_request("file_type is required")
    if not filename:
        return _bad_request("filename is required")

    try:
        parse_filename(
            filename=filename,
            expected_lockbox_number=_connection_param(req, "lockbox_number") or None,
            state_code=_connection_param(req, "state_code") or None,
            filename_prefix=_connection_param(req, "outbound_filename_prefix") or None,
            expected_suffix=_suffix_override(req, file_type),
        )
    except FileNamingError as err:
        return _bad_request(str(err))

    content_result = _file_bytes(input)
    if isinstance(content_result, gestalt.Response):
        return content_result

    remote_directory = input.remote_directory.strip() or _connection_param(req, "upload_directory") or DEFAULT_UPLOAD_DIRECTORY
    remote_path = posixpath.join(remote_directory, filename)
    try:
        with DeluxeSFTPClient.from_request(req) as client:
            client.write_file(remote_path, content_result)
    except Exception as err:
        return gestalt.Response(status=HTTPStatus.BAD_GATEWAY, body={"error": str(err)})

    return {"uploaded": True, "remote_path": remote_path, "bytes_written": len(content_result)}


@gestalt.operation(
    id="files.download",
    method="GET",
    description="Download a file from the Deluxe SFTP server.",
    read_only=True,
)
def files_download(input: DownloadFileInput, req: gestalt.Request) -> OperationResult:
    credentials_error = _validate_sftp_request(req)
    if credentials_error is not None:
        return credentials_error

    remote_path = input.remote_path.strip()
    if not remote_path:
        return _bad_request("remote_path is required")

    try:
        with DeluxeSFTPClient.from_request(req) as client:
            content = client.read_file(remote_path)
    except Exception as err:
        return gestalt.Response(status=HTTPStatus.BAD_GATEWAY, body={"error": str(err)})

    text_content: str | None
    try:
        text_content = content.decode("utf-8")
    except UnicodeDecodeError:
        text_content = None

    return {
        "remote_path": remote_path,
        "size_bytes": len(content),
        "content_base64": base64.b64encode(content).decode("ascii"),
        "text_content": text_content,
    }


def _list_inbound_files_by_date(req: gestalt.Request, file_types: set[str]) -> dict[date, list[FileIdentifier]]:
    inbound_directory = _connection_param(req, "inbound_directory") or DEFAULT_INBOUND_DIRECTORY
    expected_lockbox_number = _connection_param(req, "lockbox_number") or None
    state_code = _connection_param(req, "state_code") or None
    filename_prefix = _connection_param(req, "inbound_filename_prefix") or None

    file_ids_by_date: dict[date, list[FileIdentifier]] = {}
    with DeluxeSFTPClient.from_request(req) as client:
        filenames = client.list_files(inbound_directory)
    for remote_path in filenames:
        filename = posixpath.basename(remote_path)
        try:
            file_id = parse_filename(
                filename=filename,
                expected_lockbox_number=expected_lockbox_number,
                state_code=state_code,
                filename_prefix=filename_prefix,
                inbound_directory=inbound_directory,
            )
        except FileNamingError:
            continue
        if file_id.file_type not in file_types:
            continue
        process_date = file_id.timestamp.date()
        file_ids_by_date.setdefault(process_date, []).append(file_id)
    return file_ids_by_date


def _connection_param(req: gestalt.Request, name: str) -> str:
    return req.connection_param(name).strip()


def _parse_timestamp(raw: str) -> datetime:
    if not raw.strip():
        return datetime.now(UTC)
    value = raw.strip().replace("Z", "+00:00")
    return datetime.fromisoformat(value)


def _suffix_override(req: gestalt.Request, file_type: str) -> str | None:
    return suffix_for_file_type(
        file_type,
        stop_file_extension=_connection_param(req, "stop_file_extension") or ".csv",
        lookup_file_extension=_connection_param(req, "lookup_file_extension") or ".txt",
    )


def _file_bytes(input: UploadFileInput) -> bytes | ErrorResponse:
    if input.content_base64.strip():
        try:
            return base64.b64decode(input.content_base64, validate=True)
        except ValueError as err:
            return _bad_request(f"content_base64 is not valid base64: {err}")
    if input.text_content:
        return input.text_content.encode("utf-8")
    return _bad_request("either content_base64 or text_content is required")


def _validate_sftp_request(req: gestalt.Request) -> ErrorResponse | None:
    if not req.token.strip():
        return gestalt.Response(status=HTTPStatus.UNAUTHORIZED, body={"error": "RSA private key is required"})
    for field in ("host", "username", "host_key", "lockbox_number"):
        if not _connection_param(req, field):
            return _bad_request(f"{field} connection parameter is required")
    return None


def _file_identifier_dict(file_id: FileIdentifier) -> dict[str, Any]:
    return {
        "file_type": file_id.file_type,
        "filename": file_id.filename,
        "remote_path": file_id.remote_path,
        "timestamp": file_id.timestamp.isoformat(),
        "process_date": file_id.timestamp.date().isoformat(),
        "lockbox_number": file_id.lockbox_number,
        "state_code": file_id.state_code,
        "filename_prefix": file_id.filename_prefix,
        "filename_suffix": posixpath.splitext(file_id.filename)[1],
    }


def _bad_request(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})
