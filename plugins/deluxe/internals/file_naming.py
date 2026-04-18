from __future__ import annotations

import posixpath
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Final

DEFAULT_DATETIME_FORMAT = "%Y%m%d.%H%M%S"
DEFAULT_INBOUND_DIRECTORY = "/Inbox"
DEFAULT_UPLOAD_DIRECTORY = "/"
DEFAULT_STATE_CODE = "TX"
DEFAULT_FILENAME_PREFIX = "VAL"


class FileNamingError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class FileTypeConfig:
    type_code: str
    filename_suffix: str


FILE_TYPE_CONFIGS: Final[dict[str, FileTypeConfig]] = {
    "STOP": FileTypeConfig(type_code="STP", filename_suffix=".csv"),
    "LOOKUP": FileTypeConfig(type_code="LKP", filename_suffix=".txt"),
    "AR": FileTypeConfig(type_code="ARF", filename_suffix=".csv"),
    "CHECK_IMAGES_ZIP": FileTypeConfig(type_code="IMG", filename_suffix=".zip"),
    "RETURNS": FileTypeConfig(type_code="RET", filename_suffix=".csv"),
}

FILE_TYPE_BY_CODE: Final[dict[str, str]] = {
    config.type_code: file_type for file_type, config in FILE_TYPE_CONFIGS.items()
}

_TYPE_CODES: Final[str] = "|".join(FILE_TYPE_BY_CODE.keys())
_FILENAME_PATTERN: Final[re.Pattern[str]] = re.compile(
    rf"^([A-Z]+?)({_TYPE_CODES})([A-Z]{{2}})\.X(\d{{4}})\.(\d{{8}}\.\d{{6}})(.+)$"
)


@dataclass(frozen=True, slots=True)
class FileIdentifier:
    file_type: str
    timestamp: datetime
    lockbox_number: str
    datetime_format: str = DEFAULT_DATETIME_FORMAT
    inbound_directory: str = DEFAULT_INBOUND_DIRECTORY
    state_code: str = DEFAULT_STATE_CODE
    filename_prefix: str = DEFAULT_FILENAME_PREFIX
    filename_suffix_override: str | None = None

    @property
    def filename(self) -> str:
        config = FILE_TYPE_CONFIGS[self.file_type]
        prefix = f"{self.filename_prefix}{config.type_code}{self.state_code}"
        suffix = self.filename_suffix_override or config.filename_suffix
        return f"{prefix}.X{self.lockbox_number}.{self.timestamp.strftime(self.datetime_format)}{suffix}"

    @property
    def remote_path(self) -> str:
        return posixpath.join(self.inbound_directory, self.filename)


def suffix_for_file_type(file_type: str, *, stop_file_extension: str, lookup_file_extension: str) -> str | None:
    if file_type == "STOP":
        return stop_file_extension
    if file_type == "LOOKUP":
        return lookup_file_extension
    return None


def format_filename(
    *,
    file_type: str,
    timestamp: datetime,
    lockbox_number: str,
    state_code: str = DEFAULT_STATE_CODE,
    filename_prefix: str = DEFAULT_FILENAME_PREFIX,
    filename_suffix_override: str | None = None,
    datetime_format: str = DEFAULT_DATETIME_FORMAT,
) -> str:
    _require_supported_file_type(file_type)
    return FileIdentifier(
        file_type=file_type,
        timestamp=timestamp,
        lockbox_number=lockbox_number,
        datetime_format=datetime_format,
        state_code=state_code,
        filename_prefix=filename_prefix,
        filename_suffix_override=filename_suffix_override,
    ).filename


def parse_filename(
    *,
    filename: str,
    expected_lockbox_number: str | None = None,
    state_code: str | None = None,
    filename_prefix: str | None = None,
    expected_suffix: str | None = None,
    inbound_directory: str = DEFAULT_INBOUND_DIRECTORY,
) -> FileIdentifier:
    matches = _FILENAME_PATTERN.match(filename)
    if matches is None:
        raise FileNamingError("filename does not match Deluxe naming rules")

    matched_prefix = matches.group(1)
    matched_type_code = matches.group(2)
    matched_state_code = matches.group(3)
    matched_lockbox_number = matches.group(4)
    matched_timestamp = matches.group(5)
    matched_suffix = matches.group(6)

    file_type = FILE_TYPE_BY_CODE.get(matched_type_code)
    if file_type is None:
        raise FileNamingError("unsupported Deluxe file type code")

    if filename_prefix is not None and matched_prefix != filename_prefix:
        raise FileNamingError("filename prefix does not match expected prefix")
    if state_code is not None and matched_state_code != state_code:
        raise FileNamingError("filename state code does not match expected state code")
    if expected_lockbox_number is not None and matched_lockbox_number != expected_lockbox_number:
        raise FileNamingError("filename lockbox number does not match expected lockbox number")

    config = FILE_TYPE_CONFIGS[file_type]
    suffix_to_check = expected_suffix if expected_suffix is not None else config.filename_suffix
    if matched_suffix != suffix_to_check:
        raise FileNamingError("filename suffix does not match expected suffix")

    try:
        parsed_timestamp = datetime.strptime(matched_timestamp, DEFAULT_DATETIME_FORMAT)
    except ValueError as err:
        raise FileNamingError("filename timestamp is invalid") from err

    return FileIdentifier(
        file_type=file_type,
        timestamp=parsed_timestamp,
        lockbox_number=matched_lockbox_number,
        inbound_directory=inbound_directory,
        state_code=matched_state_code,
        filename_prefix=matched_prefix,
        filename_suffix_override=expected_suffix,
    )


def _require_supported_file_type(file_type: str) -> None:
    if file_type not in FILE_TYPE_CONFIGS:
        supported = ", ".join(sorted(FILE_TYPE_CONFIGS))
        raise FileNamingError(f"unsupported file_type {file_type!r}; expected one of {supported}")
