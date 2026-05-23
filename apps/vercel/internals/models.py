from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class VercelBlobAccess(StrEnum):
    PRIVATE = "private"
    PUBLIC = "public"


@dataclass(frozen=True, slots=True, kw_only=True)
class VercelBlobPutRequest:
    pathname: str
    body: str
    body_base64: str
    access: VercelBlobAccess
    content_type: str = ""
    add_random_suffix: bool = False
    overwrite: bool = False
    cache_control_max_age: int | None = None


@dataclass(frozen=True, slots=True, kw_only=True)
class VercelBlobGetRequest:
    url_or_path: str
    access: VercelBlobAccess
    if_none_match: str = ""
    timeout_seconds: float | None = None
    use_cache: bool = True


@dataclass(frozen=True, slots=True, kw_only=True)
class VercelBlobHeadRequest:
    url_or_path: str


@dataclass(frozen=True, slots=True, kw_only=True)
class VercelBlobListRequest:
    limit: int | None = None
    prefix: str = ""
    cursor: str = ""
    mode: str = ""


@dataclass(frozen=True, slots=True, kw_only=True)
class VercelBlobDeleteRequest:
    targets: tuple[str, ...]


@dataclass(frozen=True, slots=True, kw_only=True)
class VercelBlobCopyRequest:
    source_url_or_path: str
    destination_path: str
    access: VercelBlobAccess
    content_type: str = ""
    add_random_suffix: bool = False
    overwrite: bool = False
    cache_control_max_age: int | None = None
