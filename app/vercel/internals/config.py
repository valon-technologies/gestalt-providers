from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


class VercelBlobConfigurationError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class VercelBlobConfig:
    token: str = ""

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> VercelBlobConfig:
        value = config.get("blobReadWriteToken", "")
        if isinstance(value, str):
            return cls(token=value.strip())
        return cls()

    def require_token(self) -> str:
        return require_blob_token(self)


def blob_config_from_mapping(config: dict[str, Any]) -> VercelBlobConfig:
    return VercelBlobConfig.from_config(config)


def require_blob_token(config: VercelBlobConfig) -> str:
    token = (
        config.token
        or os.getenv("BLOB_READ_WRITE_TOKEN", "").strip()
        or os.getenv("VERCEL_BLOB_READ_WRITE_TOKEN", "").strip()
    )
    if not token:
        raise VercelBlobConfigurationError("blobReadWriteToken is not configured")
    return token
