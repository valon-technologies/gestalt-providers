from __future__ import annotations

from typing import Any


def is_managed_subject_id(subject_id: str) -> bool:
    return subject_id.strip().startswith("service_account:")


def created_by_subject_id_from_record(record: dict[str, Any]) -> str:
    return str(record.get("created_by_subject_id", "") or "").strip()
