from __future__ import annotations

from typing import Any


def subject_id_from_request(request: Any) -> str:
    context = getattr(request, "context", None)
    if context is None:
        return ""
    subject = getattr(context, "subject", None)
    if subject is None:
        return ""
    return str(getattr(subject, "id", "") or "").strip()


def is_managed_subject_id(subject_id: str) -> bool:
    return subject_id.strip().startswith("service_account:")


def created_by_subject_id_from_record(record: dict[str, Any]) -> str:
    return str(record.get("created_by_subject_id", "") or "").strip()
