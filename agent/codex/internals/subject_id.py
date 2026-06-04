from __future__ import annotations


def is_managed_subject_id(subject_id: str) -> bool:
    return subject_id.strip().startswith("service_account:")
