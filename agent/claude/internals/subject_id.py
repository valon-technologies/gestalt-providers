from __future__ import annotations

from typing import Any

import gestalt


def created_by_subject_id_from_actor(actor: Any) -> str:
    if actor is None:
        return ""
    if isinstance(actor, gestalt.AgentActor):
        return actor.subject_id.strip()
    if isinstance(actor, dict):
        return str(actor.get("subject_id", "") or "").strip()
    return str(getattr(actor, "subject_id", "") or "").strip()


def agent_actor_from_created_by_subject_id(subject_id: str) -> gestalt.AgentActor | None:
    subject_id = subject_id.strip()
    if not subject_id:
        return None
    return gestalt.AgentActor(subject_id=subject_id)


def is_managed_subject_id(subject_id: str) -> bool:
    return subject_id.strip().startswith("service_account:")


def created_by_subject_id_from_record(record: dict[str, Any]) -> str:
    subject_id = str(record.get("created_by_subject_id", "") or "").strip()
    if subject_id:
        return subject_id
    created_by = record.get("created_by")
    if isinstance(created_by, dict):
        return str(created_by.get("subject_id", "") or "").strip()
    return ""
