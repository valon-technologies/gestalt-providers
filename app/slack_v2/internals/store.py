from __future__ import annotations

import threading
from typing import Any

import gestalt

OBJECT_STORE_NAME = "slack_v2_event_registrations"

_client: Any | None = None
_init_lock = threading.Lock()
_initialized = False


def get_workflow_event_subject_for_app(*, app_id: str) -> str:
    record = _object_store().get(app_id)
    workflow_event_subject = record.get("workflow_event_subject")
    if not isinstance(workflow_event_subject, str) or not workflow_event_subject.strip():
        raise gestalt.NotFoundError(f"workflow_event_subject not found for app_id {app_id!r}")
    return workflow_event_subject.strip()


def save_slack_event_registration(
    *,
    app_id: str,
    client_id: str,
    client_secret: str,
    signing_secret: str,
    display_name: str,
    workflow_event_subject: str,
) -> None:
    _ensure_initialized()
    _object_store().put(
        {
            "id": app_id,
            "client_id": client_id,
            "client_secret": client_secret,
            "signing_secret": signing_secret,
            "display_name": display_name,
            "workflow_event_subject": workflow_event_subject,
        }
    )


def _ensure_initialized() -> None:
    global _client, _initialized
    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return
        client = gestalt.IndexedDB()
        try:
            client.create_object_store(OBJECT_STORE_NAME)
        except gestalt.AlreadyExistsError:
            pass
        _client = client
        _initialized = True


def _object_store() -> Any:
    _ensure_initialized()
    assert _client is not None
    return _client.object_store(OBJECT_STORE_NAME)
