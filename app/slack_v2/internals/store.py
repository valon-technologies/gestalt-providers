from __future__ import annotations

import threading
from typing import Any

import gestalt

OBJECT_STORE_NAME = "slack_v2_event_registrations"

_client: Any | None = None
_init_lock = threading.Lock()
_initialized = False


def get_workflow_definition_id_for_app(*, app_id: str) -> str:
    record = _object_store().get(app_id)
    workflow_definition_id = record.get("workflow_definition_id")
    if not isinstance(workflow_definition_id, str) or not workflow_definition_id.strip():
        raise gestalt.NotFoundError(f"workflow_definition_id not found for app_id {app_id!r}")
    return workflow_definition_id.strip()


def save_slack_event_registration(
    *,
    app_id: str,
    client_id: str,
    client_secret: str,
    signing_secret: str,
    display_name: str,
    workflow_definition_id: str,
) -> None:
    _ensure_initialized()
    _object_store().put(
        {
            "id": app_id,
            "client_id": client_id,
            "client_secret": client_secret,
            "signing_secret": signing_secret,
            "display_name": display_name,
            "workflow_definition_id": workflow_definition_id,
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
