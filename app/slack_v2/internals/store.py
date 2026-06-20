from __future__ import annotations

import threading
from typing import Any

import gestalt

EVENT_REGISTRATION_OBJECT_STORE_NAME = "slack_v2_event_registrations"
DEBUG_PAYLOAD_OBJECT_STORE_NAME = "slack_v2_debug_payloads"

_client: Any | None = None
_init_lock = threading.Lock()
_initialized = False


def get_workflow_event_subject_for_app(*, app_id: str) -> str:
    record = _object_store(EVENT_REGISTRATION_OBJECT_STORE_NAME).get(app_id)
    workflow_event_subject = record.get("workflow_event_subject")
    if not isinstance(workflow_event_subject, str) or not workflow_event_subject.strip():
        raise gestalt.NotFoundError(f"workflow_event_subject not found for app_id {app_id!r}")
    return workflow_event_subject.strip()


def get_signing_secret_for_app(*, app_id: str) -> str:
    record = _object_store(EVENT_REGISTRATION_OBJECT_STORE_NAME).get(app_id)
    signing_secret = record.get("signing_secret")
    if not isinstance(signing_secret, str) or not signing_secret.strip():
        raise gestalt.NotFoundError(f"signing_secret not found for app_id {app_id!r}")
    return signing_secret.strip()


def list_signing_secrets() -> list[str]:
    secrets: list[str] = []
    for key in _object_store(EVENT_REGISTRATION_OBJECT_STORE_NAME).get_all_keys():
        try:
            secrets.append(get_signing_secret_for_app(app_id=str(key)))
        except gestalt.NotFoundError:
            continue
    return secrets


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
    _object_store(EVENT_REGISTRATION_OBJECT_STORE_NAME).put(
        {
            "id": app_id,
            "client_id": client_id,
            "client_secret": client_secret,
            "signing_secret": signing_secret,
            "display_name": display_name,
            "workflow_event_subject": workflow_event_subject,
        }
    )


def save_debug_payload(*, event_id: str, payload: dict[str, Any]) -> None:
    _object_store(DEBUG_PAYLOAD_OBJECT_STORE_NAME).put(
        {
            "id": event_id,
            "payload": payload,
        }
    )


def get_debug_payload(*, event_id: str) -> dict[str, Any]:
    record = _object_store(DEBUG_PAYLOAD_OBJECT_STORE_NAME).get(event_id)
    payload = record.get("payload")
    if not isinstance(payload, dict):
        raise gestalt.NotFoundError(f"debug payload not found for event_id {event_id!r}")
    return {"id": event_id, "payload": payload}


def list_debug_payload_ids() -> list[str]:
    return sorted(_object_store(DEBUG_PAYLOAD_OBJECT_STORE_NAME).get_all_keys())


def _ensure_initialized() -> None:
    global _client, _initialized
    if _initialized:
        return
    with _init_lock:
        if _initialized:
            return
        client = gestalt.IndexedDB()
        for object_store_name in (
            EVENT_REGISTRATION_OBJECT_STORE_NAME,
            DEBUG_PAYLOAD_OBJECT_STORE_NAME,
        ):
            try:
                client.create_object_store(object_store_name)
            except gestalt.AlreadyExistsError:
                pass
        _client = client
        _initialized = True


def _object_store(name: str) -> Any:
    _ensure_initialized()
    assert _client is not None
    return _client.object_store(name)
