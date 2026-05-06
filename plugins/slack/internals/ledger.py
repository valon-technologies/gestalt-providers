from __future__ import annotations

import base64
import copy
import logging
import time
from typing import Any

import gestalt

from .models import (
    SlackAgentEvent,
    SlackAgentRoute,
    SlackAssistantLedgerConfig,
    SlackReplyRef,
)

logger = logging.getLogger(__name__)

STATUS_RECEIVED = "received"
STATUS_SIGNALED = "signaled"
STATUS_REPLY_POSTED = "reply_posted"
STATUS_FAILED = "failed"
STATUS_FAILED_REPLY_PENDING = "failed_reply_pending"
STATUS_DEAD_LETTERED = "dead_lettered"

TERMINAL_STATUSES = frozenset(
    {STATUS_REPLY_POSTED, STATUS_FAILED, STATUS_DEAD_LETTERED}
)
INDEX_RECORD_ID = "v1/index"


class SlackAssistantRequestLedger:
    def __init__(self, config: SlackAssistantLedgerConfig) -> None:
        self._config = config
        self._client: Any | None = None
        self._initialized = False

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    def close(self) -> None:
        if self._client is None:
            return
        try:
            self._client.close()
        finally:
            self._client = None
            self._initialized = False

    def get(self, record_id: str) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        try:
            return copy.deepcopy(self._store().get(record_id))
        except gestalt.NotFoundError:
            return None

    def put(self, record: dict[str, Any]) -> dict[str, Any]:
        if not self.enabled:
            return copy.deepcopy(record)
        self._store().put(copy.deepcopy(record))
        record_id = str(record.get("id") or "").strip()
        if record_id and record_id != INDEX_RECORD_ID:
            self._remember_record_id(record_id)
        return copy.deepcopy(record)

    def record_received(
        self,
        event: SlackAgentEvent,
        *,
        route: SlackAgentRoute | None,
        subject_id: str,
        reply_ref: str,
    ) -> dict[str, Any]:
        record_id = request_record_id(event)
        now = _utc_timestamp()
        existing = self.get(record_id) or {}
        status = str(existing.get("status") or "").strip()
        if status in TERMINAL_STATUSES:
            return existing
        record = {
            **existing,
            "id": record_id,
            "schema_version": 1,
            "status": STATUS_RECEIVED,
            "team_id": event.team_id,
            "channel_id": event.channel_id,
            "message_ts": event.message_ts,
            "thread_ts": event.thread_ts,
            "reply_thread_ts": event.reply_thread_ts,
            "event_id": event.event_id,
            "client_msg_id": event.client_msg_id,
            "user_id": event.user_id,
            "subject_id": subject_id,
            "route_id": route.id if route is not None else "",
            "reply_ref": reply_ref,
            "event": event_to_record(event),
            "created_at": str(existing.get("created_at") or now),
            "updated_at": now,
        }
        return self.put(record)

    def mark_signaled(
        self,
        record_id: str,
    ) -> dict[str, Any] | None:
        return self.update(
            record_id,
            status=STATUS_SIGNALED,
            last_error="",
        )

    def mark_ack_error(self, record_id: str, error: str) -> dict[str, Any] | None:
        return self.update(record_id, acknowledgement_error=error, last_error=error)

    def mark_assistant_status_error(
        self, record_id: str, error: str
    ) -> dict[str, Any] | None:
        return self.update(record_id, assistant_status_error=error, last_error=error)

    def mark_reply_posted(
        self,
        ref: SlackReplyRef,
        *,
        reply_ts: str,
        channel_id: str,
    ) -> dict[str, Any] | None:
        record_id = request_record_id_from_ref(ref)
        return self.update(
            record_id,
            status=STATUS_REPLY_POSTED,
            reply_channel_id=channel_id,
            reply_ts=reply_ts,
            replied_at=_utc_timestamp(),
            last_error="",
        )

    def mark_reply_failed(
        self,
        ref: SlackReplyRef,
        *,
        error: str,
        retryable: bool = True,
    ) -> dict[str, Any] | None:
        record_id = request_record_id_from_ref(ref)
        return self.update(
            record_id,
            status=STATUS_FAILED_REPLY_PENDING if retryable else STATUS_FAILED,
            reply_error=error,
            last_error=error,
        )

    def mark_failed(self, record_id: str, error: str) -> dict[str, Any] | None:
        return self.update(record_id, status=STATUS_FAILED, last_error=error)

    def mark_dead_lettered(self, record_id: str, error: str) -> dict[str, Any] | None:
        return self.update(record_id, status=STATUS_DEAD_LETTERED, last_error=error)

    def increment_recovery_attempts(
        self,
        record_id: str,
    ) -> dict[str, Any] | None:
        record = self.get(record_id)
        if record is None:
            return None
        attempts = _int(record.get("recovery_attempts"))
        updates: dict[str, Any] = {
            "status": STATUS_SIGNALED,
            "recovery_attempts": attempts + 1,
            "last_recovery_attempt_at": _utc_timestamp(),
            "last_error": "",
        }
        record.update({key: value for key, value in updates.items() if value is not None})
        record["updated_at"] = _utc_timestamp()
        return self.put(record)

    def update(self, record_id: str, **updates: Any) -> dict[str, Any] | None:
        if not self.enabled:
            return None
        record = self.get(record_id)
        if record is None:
            return None
        clean_updates = {
            key: value for key, value in updates.items() if value is not None
        }
        record.update(clean_updates)
        record["updated_at"] = _utc_timestamp()
        return self.put(record)

    def stale_records(self, *, now: int | None = None) -> list[dict[str, Any]]:
        if not self.enabled:
            return []
        current = now if now is not None else int(time.time())
        cutoff = current - self._config.stale_after_seconds
        stale: list[dict[str, Any]] = []
        for record_id in self._record_ids():
            record = self.get(record_id)
            if record is None:
                continue
            status = str(record.get("status") or "").strip()
            if status in TERMINAL_STATUSES:
                continue
            if _timestamp_seconds(record.get("updated_at")) <= cutoff:
                stale.append(record)
        return stale

    def _store(self) -> Any:
        client = self._ensure_client()
        if not self._initialized:
            try:
                client.create_object_store(self._config.store)
            except gestalt.AlreadyExistsError:
                pass
            self._initialized = True
        return client.object_store(self._config.store)

    def _ensure_client(self) -> Any:
        if self._client is None:
            self._client = gestalt.IndexedDB(self._config.indexeddb_provider or None)
        return self._client

    def _record_ids(self) -> list[str]:
        try:
            index = self._store().get(INDEX_RECORD_ID)
        except gestalt.NotFoundError:
            return []
        ids = index.get("record_ids") if isinstance(index, dict) else None
        if not isinstance(ids, list):
            return []
        return [str(record_id) for record_id in ids if str(record_id).strip()]

    def _remember_record_id(self, record_id: str) -> None:
        try:
            index = self._store().get(INDEX_RECORD_ID)
        except gestalt.NotFoundError:
            index = {"id": INDEX_RECORD_ID, "record_ids": []}
        ids = index.get("record_ids")
        if not isinstance(ids, list):
            ids = []
        if record_id not in ids:
            ids.append(record_id)
        index["record_ids"] = ids
        index["updated_at"] = _utc_timestamp()
        self._store().put(copy.deepcopy(index))


_ledger: SlackAssistantRequestLedger | None = None


def reset_assistant_request_ledger() -> None:
    global _ledger
    if _ledger is not None:
        _ledger.close()
    _ledger = None


def assistant_request_ledger(
    config: SlackAssistantLedgerConfig,
) -> SlackAssistantRequestLedger:
    global _ledger
    if _ledger is None or _ledger._config != config:
        if _ledger is not None:
            _ledger.close()
        _ledger = SlackAssistantRequestLedger(config)
    return _ledger


def request_record_id(event: SlackAgentEvent) -> str:
    return _record_id(
        team_id=event.team_id,
        channel_id=event.channel_id,
        message_ts=event.message_ts,
        thread_ts=event.reply_thread_ts or event.thread_ts,
        event_id=event.event_id,
        client_msg_id=event.client_msg_id,
    )


def request_record_id_from_ref(ref: SlackReplyRef) -> str:
    return _record_id(
        team_id=ref.team_id,
        channel_id=ref.channel_id,
        message_ts=ref.message_ts,
        thread_ts=ref.reply_thread_ts,
        event_id=ref.event_id,
        client_msg_id=ref.client_msg_id,
    )


def _record_id(
    *,
    team_id: str,
    channel_id: str,
    message_ts: str,
    thread_ts: str,
    event_id: str,
    client_msg_id: str,
) -> str:
    return "/".join(
        [
            "v1",
            "team",
            _b64(team_id),
            "channel",
            _b64(channel_id),
            "message",
            _b64(message_ts),
            "thread",
            _b64(thread_ts),
            "event",
            _b64(event_id),
            "client",
            _b64(client_msg_id),
        ]
    )


def event_to_record(event: SlackAgentEvent) -> dict[str, Any]:
    return {
        "callback_type": event.callback_type,
        "event_type": event.event_type,
        "event_id": event.event_id,
        "team_id": event.team_id,
        "user_id": event.user_id,
        "channel_id": event.channel_id,
        "channel_type": event.channel_type,
        "text": event.text,
        "message_ts": event.message_ts,
        "thread_ts": event.thread_ts,
        "reply_thread_ts": event.reply_thread_ts,
        "client_msg_id": event.client_msg_id,
        "addressed_to_bot": event.addressed_to_bot,
        "assistant_context_present": event.assistant_context_present,
        "bot_user_id": event.bot_user_id,
        "context_channel_id": event.context_channel_id,
        "files": [dict(file_data) for file_data in event.files],
    }


def event_from_record(record: dict[str, Any]) -> SlackAgentEvent | None:
    data = record.get("event")
    if not isinstance(data, dict):
        return None
    return SlackAgentEvent(
        callback_type=str(data.get("callback_type") or "").strip(),
        event_type=str(data.get("event_type") or "").strip(),
        event_id=str(data.get("event_id") or "").strip(),
        team_id=str(data.get("team_id") or "").strip(),
        user_id=str(data.get("user_id") or "").strip(),
        channel_id=str(data.get("channel_id") or "").strip(),
        channel_type=str(data.get("channel_type") or "").strip(),
        text=str(data.get("text") or ""),
        message_ts=str(data.get("message_ts") or "").strip(),
        thread_ts=str(data.get("thread_ts") or "").strip(),
        reply_thread_ts=str(data.get("reply_thread_ts") or "").strip(),
        client_msg_id=str(data.get("client_msg_id") or "").strip(),
        addressed_to_bot=bool(data.get("addressed_to_bot")),
        assistant_context_present=bool(data.get("assistant_context_present")),
        bot_user_id=str(data.get("bot_user_id") or "").strip(),
        context_channel_id=str(data.get("context_channel_id") or "").strip(),
        files=tuple(
            file_data for file_data in data.get("files", []) if isinstance(file_data, dict)
        ),
    )


def _b64(value: str) -> str:
    return base64.urlsafe_b64encode(str(value or "").encode("utf-8")).decode(
        "ascii"
    ).rstrip("=")


def _utc_timestamp() -> str:
    return str(int(time.time()))


def _timestamp_seconds(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
