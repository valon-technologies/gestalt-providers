from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

import gestalt

from gestalt.gen.v1 import agent_pb2 as _agent_pb2

agent_pb2: Any = cast(Any, _agent_pb2)

TERMINAL_STATUSES = {
    agent_pb2.AGENT_RUN_STATUS_SUCCEEDED,
    agent_pb2.AGENT_RUN_STATUS_FAILED,
    agent_pb2.AGENT_RUN_STATUS_CANCELED,
}


@dataclass(slots=True)
class StoredRun:
    run_id: str
    idempotency_key: str
    provider_name: str
    model: str
    status: int
    messages: list[dict[str, str]]
    output_text: str
    structured_output: dict[str, Any] | None
    status_message: str
    session_ref: str
    created_by: dict[str, str]
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    execution_ref: str
    cancel_reason: str


class SimpleRunStore:
    def __init__(self, *, run_store: str, idempotency_store: str) -> None:
        self._client = gestalt.IndexedDB()
        self._runs = self._client.object_store(run_store)
        self._idempotency = self._client.object_store(idempotency_store)
        self._run_store_name = run_store
        self._idempotency_store_name = idempotency_store

    def initialize(self) -> None:
        for name in (self._run_store_name, self._idempotency_store_name):
            try:
                self._client.create_object_store(name)
            except gestalt.AlreadyExistsError:
                pass

    def close(self) -> None:
        self._client.close()

    def begin_run(
        self,
        *,
        run_id: str,
        idempotency_key: str,
        provider_name: str,
        model: str,
        messages: list[dict[str, str]],
        session_ref: str,
        created_by: dict[str, str],
        execution_ref: str,
    ) -> tuple[StoredRun, bool]:
        existing = self.get_run(run_id)
        if existing is not None:
            return existing, False

        if idempotency_key:
            existing = self._run_for_idempotency_key(idempotency_key)
            if existing is not None:
                return existing, False

        now = _utcnow()
        run = StoredRun(
            run_id=run_id,
            idempotency_key=idempotency_key,
            provider_name=provider_name,
            model=model,
            status=agent_pb2.AGENT_RUN_STATUS_RUNNING,
            messages=messages,
            output_text="",
            structured_output=None,
            status_message="",
            session_ref=session_ref,
            created_by=created_by,
            created_at=now,
            started_at=now,
            completed_at=None,
            execution_ref=execution_ref,
            cancel_reason="",
        )

        claimed_idempotency = False
        if idempotency_key:
            try:
                self._idempotency.add(
                    {
                        "id": idempotency_key,
                        "run_id": run_id,
                        "provider_name": provider_name,
                        "created_at": now,
                    }
                )
                claimed_idempotency = True
            except gestalt.AlreadyExistsError:
                existing = self._run_for_idempotency_key(idempotency_key)
                if existing is not None:
                    return existing, False
                raise

        try:
            self._runs.add(_run_to_record(run))
        except gestalt.AlreadyExistsError:
            if claimed_idempotency:
                self._delete_idempotency(idempotency_key)
            existing = self.get_run(run_id)
            if existing is not None:
                return existing, False
            raise
        except Exception:
            if claimed_idempotency:
                self._delete_idempotency(idempotency_key)
            raise

        return run, True

    def get_run(self, run_id: str) -> StoredRun | None:
        try:
            return _record_to_run(self._runs.get(run_id))
        except gestalt.NotFoundError:
            return None

    def list_runs(self) -> list[StoredRun]:
        runs = [_record_to_run(record) for record in self._runs.get_all()]
        runs = [run for run in runs if run is not None]
        return sorted(runs, key=lambda run: (run.created_at, run.run_id), reverse=True)

    def request_cancel(self, run_id: str, reason: str) -> StoredRun | None:
        run = self.get_run(run_id)
        if run is None:
            return None
        if run.status in TERMINAL_STATUSES:
            return run
        cancel_reason = reason.strip() or "canceled"
        run.status = agent_pb2.AGENT_RUN_STATUS_CANCELED
        run.status_message = cancel_reason
        run.cancel_reason = cancel_reason
        run.completed_at = _utcnow()
        self._runs.put(_run_to_record(run))
        return self.get_run(run_id)

    def mark_succeeded(
        self,
        *,
        run_id: str,
        messages: list[dict[str, str]],
        output_text: str,
        structured_output: dict[str, Any] | None,
    ) -> StoredRun:
        current = self._require_run(run_id)
        if current.status in TERMINAL_STATUSES:
            return current
        current.status = agent_pb2.AGENT_RUN_STATUS_SUCCEEDED
        current.messages = messages
        current.output_text = output_text
        current.structured_output = structured_output
        current.status_message = ""
        current.completed_at = _utcnow()
        self._runs.put(_run_to_record(current))
        return self._require_run(run_id)

    def mark_failed(
        self,
        *,
        run_id: str,
        messages: list[dict[str, str]],
        status_message: str,
    ) -> StoredRun:
        current = self._require_run(run_id)
        if current.status in TERMINAL_STATUSES:
            return current
        current.status = agent_pb2.AGENT_RUN_STATUS_FAILED
        current.messages = messages
        current.status_message = status_message.strip()
        current.completed_at = _utcnow()
        self._runs.put(_run_to_record(current))
        return self._require_run(run_id)

    def _require_run(self, run_id: str) -> StoredRun:
        run = self.get_run(run_id)
        if run is None:
            raise RuntimeError(f"agent run {run_id!r} was not found")
        return run

    def _run_for_idempotency_key(self, idempotency_key: str) -> StoredRun | None:
        try:
            record = self._idempotency.get(idempotency_key)
        except gestalt.NotFoundError:
            return None
        run_id = str(record.get("run_id") or "").strip()
        if not run_id:
            return None
        return self.get_run(run_id)

    def _delete_idempotency(self, idempotency_key: str) -> None:
        if not idempotency_key:
            return
        try:
            self._idempotency.delete(idempotency_key)
        except gestalt.NotFoundError:
            pass


def _run_to_record(run: StoredRun) -> dict[str, Any]:
    return {
        "id": run.run_id,
        "idempotency_key": run.idempotency_key,
        "provider_name": run.provider_name,
        "model": run.model,
        "status": run.status,
        "messages": run.messages,
        "output_text": run.output_text,
        "structured_output": run.structured_output,
        "status_message": run.status_message,
        "session_ref": run.session_ref,
        "created_by": run.created_by,
        "created_at": run.created_at,
        "started_at": run.started_at,
        "completed_at": run.completed_at,
        "execution_ref": run.execution_ref,
        "cancel_reason": run.cancel_reason,
    }


def _record_to_run(record: dict[str, Any] | None) -> StoredRun | None:
    if record is None:
        return None
    return StoredRun(
        run_id=str(record.get("id") or ""),
        idempotency_key=str(record.get("idempotency_key") or ""),
        provider_name=str(record.get("provider_name") or ""),
        model=str(record.get("model") or ""),
        status=int(record.get("status") or agent_pb2.AGENT_RUN_STATUS_UNSPECIFIED),
        messages=_coerce_messages(record.get("messages")),
        output_text=str(record.get("output_text") or ""),
        structured_output=_coerce_optional_dict(record.get("structured_output")),
        status_message=str(record.get("status_message") or ""),
        session_ref=str(record.get("session_ref") or ""),
        created_by=_coerce_string_dict(record.get("created_by")),
        created_at=_coerce_required_datetime(record.get("created_at")),
        started_at=_coerce_datetime(record.get("started_at")),
        completed_at=_coerce_datetime(record.get("completed_at")),
        execution_ref=str(record.get("execution_ref") or ""),
        cancel_reason=str(record.get("cancel_reason") or ""),
    )


def _coerce_messages(raw_value: Any) -> list[dict[str, str]]:
    if not isinstance(raw_value, list):
        return []
    messages: list[dict[str, str]] = []
    for item in raw_value:
        if isinstance(item, dict):
            messages.append({"role": str(item.get("role") or ""), "text": str(item.get("text") or "")})
    return messages


def _coerce_string_dict(raw_value: Any) -> dict[str, str]:
    if not isinstance(raw_value, dict):
        return {}
    return {str(key): str(value or "") for key, value in raw_value.items()}


def _coerce_optional_dict(raw_value: Any) -> dict[str, Any] | None:
    if raw_value is None:
        return None
    if not isinstance(raw_value, dict):
        raise ValueError("stored structured_output must be an object")
    return raw_value


def _coerce_datetime(raw_value: Any) -> datetime | None:
    if raw_value is None or raw_value == "":
        return None
    if isinstance(raw_value, datetime):
        return raw_value.astimezone(UTC)
    return datetime.fromisoformat(str(raw_value)).astimezone(UTC)


def _coerce_required_datetime(raw_value: Any) -> datetime:
    parsed = _coerce_datetime(raw_value)
    if parsed is None:
        raise ValueError("expected a timestamp value")
    return parsed


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)
