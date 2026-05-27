from __future__ import annotations

import base64
import copy
import logging
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import gestalt

from .config import (
    GitHubActionPreferencesConfig,
    GitHubWebhookPolicy,
    ACTION_PREFERENCES_FAILURE_CONFIG_DEFAULT,
    SELF_FIX_DISABLED,
    SELF_FIX_MODES,
)
from .identity import GitHubPreferenceIdentity

logger = logging.getLogger(__name__)

IDENTITY_EXTERNAL_SUBJECT_ID = "external_subject_id"
IDENTITY_SUBJECT_ID = "subject_id"


class GitHubActionPreferenceStore:
    def __init__(self, config: GitHubActionPreferencesConfig) -> None:
        self._config = config
        self._client: Any | None = None

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    def get(self, record_id: str) -> dict[str, Any] | None:
        if not self.enabled:
            raise RuntimeError("GitHub action preferences are not configured")
        try:
            return copy.deepcopy(self._store().get(record_id))
        except gestalt.NotFoundError:
            return None

    def put(self, record: dict[str, Any]) -> None:
        if not self.enabled:
            raise RuntimeError("GitHub action preferences are not configured")
        self._store().put(copy.deepcopy(record))

    def delete(self, record_id: str) -> bool:
        if not self.enabled:
            raise RuntimeError("GitHub action preferences are not configured")
        try:
            self._store().delete(record_id)
        except gestalt.NotFoundError:
            return False
        return True

    def close(self) -> None:
        if self._client is None:
            return
        try:
            self._client.close()
        finally:
            self._client = None

    def _store(self) -> Any:
        client = self._ensure_client()
        return client.object_store(self._config.store)

    def _ensure_client(self) -> Any:
        if self._client is None:
            self._client = gestalt.IndexedDB(self._config.indexeddb_provider or None)
        return self._client


_store: GitHubActionPreferenceStore | None = None


def reset_action_preference_store() -> None:
    global _store
    if _store is not None:
        _store.close()
    _store = None


def action_preference_store(
    config: GitHubActionPreferencesConfig,
) -> GitHubActionPreferenceStore:
    global _store
    if _store is None or _store._config != config:
        if _store is not None:
            _store.close()
        _store = GitHubActionPreferenceStore(config)
    return _store


def apply_action_preferences(
    policy: GitHubWebhookPolicy,
    config: GitHubActionPreferencesConfig,
    identity: GitHubPreferenceIdentity,
) -> GitHubWebhookPolicy:
    if not config.enabled:
        return policy

    metadata = _base_metadata(policy, identity)
    if not identity.external_subject_id and not identity.subject_id:
        metadata["source"] = "config_default"
        metadata["reason"] = "missing_preference_identity"
        return replace(policy, action_preferences=metadata)

    try:
        record, source = _lookup_preference_record(config, policy, identity)
    except Exception as err:
        if config.failure_mode != ACTION_PREFERENCES_FAILURE_CONFIG_DEFAULT:
            raise
        logger.warning("GitHub action preference lookup failed: %s", err)
        metadata["source"] = "config_default"
        metadata["reason"] = "store_error"
        return replace(policy, action_preferences=metadata)

    if record is None:
        metadata["source"] = "config_default"
        return replace(policy, action_preferences=metadata)

    try:
        preference = _validated_preference_record(record)
    except ValueError as err:
        logger.warning("GitHub action preference record ignored: %s", err)
        metadata["source"] = "config_default"
        metadata["reason"] = "invalid_record"
        return replace(policy, action_preferences=metadata)

    allow_code_review_comments = _merge_gate(
        policy.allow_code_review_comments,
        preference.get("allow_code_review_comments"),
    )
    self_fix_mode = _merge_self_fix_mode(
        policy,
        preference.get("self_fix_mode"),
        preference.get("allow_self_fix"),
    )
    metadata.update(
        {
            "source": source,
            "record_id": str(preference["id"]),
            "effective": {
                "allow_code_review_comments": allow_code_review_comments,
                "allow_self_fix": self_fix_mode != SELF_FIX_DISABLED,
                "self_fix_mode": self_fix_mode,
            },
        }
    )
    return replace(
        policy,
        allow_code_review_comments=allow_code_review_comments,
        self_fix_mode=self_fix_mode,
        action_preferences=metadata,
    )


def set_action_preference(
    config: GitHubActionPreferencesConfig,
    *,
    repository: str,
    policy_id: str,
    identity: GitHubPreferenceIdentity,
    identity_kind: str,
    allow_code_review_comments: bool | None,
    allow_self_fix: bool | None,
    updated_by_subject_id: str,
    self_fix_mode: str | None = None,
) -> dict[str, Any]:
    validated_self_fix_mode = _optional_self_fix_mode(self_fix_mode)
    record_id = preference_record_id(
        repository=repository,
        policy_id=policy_id,
        identity=identity,
        identity_kind=identity_kind,
    )
    now = _utcnow()
    existing = action_preference_store(config).get(record_id) or {}
    record = {
        "id": record_id,
        "schema_version": 2,
        "repository": repository,
        "policy_id": policy_id,
        "identity_kind": identity_kind,
        "external_identity_type": (
            identity.external_identity_type
            if identity_kind == IDENTITY_EXTERNAL_SUBJECT_ID
            else ""
        ),
        "external_subject_id": (
            identity.external_subject_id
            if identity_kind == IDENTITY_EXTERNAL_SUBJECT_ID
            else ""
        ),
        "subject_id": identity.subject_id
        if identity_kind == IDENTITY_SUBJECT_ID
        else "",
        "allow_code_review_comments": allow_code_review_comments,
        "allow_self_fix": allow_self_fix,
        "self_fix_mode": validated_self_fix_mode,
        "updated_by_subject_id": updated_by_subject_id,
        "created_at": str(existing.get("created_at") or now),
        "updated_at": now,
    }
    action_preference_store(config).put(record)
    return record


def get_action_preference(
    config: GitHubActionPreferencesConfig,
    *,
    repository: str,
    policy_id: str,
    identity: GitHubPreferenceIdentity,
    identity_kind: str,
) -> dict[str, Any] | None:
    return action_preference_store(config).get(
        preference_record_id(
            repository=repository,
            policy_id=policy_id,
            identity=identity,
            identity_kind=identity_kind,
        )
    )


def delete_action_preference(
    config: GitHubActionPreferencesConfig,
    *,
    repository: str,
    policy_id: str,
    identity: GitHubPreferenceIdentity,
    identity_kind: str,
) -> bool:
    return action_preference_store(config).delete(
        preference_record_id(
            repository=repository,
            policy_id=policy_id,
            identity=identity,
            identity_kind=identity_kind,
        )
    )


def preference_record_id(
    *,
    repository: str,
    policy_id: str,
    identity: GitHubPreferenceIdentity,
    identity_kind: str,
) -> str:
    repository = _required_text(repository, "repository")
    policy_id = _required_text(policy_id, "policyId")
    identity_kind = _required_text(identity_kind, "identityKind")
    if identity_kind == IDENTITY_EXTERNAL_SUBJECT_ID:
        if not identity.external_identity_type or not identity.external_subject_id:
            raise ValueError("external_subject_id identity is not available")
        return (
            f"v1/repo/{_b64(repository)}/policy/{_b64(policy_id)}/external/"
            f"{_b64(identity.external_identity_type)}/{_b64(identity.external_subject_id)}"
        )
    if identity_kind == IDENTITY_SUBJECT_ID:
        if not identity.subject_id:
            raise ValueError("subject_id identity is not available")
        return (
            f"v1/repo/{_b64(repository)}/policy/{_b64(policy_id)}/subject/"
            f"{_b64(identity.subject_id)}"
        )
    raise ValueError("identityKind must be external_subject_id or subject_id")


def normalize_identity_kind(value: str, identity: GitHubPreferenceIdentity) -> str:
    normalized = value.strip()
    if normalized:
        if normalized not in {IDENTITY_EXTERNAL_SUBJECT_ID, IDENTITY_SUBJECT_ID}:
            raise ValueError("identityKind must be external_subject_id or subject_id")
        return normalized
    if identity.external_identity_type and identity.external_subject_id:
        return IDENTITY_EXTERNAL_SUBJECT_ID
    if identity.subject_id:
        return IDENTITY_SUBJECT_ID
    raise ValueError("a GitHub external identity or human subject is required")


def _lookup_preference_record(
    config: GitHubActionPreferencesConfig,
    policy: GitHubWebhookPolicy,
    identity: GitHubPreferenceIdentity,
) -> tuple[dict[str, Any] | None, str]:
    store = action_preference_store(config)
    candidates: list[tuple[str, str]] = []
    if identity.external_subject_id:
        candidates.append(
            (
                preference_record_id(
                    repository=_repository_from_identity(identity),
                    policy_id=policy.id,
                    identity=identity,
                    identity_kind=IDENTITY_EXTERNAL_SUBJECT_ID,
                ),
                IDENTITY_EXTERNAL_SUBJECT_ID,
            )
        )
    if identity.subject_id:
        candidates.append(
            (
                preference_record_id(
                    repository=_repository_from_identity(identity),
                    policy_id=policy.id,
                    identity=identity,
                    identity_kind=IDENTITY_SUBJECT_ID,
                ),
                IDENTITY_SUBJECT_ID,
            )
        )
    for record_id, source in candidates:
        record = store.get(record_id)
        if record is not None:
            return record, source
    return None, ""


def _repository_from_identity(identity: GitHubPreferenceIdentity) -> str:
    if identity.repository.strip():
        return identity.repository.strip()
    raise ValueError("repository is required for preference lookup")


def _base_metadata(
    policy: GitHubWebhookPolicy, identity: GitHubPreferenceIdentity
) -> dict[str, Any]:
    return {
        "enabled": True,
        "source": "config_default",
        "identity": identity.metadata(),
        "static_ceiling": {
            "allow_code_review_comments": policy.allow_code_review_comments,
            "allow_self_fix": policy.allow_self_fix,
            "self_fix_mode": policy.self_fix_mode,
        },
        "effective": {
            "allow_code_review_comments": policy.allow_code_review_comments,
            "allow_self_fix": policy.allow_self_fix
            and policy.self_fix_mode != SELF_FIX_DISABLED,
            "self_fix_mode": policy.self_fix_mode
            if policy.allow_self_fix
            else SELF_FIX_DISABLED,
        },
    }


def _validated_preference_record(record: dict[str, Any]) -> dict[str, Any]:
    record_id = _required_text(record.get("id"), "id")
    schema_version = int(record.get("schema_version") or 0)
    if schema_version not in {1, 2}:
        raise ValueError(f"{record_id}: unsupported schema_version")
    for field_name in ("repository", "policy_id", "identity_kind"):
        _required_text(record.get(field_name), field_name)
    identity_kind = str(record.get("identity_kind") or "").strip()
    if identity_kind == IDENTITY_EXTERNAL_SUBJECT_ID:
        _required_text(record.get("external_identity_type"), "external_identity_type")
        _required_text(record.get("external_subject_id"), "external_subject_id")
    elif identity_kind == IDENTITY_SUBJECT_ID:
        _required_text(record.get("subject_id"), "subject_id")
    else:
        raise ValueError(f"{record_id}: invalid identity_kind")
    return {
        **record,
        "schema_version": schema_version,
        "allow_code_review_comments": _optional_bool(
            record.get("allow_code_review_comments"), "allow_code_review_comments"
        ),
        "allow_self_fix": _optional_bool(
            record.get("allow_self_fix"), "allow_self_fix"
        ),
        "self_fix_mode": _optional_self_fix_mode(record.get("self_fix_mode")),
    }


def _merge_gate(static_ceiling: bool, preferred: bool | None) -> bool:
    if not static_ceiling:
        return False
    if preferred is None:
        return True
    return preferred


def _merge_self_fix_mode(
    policy: GitHubWebhookPolicy, preferred_mode: str | None, preferred_bool: bool | None
) -> str:
    if not policy.allow_self_fix or policy.self_fix_mode == SELF_FIX_DISABLED:
        return SELF_FIX_DISABLED
    if preferred_bool is False:
        return SELF_FIX_DISABLED
    if preferred_mode is not None:
        return _bounded_self_fix_mode(policy.self_fix_mode, preferred_mode)
    return policy.self_fix_mode


def _bounded_self_fix_mode(static_mode: str, preferred_mode: str) -> str:
    static_rank = SELF_FIX_MODES.index(static_mode)
    preferred_rank = SELF_FIX_MODES.index(preferred_mode)
    return SELF_FIX_MODES[min(static_rank, preferred_rank)]


def _optional_bool(value: Any, field_name: str) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    raise ValueError(f"{field_name} must be a boolean or null")


def _optional_self_fix_mode(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and value in SELF_FIX_MODES:
        return value
    raise ValueError(
        "self_fix_mode must be disabled, suggest, branch_commit, pull_request, or null"
    )


def _required_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    return text


def _b64(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii").rstrip("=")


def _utcnow() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
