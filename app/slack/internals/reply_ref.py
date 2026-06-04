from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import time
from typing import Any

from .models import (
    DIRECT_MESSAGE_CHANNEL_TYPES,
    SlackAgentEvent,
    SlackAgentRoute,
    SlackInteractionRef,
    SlackReplyRef,
)


def sign_reply_ref(
    event: SlackAgentEvent,
    subject_id: str,
    route: SlackAgentRoute | None = None,
    *,
    signing_key: bytes,
    ttl_seconds: int,
) -> str:
    payload = {
        "v": 1,
        "team_id": event.team_id,
        "channel_id": event.channel_id,
        "user_id": event.user_id,
        "channel_type": event.channel_type,
        "message_ts": event.message_ts,
        "reply_thread_ts": event.reply_thread_ts,
        "event_id": event.event_id,
        "client_msg_id": event.client_msg_id,
        "subject_id": subject_id,
        "route_id": route.id if route is not None else "",
        "workflow_key": agent_session_ref(event),
        "expires_at": int(time.time()) + ttl_seconds,
    }
    return _sign_payload(payload, signing_key=signing_key)


def verify_reply_ref(
    reply_ref: str, subject_id: str, *, signing_key: bytes
) -> SlackReplyRef:
    ref = decode_reply_ref(reply_ref, signing_key=signing_key)
    if ref.subject_id != subject_id:
        raise ValueError("reply_ref does not belong to this subject")
    return ref


def decode_reply_ref(reply_ref: str, *, signing_key: bytes) -> SlackReplyRef:
    payload = _decode_signed_payload(
        reply_ref,
        signing_key=signing_key,
        invalid_error="invalid reply_ref",
    )
    ref = reply_ref_from_payload(payload)
    if ref.expires_at < int(time.time()):
        raise ValueError("reply_ref expired")
    return ref


def reply_ref_from_payload(payload: dict[str, Any]) -> SlackReplyRef:
    if payload.get("v") != 1:
        raise ValueError("invalid reply_ref")
    try:
        expires_at = int(payload.get("expires_at") or 0)
    except (TypeError, ValueError) as err:
        raise ValueError("invalid reply_ref") from err

    ref = SlackReplyRef(
        team_id=str(payload.get("team_id") or "").strip(),
        channel_id=str(payload.get("channel_id") or "").strip(),
        message_ts=str(payload.get("message_ts") or "").strip(),
        reply_thread_ts=str(payload.get("reply_thread_ts") or "").strip(),
        event_id=str(payload.get("event_id") or "").strip(),
        subject_id=str(payload.get("subject_id") or "").strip(),
        expires_at=expires_at,
        user_id=str(payload.get("user_id") or "").strip(),
        channel_type=str(payload.get("channel_type") or "").strip(),
        route_id=str(payload.get("route_id") or "").strip(),
        client_msg_id=str(payload.get("client_msg_id") or "").strip(),
        workflow_key=str(payload.get("workflow_key") or "").strip(),
    )
    if not ref.team_id or not ref.channel_id or not ref.subject_id:
        raise ValueError("invalid reply_ref")
    return ref


def reply_ref_workflow_key(ref: SlackReplyRef) -> str:
    if ref.workflow_key:
        return ref.workflow_key
    if ref.channel_type in DIRECT_MESSAGE_CHANNEL_TYPES and not ref.reply_thread_ts:
        return f"slack:{ref.team_id}:{ref.channel_id}"
    root_ts = ref.reply_thread_ts or ref.message_ts
    return f"slack:{ref.team_id}:{ref.channel_id}:{root_ts}"


def agent_session_ref(event: SlackAgentEvent) -> str:
    if event.channel_type in DIRECT_MESSAGE_CHANNEL_TYPES and not event.thread_ts:
        return f"slack:{event.team_id}:{event.channel_id}"
    root_ts = event.thread_ts or event.message_ts
    return f"slack:{event.team_id}:{event.channel_id}:{root_ts}"


def sign_interaction_ref(
    ref: SlackReplyRef,
    *,
    action_id: str,
    action_value: str,
    expires_in_seconds: int,
    signing_key: bytes,
) -> str:
    expires_at = int(time.time()) + expires_in_seconds
    payload = {
        "v": 1,
        "team_id": ref.team_id,
        "channel_id": ref.channel_id,
        "channel_type": ref.channel_type,
        "message_ts": ref.message_ts,
        "reply_thread_ts": ref.reply_thread_ts,
        "workflow_key": reply_ref_workflow_key(ref),
        "reply_ref": resign_reply_ref(
            ref, expires_at=expires_at, signing_key=signing_key
        ),
        "subject_id": ref.subject_id,
        "user_id": ref.user_id,
        "route_id": ref.route_id,
        "action_id": action_id,
        "action_value": action_value,
        "expires_at": expires_at,
    }
    return _sign_payload(payload, signing_key=signing_key)


def verify_interaction_ref(
    interaction_ref: str, subject_id: str, *, signing_key: bytes
) -> SlackInteractionRef:
    ref = decode_interaction_ref(interaction_ref, signing_key=signing_key)
    if ref.subject_id != subject_id:
        raise ValueError("interaction_ref does not belong to this subject")
    return ref


def decode_interaction_ref(
    interaction_ref: str, *, signing_key: bytes
) -> SlackInteractionRef:
    payload = _decode_signed_payload(
        interaction_ref,
        signing_key=signing_key,
        invalid_error="invalid interaction_ref",
    )
    if payload.get("v") != 1:
        raise ValueError("invalid interaction_ref")
    try:
        expires_at = int(payload.get("expires_at") or 0)
    except (TypeError, ValueError) as err:
        raise ValueError("invalid interaction_ref") from err
    ref = SlackInteractionRef(
        team_id=str(payload.get("team_id") or "").strip(),
        channel_id=str(payload.get("channel_id") or "").strip(),
        channel_type=str(payload.get("channel_type") or "").strip(),
        message_ts=str(payload.get("message_ts") or "").strip(),
        reply_thread_ts=str(payload.get("reply_thread_ts") or "").strip(),
        workflow_key=str(payload.get("workflow_key") or "").strip(),
        reply_ref=str(payload.get("reply_ref") or "").strip(),
        subject_id=str(payload.get("subject_id") or "").strip(),
        user_id=str(payload.get("user_id") or "").strip(),
        route_id=str(payload.get("route_id") or "").strip(),
        action_id=str(payload.get("action_id") or "").strip(),
        action_value=str(payload.get("action_value") or "").strip(),
        expires_at=expires_at,
    )
    if (
        not ref.team_id
        or not ref.channel_id
        or not ref.workflow_key
        or not ref.reply_ref
        or not ref.subject_id
        or not ref.action_id
    ):
        raise ValueError("invalid interaction_ref")
    if ref.expires_at < int(time.time()):
        raise ValueError("interaction_ref expired")
    verify_reply_ref(ref.reply_ref, ref.subject_id, signing_key=signing_key)
    return ref


def resign_reply_ref(
    ref: SlackReplyRef, *, expires_at: int, signing_key: bytes
) -> str:
    payload = {
        "v": 1,
        "team_id": ref.team_id,
        "channel_id": ref.channel_id,
        "user_id": ref.user_id,
        "channel_type": ref.channel_type,
        "message_ts": ref.message_ts,
        "reply_thread_ts": ref.reply_thread_ts,
        "event_id": ref.event_id,
        "client_msg_id": ref.client_msg_id,
        "subject_id": ref.subject_id,
        "route_id": ref.route_id,
        "workflow_key": reply_ref_workflow_key(ref),
        "expires_at": expires_at,
    }
    return _sign_payload(payload, signing_key=signing_key)


def _sign_payload(payload: dict[str, Any], *, signing_key: bytes) -> str:
    encoded_payload = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    signature = hmac.new(signing_key, encoded_payload, hashlib.sha256).digest()
    return f"{_base64url_encode(encoded_payload)}.{_base64url_encode(signature)}"


def _decode_signed_payload(
    value: str, *, signing_key: bytes, invalid_error: str
) -> dict[str, Any]:
    payload_part, separator, signature_part = value.strip().partition(".")
    if not separator:
        raise ValueError(invalid_error)

    try:
        encoded_payload = _base64url_decode(payload_part)
        signature = _base64url_decode(signature_part)
    except (binascii.Error, ValueError) as err:
        raise ValueError(invalid_error) from err

    expected_signature = hmac.new(signing_key, encoded_payload, hashlib.sha256).digest()
    if not hmac.compare_digest(signature, expected_signature):
        raise ValueError(invalid_error)

    try:
        payload = json.loads(encoded_payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as err:
        raise ValueError(invalid_error) from err
    if not isinstance(payload, dict):
        raise ValueError(invalid_error)
    return payload


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _base64url_decode(value: str) -> bytes:
    if not value:
        raise ValueError("empty base64url value")
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)
