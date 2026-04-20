from __future__ import annotations

from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt

from internals import create_draft, forward_message, reply_message, send_draft, send_message, update_draft

ErrorResponse: TypeAlias = gestalt.Response[dict[str, str]]
OperationResult: TypeAlias = dict[str, Any] | ErrorResponse


class SendMessageInput(gestalt.Model):
    to: str = gestalt.field(description="Recipient email address")
    subject: str = gestalt.field(description="Email subject")
    body: str = gestalt.field(description="Plain text body")
    cc: str = gestalt.field(description="CC recipients (comma-separated)", default="", required=False)
    bcc: str = gestalt.field(description="BCC recipients (comma-separated)", default="", required=False)
    html_body: str = gestalt.field(
        description="HTML body (sent as alternative to plain text)",
        default="",
        required=False,
    )


class CreateDraftInput(gestalt.Model):
    to: str = gestalt.field(description="Recipient email address")
    subject: str = gestalt.field(description="Email subject")
    body: str = gestalt.field(description="Plain text body")
    cc: str = gestalt.field(description="CC recipients (comma-separated)", default="", required=False)
    bcc: str = gestalt.field(description="BCC recipients (comma-separated)", default="", required=False)
    html_body: str = gestalt.field(description="HTML body", default="", required=False)


class UpdateDraftInput(gestalt.Model):
    id: str = gestalt.field(description="Draft ID")
    to: str = gestalt.field(description="Recipient email address")
    subject: str = gestalt.field(description="Email subject")
    body: str = gestalt.field(description="Plain text body")
    cc: str = gestalt.field(description="CC recipients (comma-separated)", default="", required=False)
    bcc: str = gestalt.field(description="BCC recipients (comma-separated)", default="", required=False)
    html_body: str = gestalt.field(description="HTML body", default="", required=False)


class SendDraftInput(gestalt.Model):
    id: str = gestalt.field(description="Draft ID")


class ReplyMessageInput(gestalt.Model):
    message_id: str = gestalt.field(description="Original message ID")
    body: str = gestalt.field(description="Reply body")
    cc: str = gestalt.field(description="CC recipients (comma-separated)", default="", required=False)
    reply_all: bool = gestalt.field(description="Reply to all recipients", default=False, required=False)
    html_body: str = gestalt.field(description="HTML body", default="", required=False)


class ForwardMessageInput(gestalt.Model):
    message_id: str = gestalt.field(description="Message to forward")
    to: str = gestalt.field(description="Forward recipient")
    additional_text: str = gestalt.field(
        description="Text to prepend to forwarded content",
        default="",
        required=False,
    )
    cc: str = gestalt.field(description="CC recipients (comma-separated)", default="", required=False)


@gestalt.operation(
    id="messages.send",
    method="POST",
    description="Send an email message",
)
def messages_send(input: SendMessageInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.to or not input.subject or not input.body:
        return _bad_request("to, subject, and body are required")

    try:
        return send_message(req.token, input.to, input.subject, input.body, input.cc, input.bcc, input.html_body)
    except RuntimeError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="drafts.create",
    method="POST",
    description="Create an email draft",
)
def drafts_create(input: CreateDraftInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.to or not input.subject or not input.body:
        return _bad_request("to, subject, and body are required")

    try:
        return create_draft(req.token, input.to, input.subject, input.body, input.cc, input.bcc, input.html_body)
    except RuntimeError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="drafts.update",
    method="PUT",
    description="Update an email draft",
)
def drafts_update(input: UpdateDraftInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.id or not input.to or not input.subject or not input.body:
        return _bad_request("id, to, subject, and body are required")

    try:
        return update_draft(req.token, input.id, input.to, input.subject, input.body, input.cc, input.bcc, input.html_body)
    except RuntimeError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="drafts.send",
    method="POST",
    description="Send an existing email draft",
)
def drafts_send(input: SendDraftInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.id:
        return _bad_request("id is required")

    try:
        return send_draft(req.token, input.id)
    except RuntimeError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="messages.reply",
    method="POST",
    description="Reply to an existing message",
)
def messages_reply(input: ReplyMessageInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.message_id or not input.body:
        return _bad_request("message_id and body are required")

    try:
        return reply_message(req.token, input.message_id, input.body, input.cc, input.reply_all, input.html_body)
    except RuntimeError as err:
        return _server_error(str(err))


@gestalt.operation(
    id="messages.forward",
    method="POST",
    description="Forward a message to new recipients",
)
def messages_forward(input: ForwardMessageInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.message_id or not input.to:
        return _bad_request("message_id and to are required")

    try:
        return forward_message(req.token, input.message_id, input.to, input.additional_text, input.cc)
    except RuntimeError as err:
        return _server_error(str(err))


def _validate_token(req: gestalt.Request) -> ErrorResponse | None:
    if not req.token.strip():
        return gestalt.Response(status=HTTPStatus.UNAUTHORIZED, body={"error": "token is required"})
    return None


def _bad_request(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})


def _server_error(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": message})
