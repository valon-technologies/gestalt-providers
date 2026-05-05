from __future__ import annotations

from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt

import internals.client as client_module
from internals.client import GmailAPIError, GmailClientError
from internals.mime import MIMEParams
from internals.operations import (
    GmailForwardRequest,
    GmailReplyRequest,
    create_draft,
    forward_message,
    reply_message,
    send_draft,
    send_message,
    update_draft,
)
from internals.platform_identity import (
    PlatformIdentityConfig,
    PlatformIdentityError,
    parse_platform_identity_config,
    platform_token_for_operation,
)

ErrorResponse: TypeAlias = gestalt.Response[dict[str, str]]
OperationResult: TypeAlias = dict[str, Any] | ErrorResponse

plugin = gestalt.Plugin("gmail")
_platform_identity_config = PlatformIdentityConfig()


@plugin.configure
def configure(_name: str, config: dict[str, Any]) -> None:
    global _platform_identity_config
    _platform_identity_config = parse_platform_identity_config(config)


class MessagesListInput(gestalt.Model):
    q: str = gestalt.field(description="Gmail search query", default="", required=False)
    labelIds: list[str] = gestalt.field(
        description="Label IDs to restrict the search to",
        default_factory=list,
        required=False,
    )
    maxResults: int | None = gestalt.field(
        description="Maximum number of messages to return",
        default=None,
        required=False,
    )
    pageToken: str = gestalt.field(
        description="Page token returned by a previous list call",
        default="",
        required=False,
    )
    includeSpamTrash: bool = gestalt.field(
        description="Include messages from SPAM and TRASH",
        default=False,
        required=False,
    )
    fields: str = gestalt.field(
        description="Partial response fields selector",
        default="",
        required=False,
    )


class MessageGetInput(gestalt.Model):
    id: str = gestalt.field(description="Message ID")
    format: str = gestalt.field(
        description="Message format: minimal, full, raw, or metadata",
        default="",
        required=False,
    )
    metadataHeaders: list[str] = gestalt.field(
        description="Metadata headers to include when format is metadata",
        default_factory=list,
        required=False,
    )
    fields: str = gestalt.field(
        description="Partial response fields selector",
        default="",
        required=False,
    )


class ThreadGetInput(gestalt.Model):
    id: str = gestalt.field(description="Thread ID")
    format: str = gestalt.field(
        description="Message format for messages in the thread",
        default="",
        required=False,
    )
    metadataHeaders: list[str] = gestalt.field(
        description="Metadata headers to include when format is metadata",
        default_factory=list,
        required=False,
    )
    fields: str = gestalt.field(
        description="Partial response fields selector",
        default="",
        required=False,
    )


class CommonFieldsInput(gestalt.Model):
    fields: str = gestalt.field(
        description="Partial response fields selector",
        default="",
        required=False,
    )


class SendMessageInput(gestalt.Model):
    to: str = gestalt.field(description="Recipient email address")
    subject: str = gestalt.field(description="Email subject")
    body: str = gestalt.field(description="Plain text body")
    cc: str = gestalt.field(
        description="CC recipients (comma-separated)", default="", required=False
    )
    bcc: str = gestalt.field(
        description="BCC recipients (comma-separated)", default="", required=False
    )
    html_body: str = gestalt.field(
        description="HTML body (sent as alternative to plain text)",
        default="",
        required=False,
    )


class CreateDraftInput(gestalt.Model):
    to: str = gestalt.field(description="Recipient email address")
    subject: str = gestalt.field(description="Email subject")
    body: str = gestalt.field(description="Plain text body")
    cc: str = gestalt.field(
        description="CC recipients (comma-separated)", default="", required=False
    )
    bcc: str = gestalt.field(
        description="BCC recipients (comma-separated)", default="", required=False
    )
    html_body: str = gestalt.field(description="HTML body", default="", required=False)


class UpdateDraftInput(gestalt.Model):
    id: str = gestalt.field(description="Draft ID")
    to: str = gestalt.field(description="Recipient email address")
    subject: str = gestalt.field(description="Email subject")
    body: str = gestalt.field(description="Plain text body")
    cc: str = gestalt.field(
        description="CC recipients (comma-separated)", default="", required=False
    )
    bcc: str = gestalt.field(
        description="BCC recipients (comma-separated)", default="", required=False
    )
    html_body: str = gestalt.field(description="HTML body", default="", required=False)


class SendDraftInput(gestalt.Model):
    id: str = gestalt.field(description="Draft ID")


class ReplyMessageInput(gestalt.Model):
    message_id: str = gestalt.field(description="Original message ID")
    body: str = gestalt.field(description="Reply body")
    cc: str = gestalt.field(
        description="CC recipients (comma-separated)", default="", required=False
    )
    reply_all: bool = gestalt.field(
        description="Reply to all recipients", default=False, required=False
    )
    html_body: str = gestalt.field(description="HTML body", default="", required=False)


class ForwardMessageInput(gestalt.Model):
    message_id: str = gestalt.field(description="Message to forward")
    to: str = gestalt.field(description="Forward recipient")
    additional_text: str = gestalt.field(
        description="Text to prepend to forwarded content",
        default="",
        required=False,
    )
    cc: str = gestalt.field(
        description="CC recipients (comma-separated)", default="", required=False
    )


@plugin.operation(
    id="messages.list",
    method="GET",
    description="List Gmail messages",
    tags=["email", "mail"],
    read_only=True,
)
def messages_list(input: MessagesListInput, req: gestalt.Request) -> OperationResult:
    token = _read_token("messages.list", req)
    if isinstance(token, gestalt.Response):
        return token
    try:
        return client_module.get_json(
            client_module.messages_list_url(
                q=input.q,
                label_ids=input.labelIds,
                max_results=input.maxResults,
                page_token=input.pageToken,
                include_spam_trash=input.includeSpamTrash,
                fields=input.fields,
            ),
            token,
        )
    except GmailAPIError as err:
        return gestalt.Response(status=err.status, body=err.raw_body)
    except GmailClientError as err:
        return _server_error(str(err))


@plugin.operation(
    id="messages.get",
    method="GET",
    description="Get a Gmail message",
    tags=["email", "mail"],
    read_only=True,
)
def messages_get(input: MessageGetInput, req: gestalt.Request) -> OperationResult:
    if not input.id:
        return _bad_request("id is required")
    token = _read_token("messages.get", req)
    if isinstance(token, gestalt.Response):
        return token
    try:
        return client_module.get_json(
            client_module.message_url(
                input.id,
                format=input.format,
                metadata_headers=input.metadataHeaders,
                fields=input.fields,
            ),
            token,
        )
    except GmailAPIError as err:
        return gestalt.Response(status=err.status, body=err.raw_body)
    except GmailClientError as err:
        return _server_error(str(err))


@plugin.operation(
    id="threads.get",
    method="GET",
    description="Get a Gmail thread",
    tags=["email", "mail"],
    read_only=True,
)
def threads_get(input: ThreadGetInput, req: gestalt.Request) -> OperationResult:
    if not input.id:
        return _bad_request("id is required")
    token = _read_token("threads.get", req)
    if isinstance(token, gestalt.Response):
        return token
    try:
        return client_module.get_json(
            client_module.thread_url(
                input.id,
                format=input.format,
                metadata_headers=input.metadataHeaders,
                fields=input.fields,
            ),
            token,
        )
    except GmailAPIError as err:
        return gestalt.Response(status=err.status, body=err.raw_body)
    except GmailClientError as err:
        return _server_error(str(err))


@plugin.operation(
    id="labels.list",
    method="GET",
    description="List Gmail labels",
    tags=["email", "mail"],
    read_only=True,
)
def labels_list(input: CommonFieldsInput, req: gestalt.Request) -> OperationResult:
    token = _read_token("labels.list", req)
    if isinstance(token, gestalt.Response):
        return token
    try:
        return client_module.get_json(
            client_module.labels_url(fields=input.fields), token
        )
    except GmailAPIError as err:
        return gestalt.Response(status=err.status, body=err.raw_body)
    except GmailClientError as err:
        return _server_error(str(err))


@plugin.operation(
    id="getProfile",
    method="GET",
    description="Get the Gmail profile",
    tags=["email", "mail"],
    read_only=True,
)
def get_profile(input: CommonFieldsInput, req: gestalt.Request) -> OperationResult:
    token = _read_token("getProfile", req)
    if isinstance(token, gestalt.Response):
        return token
    try:
        return client_module.get_json(
            client_module.profile_url(fields=input.fields), token
        )
    except GmailAPIError as err:
        return gestalt.Response(status=err.status, body=err.raw_body)
    except GmailClientError as err:
        return _server_error(str(err))


@plugin.operation(
    id="messages.send",
    method="POST",
    description="Send an email message",
    tags=["email", "mail"],
)
def messages_send(input: SendMessageInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.to or not input.subject or not input.body:
        return _bad_request("to, subject, and body are required")

    try:
        return send_message(req.token, _mime_params_from_input(input))
    except GmailAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except GmailClientError as err:
        return _server_error(str(err))


@plugin.operation(
    id="drafts.create",
    method="POST",
    description="Create an email draft",
    tags=["email", "mail"],
)
def drafts_create(input: CreateDraftInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.to or not input.subject or not input.body:
        return _bad_request("to, subject, and body are required")

    try:
        return create_draft(req.token, _mime_params_from_input(input))
    except GmailAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except GmailClientError as err:
        return _server_error(str(err))


@plugin.operation(
    id="drafts.update",
    method="PUT",
    description="Update an email draft",
    tags=["email", "mail"],
)
def drafts_update(input: UpdateDraftInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.id or not input.to or not input.subject or not input.body:
        return _bad_request("id, to, subject, and body are required")

    try:
        return update_draft(req.token, input.id, _mime_params_from_input(input))
    except GmailAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except GmailClientError as err:
        return _server_error(str(err))


@plugin.operation(
    id="drafts.send",
    method="POST",
    description="Send an existing email draft",
    tags=["email", "mail"],
)
def drafts_send(input: SendDraftInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.id:
        return _bad_request("id is required")

    try:
        return send_draft(req.token, input.id)
    except GmailAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except GmailClientError as err:
        return _server_error(str(err))


@plugin.operation(
    id="messages.reply",
    method="POST",
    description="Reply to an existing message",
    tags=["email", "mail"],
)
def messages_reply(input: ReplyMessageInput, req: gestalt.Request) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.message_id or not input.body:
        return _bad_request("message_id and body are required")

    try:
        return reply_message(
            req.token,
            GmailReplyRequest(
                message_id=input.message_id,
                body=input.body,
                cc=input.cc,
                reply_all=input.reply_all,
                html_body=input.html_body,
            ),
        )
    except GmailAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except GmailClientError as err:
        return _server_error(str(err))


@plugin.operation(
    id="messages.forward",
    method="POST",
    description="Forward a message to new recipients",
    tags=["email", "mail"],
)
def messages_forward(
    input: ForwardMessageInput, req: gestalt.Request
) -> OperationResult:
    token_error = _validate_token(req)
    if token_error is not None:
        return token_error
    if not input.message_id or not input.to:
        return _bad_request("message_id and to are required")

    try:
        return forward_message(
            req.token,
            GmailForwardRequest(
                message_id=input.message_id,
                to=input.to,
                additional_text=input.additional_text,
                cc=input.cc,
            ),
        )
    except GmailAPIError as err:
        return gestalt.Response(status=err.status, body=err.body)
    except GmailClientError as err:
        return _server_error(str(err))


def _validate_token(req: gestalt.Request) -> ErrorResponse | None:
    if not req.token.strip():
        return gestalt.Response(
            status=HTTPStatus.UNAUTHORIZED, body={"error": "token is required"}
        )
    return None


def _read_token(operation: str, req: gestalt.Request) -> str | ErrorResponse:
    mode = str(req.credential.mode or "").strip()
    if mode == "none":
        try:
            return platform_token_for_operation(_platform_identity_config, operation)
        except PlatformIdentityError as err:
            return _server_error(str(err))
    if mode == "platform":
        return gestalt.Response(
            status=HTTPStatus.FORBIDDEN,
            body={"error": "platform Gmail reads require credentialMode none"},
        )

    token = req.token.strip()
    if not token:
        return gestalt.Response(
            status=HTTPStatus.UNAUTHORIZED, body={"error": "token is required"}
        )
    return token


def _mime_params_from_input(
    input: SendMessageInput | CreateDraftInput | UpdateDraftInput,
) -> MIMEParams:
    return MIMEParams(
        to=input.to,
        subject=input.subject,
        body=input.body,
        cc=input.cc,
        bcc=input.bcc,
        html_body=input.html_body,
    )


def _bad_request(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})


def _server_error(message: str) -> ErrorResponse:
    return gestalt.Response(
        status=HTTPStatus.INTERNAL_SERVER_ERROR, body={"error": message}
    )
