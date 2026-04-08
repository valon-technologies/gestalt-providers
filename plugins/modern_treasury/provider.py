from __future__ import annotations

import json
from http import HTTPStatus
from typing import Any, TypeAlias

import gestalt

from internals import ModernTreasuryAPIError, ModernTreasuryClient

ErrorResponse: TypeAlias = gestalt.Response[dict[str, Any]]
OperationResult: TypeAlias = dict[str, Any] | list[Any] | ErrorResponse


class ListInput(gestalt.Model):
    after_cursor: str | None = gestalt.field(
        description="Pagination cursor returned by a previous Modern Treasury list request.",
        default=None,
        required=False,
    )
    per_page: int | None = gestalt.field(
        description="Maximum number of records to return.",
        default=None,
        required=False,
    )
    query_json: str = gestalt.field(
        description="Optional JSON object of additional official query parameters to pass through unchanged.",
        default="",
        required=False,
    )


class GetByIdInput(gestalt.Model):
    id: str = gestalt.field(description="Modern Treasury object ID.")


class ExternalAccountCreateInput(gestalt.Model):
    counterparty_id: str = gestalt.field(description="Counterparty ID that will own the external account.")
    idempotency_key: str = gestalt.field(description="Idempotency key for the create request.")
    name: str = gestalt.field(description="Display name for the external account.", default="", required=False)
    account_type: str = gestalt.field(
        description="Depository account type, for example checking or savings.",
        default="",
        required=False,
    )
    party_name: str = gestalt.field(description="Name of the account holder.", default="", required=False)
    party_type: str = gestalt.field(description="Counterparty party type.", default="", required=False)
    account_number: str = gestalt.field(
        description="Bank account number. Required unless plaid_processor_token is supplied.",
        default="",
        required=False,
    )
    account_number_type: str = gestalt.field(
        description="Modern Treasury account number type, usually other.",
        default="other",
        required=False,
    )
    routing_number: str = gestalt.field(
        description="Routing number. Required unless plaid_processor_token is supplied.",
        default="",
        required=False,
    )
    routing_number_type: str = gestalt.field(
        description="Routing number type such as aba or swift.",
        default="",
        required=False,
    )
    payment_type: str = gestalt.field(
        description="Optional payment rail hint for the routing detail.",
        default="",
        required=False,
    )
    plaid_processor_token: str = gestalt.field(
        description="Plaid processor token used instead of direct account and routing details.",
        default="",
        required=False,
    )
    external_id: str = gestalt.field(description="Optional external ID.", default="", required=False)
    metadata_json: str = gestalt.field(
        description="Optional JSON object of metadata to attach to the external account.",
        default="",
        required=False,
    )
    party_address_json: str = gestalt.field(
        description="Optional JSON object matching Modern Treasury's party_address shape.",
        default="",
        required=False,
    )
    payload_json: str = gestalt.field(
        description="Optional full JSON payload. When provided, it is sent as-is and other fields are ignored.",
        default="",
        required=False,
    )


class PaymentOrderCreateInput(gestalt.Model):
    idempotency_key: str = gestalt.field(description="Idempotency key for the create request.")
    amount: int = gestalt.field(description="Amount in the currency's smallest unit, for example cents.")
    direction: str = gestalt.field(description="Payment direction, such as credit or debit.")
    originating_account_id: str = gestalt.field(description="Originating internal account ID.")
    type: str = gestalt.field(description="Payment order type, such as ach, wire, or check.")
    currency: str = gestalt.field(description="ISO currency code.", default="USD", required=False)
    receiving_account_id: str = gestalt.field(
        description="Receiving account ID.",
        default="",
        required=False,
    )
    description: str = gestalt.field(description="Payment description.", default="", required=False)
    effective_date: str = gestalt.field(
        description="Effective date in YYYY-MM-DD format.",
        default="",
        required=False,
    )
    process_after: str = gestalt.field(
        description="Process-after timestamp in ISO-8601 format.",
        default="",
        required=False,
    )
    metadata_json: str = gestalt.field(
        description="Optional JSON object of payment metadata.",
        default="",
        required=False,
    )
    remittance_information: str = gestalt.field(
        description="Optional remittance information for ACH and wire payments.",
        default="",
        required=False,
    )
    statement_descriptor: str = gestalt.field(
        description="Optional statement descriptor for check payments.",
        default="",
        required=False,
    )
    external_id: str = gestalt.field(description="Optional external ID.", default="", required=False)
    priority: str = gestalt.field(description="Optional payment priority.", default="", required=False)
    ledger_transaction_id: str = gestalt.field(
        description="Optional ledger transaction ID.",
        default="",
        required=False,
    )
    payload_json: str = gestalt.field(
        description="Optional full JSON payload. When provided, it is sent as-is and other fields are ignored.",
        default="",
        required=False,
    )


class CancelPaymentOrderInput(gestalt.Model):
    id: str = gestalt.field(description="Payment order ID.")


class ValidateRoutingNumberInput(gestalt.Model):
    routing_number: str = gestalt.field(description="Routing number to validate.")
    routing_number_type: str = gestalt.field(description="Routing number type such as aba.")


@gestalt.operation(
    id="counterparties.list",
    method="GET",
    description="List Modern Treasury counterparties.",
    read_only=True,
)
def counterparties_list(input: ListInput, req: gestalt.Request) -> OperationResult:
    return _list_resource(req, path="/counterparties", input=input)


@gestalt.operation(
    id="externalAccounts.list",
    method="GET",
    description="List Modern Treasury external accounts.",
    read_only=True,
)
def external_accounts_list(input: ListInput, req: gestalt.Request) -> OperationResult:
    return _list_resource(req, path="/external_accounts", input=input)


@gestalt.operation(
    id="externalAccounts.get",
    method="GET",
    description="Get a single Modern Treasury external account.",
    read_only=True,
)
def external_accounts_get(input: GetByIdInput, req: gestalt.Request) -> OperationResult:
    if not input.id.strip():
        return _bad_request("id is required")
    return _request(req, method="GET", path=f"/external_accounts/{input.id.strip()}")


@gestalt.operation(
    id="externalAccounts.create",
    method="POST",
    description="Create a Modern Treasury external account.",
)
def external_accounts_create(input: ExternalAccountCreateInput, req: gestalt.Request) -> OperationResult:
    payload_result = _build_external_account_payload(input)
    if isinstance(payload_result, gestalt.Response):
        return payload_result
    return _request(
        req,
        method="POST",
        path="/external_accounts",
        body=payload_result,
        idempotency_key=input.idempotency_key,
    )


@gestalt.operation(
    id="paymentOrders.list",
    method="GET",
    description="List Modern Treasury payment orders.",
    read_only=True,
)
def payment_orders_list(input: ListInput, req: gestalt.Request) -> OperationResult:
    return _list_resource(req, path="/payment_orders", input=input)


@gestalt.operation(
    id="paymentOrders.get",
    method="GET",
    description="Get a single Modern Treasury payment order.",
    read_only=True,
)
def payment_orders_get(input: GetByIdInput, req: gestalt.Request) -> OperationResult:
    if not input.id.strip():
        return _bad_request("id is required")
    return _request(req, method="GET", path=f"/payment_orders/{input.id.strip()}")


@gestalt.operation(
    id="paymentOrders.create",
    method="POST",
    description="Create a Modern Treasury payment order.",
)
def payment_orders_create(input: PaymentOrderCreateInput, req: gestalt.Request) -> OperationResult:
    payload_result = _build_payment_order_payload(input)
    if isinstance(payload_result, gestalt.Response):
        return payload_result
    return _request(
        req,
        method="POST",
        path="/payment_orders",
        body=payload_result,
        idempotency_key=input.idempotency_key,
    )


@gestalt.operation(
    id="paymentOrders.cancel",
    method="PATCH",
    description="Cancel a Modern Treasury payment order by setting its status to cancelled.",
)
def payment_orders_cancel(input: CancelPaymentOrderInput, req: gestalt.Request) -> OperationResult:
    payment_order_id = input.id.strip()
    if not payment_order_id:
        return _bad_request("id is required")
    return _request(
        req,
        method="PATCH",
        path=f"/payment_orders/{payment_order_id}",
        body={"status": "cancelled"},
        idempotency_key=f"cancel-{payment_order_id}",
    )


@gestalt.operation(
    id="transactions.list",
    method="GET",
    description="List Modern Treasury transactions.",
    read_only=True,
)
def transactions_list(input: ListInput, req: gestalt.Request) -> OperationResult:
    return _list_resource(req, path="/transactions", input=input)


@gestalt.operation(
    id="transactions.get",
    method="GET",
    description="Get a single Modern Treasury transaction.",
    read_only=True,
)
def transactions_get(input: GetByIdInput, req: gestalt.Request) -> OperationResult:
    if not input.id.strip():
        return _bad_request("id is required")
    return _request(req, method="GET", path=f"/transactions/{input.id.strip()}")


@gestalt.operation(
    id="returns.list",
    method="GET",
    description="List Modern Treasury returns.",
    read_only=True,
)
def returns_list(input: ListInput, req: gestalt.Request) -> OperationResult:
    return _list_resource(req, path="/returns", input=input)


@gestalt.operation(
    id="incomingPaymentDetails.list",
    method="GET",
    description="List Modern Treasury incoming payment details.",
    read_only=True,
)
def incoming_payment_details_list(input: ListInput, req: gestalt.Request) -> OperationResult:
    return _list_resource(req, path="/incoming_payment_details", input=input)


@gestalt.operation(
    id="incomingPaymentDetails.get",
    method="GET",
    description="Get a single Modern Treasury incoming payment detail.",
    read_only=True,
)
def incoming_payment_details_get(input: GetByIdInput, req: gestalt.Request) -> OperationResult:
    if not input.id.strip():
        return _bad_request("id is required")
    return _request(req, method="GET", path=f"/incoming_payment_details/{input.id.strip()}")


@gestalt.operation(
    id="balanceReports.list",
    method="GET",
    description="List Modern Treasury balance reports.",
    read_only=True,
)
def balance_reports_list(input: ListInput, req: gestalt.Request) -> OperationResult:
    return _list_resource(req, path="/balance_reports", input=input)


@gestalt.operation(
    id="routingNumbers.validate",
    method="GET",
    description="Validate a routing number through Modern Treasury.",
    read_only=True,
)
def routing_numbers_validate(input: ValidateRoutingNumberInput, req: gestalt.Request) -> OperationResult:
    if not input.routing_number.strip():
        return _bad_request("routing_number is required")
    if not input.routing_number_type.strip():
        return _bad_request("routing_number_type is required")
    return _request(
        req,
        method="GET",
        path="/validations/routing_numbers",
        query={
            "routing_number": input.routing_number.strip(),
            "routing_number_type": input.routing_number_type.strip(),
        },
    )


def _list_resource(req: gestalt.Request, *, path: str, input: ListInput) -> OperationResult:
    query_result = _parse_optional_json_object("query_json", input.query_json)
    if isinstance(query_result, gestalt.Response):
        return query_result
    query = query_result
    if input.after_cursor:
        query["after_cursor"] = input.after_cursor.strip()
    if input.per_page is not None:
        query["per_page"] = input.per_page
    return _request(req, method="GET", path=path, query=query)


def _build_external_account_payload(input: ExternalAccountCreateInput) -> dict[str, Any] | ErrorResponse:
    if input.payload_json.strip():
        payload_result = _parse_required_json_object("payload_json", input.payload_json)
        if isinstance(payload_result, gestalt.Response):
            return payload_result
        return payload_result

    if not input.counterparty_id.strip():
        return _bad_request("counterparty_id is required")
    if not input.idempotency_key.strip():
        return _bad_request("idempotency_key is required")

    payload: dict[str, Any] = {"counterparty_id": input.counterparty_id.strip()}
    if input.name.strip():
        payload["name"] = input.name.strip()
    if input.account_type.strip():
        payload["account_type"] = input.account_type.strip()
    if input.party_name.strip():
        payload["party_name"] = input.party_name.strip()
    if input.party_type.strip():
        payload["party_type"] = input.party_type.strip()
    if input.external_id.strip():
        payload["external_id"] = input.external_id.strip()

    metadata_result = _parse_optional_json_object("metadata_json", input.metadata_json)
    if isinstance(metadata_result, gestalt.Response):
        return metadata_result
    if metadata_result:
        payload["metadata"] = metadata_result

    address_result = _parse_optional_json_object("party_address_json", input.party_address_json)
    if isinstance(address_result, gestalt.Response):
        return address_result
    if address_result:
        payload["party_address"] = address_result

    if input.plaid_processor_token.strip():
        payload["plaid_processor_token"] = input.plaid_processor_token.strip()
        return payload

    if not input.account_number.strip():
        return _bad_request("account_number is required when plaid_processor_token is not provided")
    if not input.routing_number.strip():
        return _bad_request("routing_number is required when plaid_processor_token is not provided")
    if not input.routing_number_type.strip():
        return _bad_request("routing_number_type is required when plaid_processor_token is not provided")

    payload["account_details"] = [
        {
            "account_number": input.account_number.strip(),
            "account_number_type": input.account_number_type.strip() or "other",
        }
    ]
    routing_detail = {
        "routing_number": input.routing_number.strip(),
        "routing_number_type": input.routing_number_type.strip(),
    }
    if input.payment_type.strip():
        routing_detail["payment_type"] = input.payment_type.strip()
    payload["routing_details"] = [routing_detail]
    return payload


def _build_payment_order_payload(input: PaymentOrderCreateInput) -> dict[str, Any] | ErrorResponse:
    if input.payload_json.strip():
        payload_result = _parse_required_json_object("payload_json", input.payload_json)
        if isinstance(payload_result, gestalt.Response):
            return payload_result
        return payload_result

    if not input.idempotency_key.strip():
        return _bad_request("idempotency_key is required")
    if input.amount <= 0:
        return _bad_request("amount must be greater than zero")
    if not input.direction.strip():
        return _bad_request("direction is required")
    if not input.originating_account_id.strip():
        return _bad_request("originating_account_id is required")
    if not input.type.strip():
        return _bad_request("type is required")

    payload: dict[str, Any] = {
        "amount": input.amount,
        "currency": input.currency.strip() or "USD",
        "direction": input.direction.strip(),
        "originating_account_id": input.originating_account_id.strip(),
        "type": input.type.strip(),
    }
    if input.receiving_account_id.strip():
        payload["receiving_account_id"] = input.receiving_account_id.strip()
    if input.description.strip():
        payload["description"] = input.description.strip()
    if input.effective_date.strip():
        payload["effective_date"] = input.effective_date.strip()
    if input.process_after.strip():
        payload["process_after"] = input.process_after.strip()
    if input.remittance_information.strip():
        payload["remittance_information"] = input.remittance_information.strip()
    if input.statement_descriptor.strip():
        payload["statement_descriptor"] = input.statement_descriptor.strip()
    if input.external_id.strip():
        payload["external_id"] = input.external_id.strip()
    if input.priority.strip():
        payload["priority"] = input.priority.strip()
    if input.ledger_transaction_id.strip():
        payload["ledger_transaction_id"] = input.ledger_transaction_id.strip()

    metadata_result = _parse_optional_json_object("metadata_json", input.metadata_json)
    if isinstance(metadata_result, gestalt.Response):
        return metadata_result
    if metadata_result:
        payload["metadata"] = metadata_result
    return payload


def _request(
    req: gestalt.Request,
    *,
    method: str,
    path: str,
    query: dict[str, Any] | None = None,
    body: dict[str, Any] | list[Any] | None = None,
    idempotency_key: str = "",
) -> OperationResult:
    credentials_error = _validate_credentials(req)
    if credentials_error is not None:
        return credentials_error
    client = ModernTreasuryClient.from_request(req)
    try:
        result = client.request(method=method, path=path, query=query, body=body, idempotency_key=idempotency_key)
        if isinstance(result, dict):
            return result
        if isinstance(result, list):
            return result
        return {"data": result}
    except ModernTreasuryAPIError as err:
        return gestalt.Response(
            status=err.status,
            body={"error": err.message, "details": err.details},
        )


def _validate_credentials(req: gestalt.Request) -> ErrorResponse | None:
    if not req.token.strip():
        return gestalt.Response(status=HTTPStatus.UNAUTHORIZED, body={"error": "API key is required"})
    if not req.connection_param("org_id").strip():
        return _bad_request("org_id connection parameter is required")
    return None


def _parse_optional_json_object(name: str, raw: str) -> dict[str, Any] | ErrorResponse:
    if not raw.strip():
        return {}
    return _parse_required_json_object(name, raw)


def _parse_required_json_object(name: str, raw: str) -> dict[str, Any] | ErrorResponse:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as err:
        return _bad_request(f"{name} must be valid JSON: {err.msg}")
    if not isinstance(value, dict):
        return _bad_request(f"{name} must be a JSON object")
    return value


def _bad_request(message: str) -> ErrorResponse:
    return gestalt.Response(status=HTTPStatus.BAD_REQUEST, body={"error": message})
