from __future__ import annotations

import io
import json
import unittest
import urllib.error
import urllib.parse
import urllib.request
from email.message import Message
from http import HTTPStatus
from typing import cast
from unittest import mock

import gestalt

import provider as provider_module


class FakeHTTPResponse:
    def __init__(self, body: str) -> None:
        self._body = body.encode("utf-8")

    def __enter__(self) -> FakeHTTPResponse:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def make_request() -> gestalt.Request:
    return gestalt.Request(token="mt-secret", connection_params={"org_id": "org_123"})


def make_http_error(url: str, status: int, body: str) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        url=url,
        code=status,
        msg="error",
        hdrs=Message(),
        fp=io.BytesIO(body.encode("utf-8")),
    )


def header(request: urllib.request.Request, name: str) -> str | None:
    target = name.lower()
    for key, value in request.header_items():
        if key.lower() == target:
            return value
    return request.get_header(name)


class ModernTreasuryProviderTests(unittest.TestCase):
    def test_validate_routing_number_uses_expected_endpoint_and_query(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "GET")
            self.assertTrue((header(request, "Authorization") or "").startswith("Basic "))

            parsed = urllib.parse.urlsplit(request.full_url)
            self.assertEqual(parsed.path, "/api/validations/routing_numbers")
            self.assertEqual(
                urllib.parse.parse_qs(parsed.query),
                {"routing_number": ["021000021"], "routing_number_type": ["aba"]},
            )
            return FakeHTTPResponse('{"status":"ok"}')

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=fake_urlopen):
            result = provider_module.routing_numbers_validate(
                provider_module.ValidateRoutingNumberInput(
                    routing_number="021000021",
                    routing_number_type="aba",
                ),
                make_request(),
            )

        self.assertEqual(result, {"status": "ok"})

    def test_create_payment_order_builds_json_payload_and_idempotency_header(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            self.assertEqual(timeout, 30)
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(header(request, "Idempotency-Key"), "idem-123")
            self.assertEqual(header(request, "Content-Type"), "application/json")
            self.assertEqual(urllib.parse.urlsplit(request.full_url).path, "/api/payment_orders")

            payload = json.loads(cast(bytes, request.data).decode("utf-8"))
            self.assertEqual(payload["amount"], 1250)
            self.assertEqual(payload["originating_account_id"], "ia_123")
            self.assertEqual(payload["receiving_account_id"], "ea_456")
            self.assertEqual(payload["metadata"], {"loan_number": "10001"})
            return FakeHTTPResponse('{"id":"po_123"}')

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=fake_urlopen):
            result = provider_module.payment_orders_create(
                provider_module.PaymentOrderCreateInput(
                    idempotency_key="idem-123",
                    amount=1250,
                    direction="credit",
                    originating_account_id="ia_123",
                    receiving_account_id="ea_456",
                    type="ach",
                    metadata_json='{"loan_number":"10001"}',
                ),
                make_request(),
            )

        self.assertEqual(result, {"id": "po_123"})

    def test_create_external_account_builds_nested_routing_and_account_details(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            self.assertEqual(request.get_method(), "POST")
            self.assertEqual(urllib.parse.urlsplit(request.full_url).path, "/api/external_accounts")
            payload = json.loads(cast(bytes, request.data).decode("utf-8"))
            self.assertEqual(payload["counterparty_id"], "cp_123")
            self.assertEqual(payload["account_details"][0]["account_number"], "123456789")
            self.assertEqual(payload["routing_details"][0]["routing_number"], "021000021")
            self.assertEqual(payload["party_address"]["line1"], "100 Main St")
            return FakeHTTPResponse('{"id":"ea_123"}')

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=fake_urlopen):
            result = provider_module.external_accounts_create(
                provider_module.ExternalAccountCreateInput(
                    counterparty_id="cp_123",
                    idempotency_key="idem-456",
                    account_number="123456789",
                    routing_number="021000021",
                    routing_number_type="aba",
                    party_address_json='{"line1":"100 Main St","locality":"New York"}',
                ),
                make_request(),
            )

        self.assertEqual(result, {"id": "ea_123"})

    def test_list_payment_orders_merges_pagination_and_query_json(self) -> None:
        def fake_urlopen(request: urllib.request.Request, timeout: float = 30) -> FakeHTTPResponse:
            parsed = urllib.parse.urlsplit(request.full_url)
            self.assertEqual(parsed.path, "/api/payment_orders")
            self.assertEqual(
                urllib.parse.parse_qs(parsed.query),
                {"after_cursor": ["next"], "per_page": ["50"], "status": ["completed"]},
            )
            return FakeHTTPResponse('{"data":[]}')

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=fake_urlopen):
            result = provider_module.payment_orders_list(
                provider_module.ListInput(
                    after_cursor="next",
                    per_page=50,
                    query_json='{"status":"completed"}',
                ),
                make_request(),
            )

        self.assertEqual(result, {"data": []})

    def test_missing_org_id_is_rejected(self) -> None:
        result = provider_module.transactions_get(
            provider_module.GetByIdInput(id="trxn_123"),
            gestalt.Request(token="mt-secret"),
        )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(response.body, {"error": "org_id connection parameter is required"})

    def test_api_error_preserves_status_and_message(self) -> None:
        error = make_http_error(
            "https://app.moderntreasury.com/api/transactions/trxn_123",
            404,
            '{"message":"not found"}',
        )

        with mock.patch("internals.client.urllib.request.urlopen", side_effect=error):
            result = provider_module.transactions_get(
                provider_module.GetByIdInput(id="trxn_123"),
                make_request(),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, object]], result)
        self.assertEqual(response.status, 404)
        self.assertEqual(response.body["error"], "not found")


if __name__ == "__main__":
    unittest.main()
