import unittest
from http import HTTPStatus
from unittest import mock

import gestalt
from google.api_core import exceptions as google_exceptions
from google.cloud.bigquery import SchemaField

import provider


class QueryTests(unittest.TestCase):
    def test_query_returns_structured_bad_request(self) -> None:
        client = mock.Mock()
        client.query.side_effect = google_exceptions.BadRequest(
            'Table "loans" must be qualified with a dataset (e.g. dataset.table).'
        )

        with mock.patch.object(provider.bigquery, "Client") as client_cls:
            client_cls.return_value.__enter__.return_value = client

            result = provider.query(
                provider.QueryInput(project_id="serviceone", query="SELECT COUNT(1) FROM loans"),
                gestalt.Request(token="token"),
            )

        self.assertIsInstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            result.body,
            {"error": 'Table "loans" must be qualified with a dataset (e.g. dataset.table).'},
        )

    def test_query_returns_generic_google_api_failure_as_500(self) -> None:
        client = mock.Mock()
        client.query.side_effect = google_exceptions.GoogleAPICallError("generic issue")

        with mock.patch.object(provider.bigquery, "Client") as client_cls:
            client_cls.return_value.__enter__.return_value = client

            result = provider.query(
                provider.QueryInput(project_id="serviceone", query="SELECT 1"),
                gestalt.Request(token="token"),
            )

        self.assertIsInstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(result.body, {"error": "generic issue"})

    def test_query_success_preserves_existing_output_shape(self) -> None:
        iterator = FakeIterator(
            rows=[{"count": 1, "amount": provider.decimal.Decimal("12.50")}],
            schema=[SchemaField("count", "INT64"), SchemaField("amount", "NUMERIC")],
            total_rows=1,
        )
        job = mock.Mock()
        job.result.return_value = iterator
        client = mock.Mock()
        client.query.return_value = job

        with mock.patch.object(provider.bigquery, "Client") as client_cls:
            client_cls.return_value.__enter__.return_value = client

            result = provider.query(
                provider.QueryInput(project_id="serviceone", query="SELECT 1"),
                gestalt.Request(token="token"),
            )

        self.assertEqual(result.total_rows, 1)
        self.assertEqual(result.rows, [{"count": 1, "amount": "12.50"}])
        self.assertEqual(result.schema[0].name, "count")
        self.assertEqual(result.schema[1].name, "amount")
        self.assertTrue(result.job_complete)


class FakeIterator:
    def __init__(self, rows: list[dict[str, object]], schema: list[SchemaField], total_rows: int) -> None:
        self._rows = rows
        self.schema = schema
        self.total_rows = total_rows

    def __iter__(self):
        return iter([FakeRow(row) for row in self._rows])


class FakeRow:
    def __init__(self, values: dict[str, object]) -> None:
        self._values = values

    def items(self):
        return self._values.items()


if __name__ == "__main__":
    unittest.main()
