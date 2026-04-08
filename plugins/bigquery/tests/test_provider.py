import unittest
from http import HTTPStatus
from typing import cast
from unittest import mock

import gestalt
from google.api_core.exceptions import BadRequest, GoogleAPICallError
from google.cloud.bigquery import DatasetReference, SchemaField

import internals.client as client_module
import provider as provider_module


class QueryProviderTests(unittest.TestCase):
    def test_query_rejects_missing_project_id(self) -> None:
        result = provider_module.query(
            provider_module.QueryInput(project_id=" ", query="SELECT 1"),
            gestalt.Request(token="token"),
        )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(response.body, {"error": "project_id is required"})

    def test_query_rejects_missing_query(self) -> None:
        result = provider_module.query(
            provider_module.QueryInput(project_id="serviceone", query=" "),
            gestalt.Request(token="token"),
        )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(response.body, {"error": "query is required"})

    def test_query_returns_structured_bad_request(self) -> None:
        client = mock.Mock()
        client.query.side_effect = BadRequest(
            'Table "loans" must be qualified with a dataset (e.g. dataset.table).'
        )

        with mock.patch.object(client_module.bigquery, "Client") as client_cls:
            client_cls.return_value.__enter__.return_value = client

            result = provider_module.query(
                provider_module.QueryInput(project_id="serviceone", query="SELECT COUNT(1) FROM loans"),
                gestalt.Request(token="token"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.body,
            {"error": 'Table "loans" must be qualified with a dataset (e.g. dataset.table).'},
        )

    def test_query_returns_generic_google_api_failure_as_500(self) -> None:
        client = mock.Mock()
        client.query.side_effect = GoogleAPICallError("generic issue")

        with mock.patch.object(client_module.bigquery, "Client") as client_cls:
            client_cls.return_value.__enter__.return_value = client

            result = provider_module.query(
                provider_module.QueryInput(project_id="serviceone", query="SELECT 1"),
                gestalt.Request(token="token"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.INTERNAL_SERVER_ERROR)
        self.assertEqual(response.body, {"error": "generic issue"})

    def test_query_success_preserves_existing_output_shape(self) -> None:
        iterator = FakeIterator(
            rows=[{"count": 1, "amount": client_module.decimal.Decimal("12.50")}],
            schema=[SchemaField("count", "INT64"), SchemaField("amount", "NUMERIC")],
            total_rows=1,
        )
        job = mock.Mock()
        job.result.return_value = iterator
        client = mock.Mock()
        client.query.return_value = job

        with mock.patch.object(client_module.bigquery, "Client") as client_cls:
            client_cls.return_value.__enter__.return_value = client

            result = provider_module.query(
                provider_module.QueryInput(project_id="serviceone", query="SELECT 1"),
                gestalt.Request(token="token"),
            )

        output = cast(provider_module.QueryOutput, result)
        self.assertEqual(output.total_rows, 1)
        self.assertEqual(output.rows, [{"count": 1, "amount": "12.50"}])
        self.assertEqual(output.schema[0].name, "count")
        self.assertEqual(output.schema[1].name, "amount")
        self.assertTrue(output.job_complete)

    def test_query_sets_default_dataset_when_provided(self) -> None:
        iterator = FakeIterator(rows=[], schema=[], total_rows=0)
        job = mock.Mock()
        job.result.return_value = iterator
        client = mock.Mock()
        client.query.return_value = job

        with (
            mock.patch.object(client_module.bigquery, "Client") as client_cls,
            mock.patch.object(client_module, "QueryJobConfig", side_effect=lambda **kwargs: kwargs) as query_job_config,
        ):
            client_cls.return_value.__enter__.return_value = client

            provider_module.query(
                provider_module.QueryInput(project_id="serviceone", dataset=" reporting ", query="SELECT * FROM loans"),
                gestalt.Request(token="token"),
            )

        default_dataset = query_job_config.call_args.kwargs["default_dataset"]
        self.assertIsInstance(default_dataset, DatasetReference)
        self.assertEqual(default_dataset.project, "serviceone")
        self.assertEqual(default_dataset.dataset_id, "reporting")

    def test_query_leaves_default_dataset_unset_when_missing(self) -> None:
        iterator = FakeIterator(rows=[], schema=[], total_rows=0)
        job = mock.Mock()
        job.result.return_value = iterator
        client = mock.Mock()
        client.query.return_value = job

        with (
            mock.patch.object(client_module.bigquery, "Client") as client_cls,
            mock.patch.object(client_module, "QueryJobConfig", side_effect=lambda **kwargs: kwargs) as query_job_config,
        ):
            client_cls.return_value.__enter__.return_value = client

            provider_module.query(
                provider_module.QueryInput(project_id="serviceone", query="SELECT * FROM reporting.loans"),
                gestalt.Request(token="token"),
            )

        self.assertIsNone(query_job_config.call_args.kwargs["default_dataset"])


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
