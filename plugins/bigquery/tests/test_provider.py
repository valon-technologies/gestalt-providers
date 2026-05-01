import datetime as dt
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
                provider_module.QueryInput(
                    project_id="serviceone", query="SELECT COUNT(1) FROM loans"
                ),
                gestalt.Request(token="token"),
            )

        self.assertIsInstance(result, gestalt.Response)
        response = cast(gestalt.Response[dict[str, str]], result)
        self.assertEqual(response.status, HTTPStatus.BAD_REQUEST)
        self.assertEqual(
            response.body,
            {
                "error": 'Table "loans" must be qualified with a dataset (e.g. dataset.table).'
            },
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
            rows=[
                {
                    "count": 1,
                    "amount": client_module.decimal.Decimal("12.50"),
                    "blob": b"hi",
                }
            ],
            schema=[
                SchemaField("count", "INT64"),
                SchemaField("amount", "NUMERIC"),
                SchemaField("blob", "BYTES"),
            ],
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
        self.assertEqual(output.rows, [{"count": 1, "amount": "12.50", "blob": "aGk="}])
        self.assertEqual(output.schema[0].name, "count")
        self.assertEqual(output.schema[1].name, "amount")
        self.assertEqual(output.schema[2].name, "blob")
        self.assertTrue(output.job_complete)

    def test_query_applies_max_results_as_row_limit(self) -> None:
        iterator = FakeIterator(
            rows=[{"count": 1}, {"count": 2}],
            schema=[SchemaField("count", "INT64")],
            total_rows=2,
        )
        job = mock.Mock()
        job.result.return_value = iterator
        client = mock.Mock()
        client.query.return_value = job

        with mock.patch.object(client_module.bigquery, "Client") as client_cls:
            client_cls.return_value.__enter__.return_value = client

            result = provider_module.query(
                provider_module.QueryInput(
                    project_id="serviceone",
                    query="SELECT 1",
                    max_results=1,
                ),
                gestalt.Request(token="token"),
            )

        output = cast(provider_module.QueryOutput, result)
        self.assertEqual(output.rows, [{"count": 1}])

    def test_query_sanitizes_nested_rows_and_non_finite_floats(self) -> None:
        iterator = FakeIterator(
            rows=[
                {
                    "child": FakeRow(
                        {
                            "amount": client_module.decimal.Decimal("3.14"),
                            "day": dt.date(2026, 5, 1),
                        }
                    ),
                    "children": [FakeRow({"payload": b"ok"})],
                    "nan_value": float("nan"),
                    "positive_infinity": float("inf"),
                    "negative_infinity": float("-inf"),
                }
            ],
            schema=[
                SchemaField("child", "RECORD"),
                SchemaField("children", "RECORD", mode="REPEATED"),
                SchemaField("nan_value", "FLOAT64"),
                SchemaField("positive_infinity", "FLOAT64"),
                SchemaField("negative_infinity", "FLOAT64"),
            ],
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
        self.assertEqual(
            output.rows,
            [
                {
                    "child": {"amount": "3.14", "day": "2026-05-01"},
                    "children": [{"payload": "b2s="}],
                    "nan_value": "NaN",
                    "positive_infinity": "Infinity",
                    "negative_infinity": "-Infinity",
                }
            ],
        )

    def test_query_sets_default_dataset_when_provided(self) -> None:
        iterator = FakeIterator(rows=[], schema=[], total_rows=0)
        job = mock.Mock()
        job.result.return_value = iterator
        client = mock.Mock()
        client.query.return_value = job

        with (
            mock.patch.object(client_module.bigquery, "Client") as client_cls,
            mock.patch.object(
                client_module, "QueryJobConfig", side_effect=lambda **kwargs: kwargs
            ) as query_job_config,
        ):
            client_cls.return_value.__enter__.return_value = client

            provider_module.query(
                provider_module.QueryInput(
                    project_id="serviceone",
                    dataset=" reporting ",
                    query="SELECT * FROM loans",
                ),
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
            mock.patch.object(
                client_module, "QueryJobConfig", side_effect=lambda **kwargs: kwargs
            ) as query_job_config,
        ):
            client_cls.return_value.__enter__.return_value = client

            provider_module.query(
                provider_module.QueryInput(
                    project_id="serviceone", query="SELECT * FROM reporting.loans"
                ),
                gestalt.Request(token="token"),
            )

        self.assertIsNone(query_job_config.call_args.kwargs["default_dataset"])


class FakeIterator:
    def __init__(
        self, rows: list[dict[str, object]], schema: list[SchemaField], total_rows: int
    ) -> None:
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
