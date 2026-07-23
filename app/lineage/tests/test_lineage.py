import datetime as dt
from http import HTTPStatus
import unittest
from unittest import mock

import gestalt

import internals.lineage as lineage_module
from internals.lineage import Edge, traverse
import provider as provider_module


EDGES = (
    Edge("stg_loans", "loan_id", "raw.loans", "id"),
    Edge("fct_loans", "loan_id", "stg_loans", "loan_id"),
    Edge("mart_loans", "loan_id", "fct_loans", "loan_id"),
    Edge("loan_report", "loan_id", "fct_loans", "loan_id"),
)


class LineageSnapshotTests(unittest.TestCase):
    def test_load_uses_the_column_lineage_contract(self) -> None:
        client = mock.Mock()
        client.query.return_value.result.return_value = [
            {
                "downstream_model": "stg_loans",
                "downstream_column": "loan_id",
                "upstream_model": "raw.loans",
                "upstream_column": "id",
                "generated_at": dt.datetime(
                    2026, 7, 23, 11, 23, 22, tzinfo=dt.UTC
                ),
            }
        ]

        with mock.patch.object(lineage_module.bigquery, "Client") as client_class:
            client_class.return_value.__enter__.return_value = client
            snapshot = lineage_module.LineageSnapshot.load("service_mac")

        query = client.query.call_args.args[0]
        job_config = client.query.call_args.kwargs["job_config"]
        tenant_parameter = job_config.query_parameters[0]
        self.assertIn(lineage_module.LINEAGE_TABLE, query)
        self.assertIn("edge_level = 'column'", query)
        self.assertEqual(tenant_parameter.name, "tenant")
        self.assertEqual(tenant_parameter.value, "service_mac")
        self.assertEqual(snapshot.edges, (EDGES[0],))
        self.assertEqual(snapshot.generated_at, "2026-07-23T11:23:22+00:00")


class TraverseTests(unittest.TestCase):
    def test_upstream_lineage(self) -> None:
        result = traverse(
            EDGES,
            model="mart_loans",
            column="loan_id",
            direction="upstream",
            max_depth=20,
        )

        self.assertEqual(
            [(node.model, node.column, node.depth) for node in result],
            [
                ("fct_loans", "loan_id", 1),
                ("stg_loans", "loan_id", 2),
                ("raw.loans", "id", 3),
            ],
        )

    def test_operation_returns_downstream_lineage(self) -> None:
        snapshot = provider_module.LineageSnapshot(
            edges=EDGES,
            generated_at="2026-07-23T11:23:22+00:00",
        )
        with mock.patch.object(
            provider_module.LineageSnapshot,
            "load",
            return_value=snapshot,
        ):
            result = provider_module.get_column_lineage(
                provider_module.GetColumnLineageInput(
                    tenant="valon_analytics",
                    model="raw.loans",
                    column="id",
                    direction="downstream",
                ),
                gestalt.Request(),
            )

        self.assertIsInstance(result, provider_module.GetColumnLineageOutput)
        assert isinstance(result, provider_module.GetColumnLineageOutput)
        self.assertEqual(
            [(node.model, node.column, node.depth) for node in result.results],
            [
                ("stg_loans", "loan_id", 1),
                ("fct_loans", "loan_id", 2),
                ("loan_report", "loan_id", 3),
                ("mart_loans", "loan_id", 3),
            ],
        )

    def test_operation_rejects_tenant_without_lineage_data(self) -> None:
        with mock.patch.object(
            provider_module.LineageSnapshot,
            "load",
            return_value=provider_module.LineageSnapshot(
                edges=(),
                generated_at=None,
            ),
        ):
            result = provider_module.get_column_lineage(
                provider_module.GetColumnLineageInput(
                    tenant="unknown",
                    model="model",
                    column="column",
                    direction="upstream",
                ),
                gestalt.Request(),
            )

        self.assertIsInstance(result, gestalt.Response)
        assert isinstance(result, gestalt.Response)
        self.assertEqual(result.status, HTTPStatus.NOT_FOUND)
        self.assertEqual(
            result.body,
            {"error": "No column lineage data found for tenant: unknown"},
        )

    def test_max_depth_stops_traversal(self) -> None:
        result = traverse(
            EDGES,
            model="raw.loans",
            column="id",
            direction="downstream",
            max_depth=1,
        )

        self.assertEqual(
            [(node.model, node.column, node.depth) for node in result],
            [("stg_loans", "loan_id", 1)],
        )

    def test_cycle_does_not_revisit_the_start(self) -> None:
        result = traverse(
            (
                Edge("model_b", "id", "model_a", "id"),
                Edge("model_a", "id", "model_b", "id"),
            ),
            model="model_a",
            column="id",
            direction="downstream",
            max_depth=20,
        )

        self.assertEqual(
            [(node.model, node.column, node.depth) for node in result],
            [("model_b", "id", 1)],
        )


if __name__ == "__main__":
    unittest.main()
