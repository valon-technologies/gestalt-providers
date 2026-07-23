from __future__ import annotations

import datetime as dt
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

from google.cloud import bigquery

PROJECT_ID = "valon-analytics-prod"
LINEAGE_TABLE = f"{PROJECT_ID}.infra_monitoring.dbt_column_lineage"


@dataclass(frozen=True, slots=True)
class Edge:
    downstream_model: str
    downstream_column: str
    upstream_model: str
    upstream_column: str


@dataclass(frozen=True, slots=True)
class LineageNode:
    model: str
    column: str
    depth: int


@dataclass(frozen=True, slots=True)
class LineageSnapshot:
    edges: tuple[Edge, ...]
    generated_at: str | None

    @classmethod
    def load(cls, tenant: str) -> LineageSnapshot:
        query = f"""
            SELECT
              downstream_model,
              downstream_column,
              upstream_model,
              upstream_column,
              generated_at
            FROM `{LINEAGE_TABLE}`
            WHERE tenant = @tenant
              AND edge_level = 'column'
            ORDER BY
              downstream_model,
              downstream_column,
              upstream_model,
              upstream_column
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("tenant", "STRING", tenant),
            ]
        )
        with bigquery.Client(project=PROJECT_ID) as client:
            rows = client.query(query, job_config=job_config).result()

        edges: list[Edge] = []
        generated_at: dt.datetime | None = None
        for row in rows:
            edges.append(
                Edge(
                    downstream_model=row["downstream_model"],
                    downstream_column=row["downstream_column"],
                    upstream_model=row["upstream_model"],
                    upstream_column=row["upstream_column"],
                )
            )
            row_generated_at = row["generated_at"]
            if generated_at is None or row_generated_at > generated_at:
                generated_at = row_generated_at

        return cls(
            edges=tuple(edges),
            generated_at=generated_at.isoformat() if generated_at else None,
        )


def traverse(
    edges: Iterable[Edge],
    *,
    model: str,
    column: str,
    direction: Literal["upstream", "downstream"],
    max_depth: int,
) -> list[LineageNode]:
    adjacency: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
    for edge in edges:
        if direction == "upstream":
            start = (edge.downstream_model, edge.downstream_column)
            end = (edge.upstream_model, edge.upstream_column)
        else:
            start = (edge.upstream_model, edge.upstream_column)
            end = (edge.downstream_model, edge.downstream_column)
        adjacency[start].append(end)

    root = (model, column)
    seen = {root}
    frontier = [root]
    results: list[LineageNode] = []

    for depth in range(1, max_depth + 1):
        next_frontier: list[tuple[str, str]] = []
        for current in frontier:
            for next_model, next_column in adjacency.get(current, []):
                node = (next_model, next_column)
                if node in seen:
                    continue
                seen.add(node)
                next_frontier.append(node)
                results.append(
                    LineageNode(
                        model=next_model,
                        column=next_column,
                        depth=depth,
                    )
                )
        if not next_frontier:
            break
        frontier = next_frontier

    return sorted(results, key=lambda node: (node.depth, node.model, node.column))
