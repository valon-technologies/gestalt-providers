# dbt Column Lineage

Returns upstream or downstream column lineage from
`valon-analytics-prod.infra_monitoring.dbt_column_lineage`.

## Operation

`get_column_lineage` accepts:

- `tenant`
- `model`
- `column`
- `direction`: `upstream` or `downstream`
- `max_depth`: optional, defaults to 20

The provider uses Application Default Credentials. Its runtime identity needs
BigQuery job access and read access to the lineage table.
