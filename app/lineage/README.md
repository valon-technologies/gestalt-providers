# dbt Column Lineage

Returns upstream or downstream column lineage from
`valon-analytics-prod.infra_monitoring.dbt_column_lineage`.

## Operation

`get_column_lineage` accepts:

- `tenant`: organization or environment scope
  - `1` or `valon_mortgage`: Valon Mortgage
  - `9` or `service_mac`: ServiceMac
  - `1,9`, `1, 9`, or `valon_analytics`: combined multi-tenant analytics
- `model`
- `column`
- `direction`: `upstream` for source columns or `downstream` for dependent columns
- `max_depth`: optional, defaults to 20

Example request:

```json
{
  "tenant": "1",
  "model": "dim_loans",
  "column": "loan_sid",
  "direction": "upstream"
}
```

The response uses the canonical tenant name (`valon_mortgage`, `service_mac`,
or `valon_analytics`).

The provider uses Application Default Credentials. Its runtime identity needs
BigQuery job access and read access to the lineage table.
