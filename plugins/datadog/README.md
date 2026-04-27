# Datadog

Manage dashboards, monitors, incidents, logs, RUM, and CI Visibility.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  datadog:
    source: github.com/valon-technologies/gestalt-providers/plugins/datadog
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on a local OpenAPI specification. Exposes operations
for managing Datadog dashboards, monitors, incidents, log queries, Real User
Monitoring (RUM) data, and CI Visibility pipeline events.

## Required Datadog scopes

The plugin authenticates with a Datadog API key + Application key. Grant the
Application key the **least-privilege** scopes that match the operations you
intend to invoke.

### Recommended scope set

For an agent / shared key that runs the operations exposed today plus the
read-only expansions the plugin is likely to grow into:

```
ci_visibility_read       # search_ci_pipelines and future read-side CI ops
dashboards_read          # list_dashboards, get_dashboard
dashboards_write         # create_dashboard, update_dashboard
incident_read            # list_incidents
incident_write           # create_incident, update_incident
logs_read_data           # search_logs
metrics_read             # query_metrics
monitors_read            # list_monitors, get_monitor
monitors_write           # create_monitor, update_monitor, create_downtime
rum_apps_read            # list_rum_events, search_rum_events, aggregate_rum_events
synthetics_read          # future synthetics read ops
```

Optional, useful for read-only coverage of related products:

```
apm_read
cd_visibility_read
cloud_cost_management_read
dora_metrics_read
logs_read_archives, logs_read_config, logs_read_index_data, logs_read_workspaces
logs_live_tail
synthetics_default_settings_read
synthetics_global_variable_read
synthetics_private_location_read
timeseries_query
```

### Scopes to avoid on a shared / agent key

These scopes give the key the ability to silently destroy data, suppress
alerts, or expose internal data publicly. Do **not** grant them unless a
specific operation requires it, and prefer a personal key for one-off work
that does:

| Scope | Why to avoid |
|---|---|
| `logs_delete_data` | Permanently deletes log data. No legitimate API-key use case for most teams. |
| `dashboards_public_share` | Creates internet-public dashboards. One leak puts customer / PII data on the open web. |
| `embeddable_graphs_share` | Same public-exposure risk for individual graphs. |
| `logs_write_forwarding_rules` | Lets the key forward logs to attacker-controlled destinations (silent exfiltration). |
| `logs_write_pipelines` | Rewrite log content at ingest — can hide evidence of activity. |
| `logs_write_processors` | Same as above. |
| `logs_write_exclusion_filters` | Drop logs at ingest — can suppress security signal. |
| `logs_modify_indexes` | Change log retention / indexing — can drop data wholesale. |
| `logs_write_historical_view` | Rehydrates archived logs; large unexpected cost trigger. |

`monitors_write` is in the recommended set above because the plugin exposes
write operations for monitors and downtimes; understand that this includes the
ability to disable / mute alerts. Scope your application key narrower if the
agent only needs to read.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
