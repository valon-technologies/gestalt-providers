# Datadog

Manage dashboards, monitors, incidents, logs, RUM, CI Visibility, Synthetics, users, and roles.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  datadog:
    source: github.com/valon-technologies/gestalt-providers/app/datadog
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on a local OpenAPI specification. Exposes operations
for managing Datadog dashboards, monitors, metric metadata, metric tag
configurations, incidents, log queries, Real User Monitoring (RUM) data,
CI Visibility pipeline events, Synthetic tests, users, roles, and user
invitations.

Metric configuration operations:

```bash
gestalt invoke datadog get_metric_metadata \
  -p metric_name=gestaltd.operation.duration

gestalt invoke datadog update_metric_metadata \
  -p metric_name=gestaltd.operation.duration \
  -p unit=second \
  -p 'description=Measures gestaltd operation invocation duration.'

gestalt invoke datadog create_metric_tag_configuration \
  -p metric_name=gestaltd.operation.duration \
  -p 'data:={"type":"manage_tags","id":"gestaltd.operation.duration","attributes":{"metric_type":"distribution","tags":["env","service","gestalt.provider","gestalt.operation"],"include_percentiles":true}}'

gestalt invoke datadog update_metric_tag_configuration \
  -p metric_name=gestaltd.operation.duration \
  -p 'data:={"type":"manage_tags","id":"gestaltd.operation.duration","attributes":{"tags":["env","service","gestalt.provider","gestalt.operation"],"include_percentiles":true}}'
```

User management operations:

```bash
gestalt invoke datadog list_users
gestalt invoke datadog get_user -p user_id=<USER_ID>

gestalt invoke datadog create_user \
  -p 'data:={"type":"users","attributes":{"email":"user@example.com","name":"Jane Doe","title":"Engineer"}}'

gestalt invoke datadog update_user \
  -p user_id=<USER_ID> \
  -p 'data:={"type":"users","id":"<USER_ID>","attributes":{"name":"Jane Smith","disabled":false}}'

gestalt invoke datadog disable_user -p user_id=<USER_ID>

gestalt invoke datadog list_roles
gestalt invoke datadog get_role -p role_id=<ROLE_ID>
gestalt invoke datadog list_user_roles -p user_id=<USER_ID>
```

User invitation operations:

```bash
gestalt invoke datadog list_user_invitations

gestalt invoke datadog create_user_invitations \
  -p 'data:=[{"type":"user_invitations","attributes":{"email":"newuser@example.com"}}]'

gestalt invoke datadog get_user_invitation -p user_invitation_uuid=<UUID>
```

Sorted RUM aggregation:

```bash
gestalt invoke datadog aggregate_rum_events --format json \
  -p 'filter:={"query":"env:prod service:web @type:view","from":"now-30d","to":"now"}' \
  -p 'compute:=[{"aggregation":"pc75","metric":"@view.largest_contentful_paint"}]' \
  -p 'group_by:=[{"facet":"@view.name","limit":10,"sort":{"aggregation":"pc75","metric":"@view.largest_contentful_paint","order":"desc","type":"measure"}},{"facet":"@context.context.owner","limit":10}]'
```

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  datadog:
    source: github.com/valon-technologies/gestalt-providers/app/datadog
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses manual credentials.
  - Configure the Datadog API key as `DD-API-KEY`.
  - Configure the Datadog application key as `DD-APPLICATION-KEY`.

Operation surfaces: OpenAPI.

Representative operations include:

- `list_monitors`
- `list_dashboards`
- `create_dashboard`
- `get_dashboard`
- `update_dashboard`
- `list_monitors`
- `create_monitor`
- `get_monitor`
- `update_monitor`

- Provide both Datadog API key and application key when creating the manual connection.

## Usage Examples

Grant another provider or workflow permission to invoke this app before calling it:

```yaml
apps:
  example_consumer:
    invokes:
      - app: datadog
        operation: list_monitors
```

Example `list_monitors` call:

```ts
await invoker.invoke("datadog", "list_monitors", { page: 0, page_size: 20 });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
