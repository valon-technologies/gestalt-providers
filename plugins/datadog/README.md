# Datadog

Manage dashboards, monitors, incidents, logs, RUM, CI Visibility, Synthetics, users, and roles.

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

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
