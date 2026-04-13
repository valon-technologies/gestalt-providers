# Rippling

Access company, employee, org structure, identity, leave, and time data from Rippling's REST API.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  rippling:
    source: github.com/valon-technologies/gestalt-providers/plugins/rippling
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Curated OpenAPI-backed provider with a sibling `openapi.yaml`, covering
companies, employees, compensations, departments, teams, leave management, time
entries, work locations, and more. Includes limited write support for employee
updates, leave request creation and updates, and time entry create, update, and
delete operations.

Authenticates with a bearer API token.

## Connections

The manifest now exposes both a per-user `default` connection and an additive
`identity` connection for shared deployment credentials. Workloads should bind
to `identity`; existing user flows continue to use `default`.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
