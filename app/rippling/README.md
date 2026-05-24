# Rippling

Access company, employee, org structure, payroll, identity, leave, and time data from Rippling's REST API.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  rippling:
    source: github.com/valon-technologies/gestalt-providers/app/rippling
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Curated OpenAPI-backed provider with a sibling `openapi.yaml`, covering
companies, employees, compensations, departments, teams, leave management, time
entries, work locations, payroll runs, earning types, and more. Includes write
support for employee updates, department/title/work-location management, job
assignments/codes/dimensions, leave request creation and updates, time entry
create/update/delete, and payroll earnings.

Authenticates with a bearer API token.

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  rippling:
    source: github.com/valon-technologies/gestalt-providers/app/rippling
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses bearer token; mode `user`.
  - Credential fields: `token`.
  - `token`: Create one in [Rippling API Tokens](https://app.rippling.com/developer/api-tokens)

Operation surfaces: OpenAPI.

Representative operations include:

- `Employees.List`
- `Companies.List`
- `Compensations.List`
- `Compensations.Get`
- `CurrentUser.Get`
- `CustomFields.List`
- `Departments.List`
- `Departments.Create`
- `Departments.Get`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
apps:
  example_consumer:
    invokes:
      - plugin: rippling
        operation: Employees.List
```

Example `Employees.List` call:

```ts
await invoker.invoke("rippling", "Employees.List", { limit: 20 });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
