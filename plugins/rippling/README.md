# Rippling

Access company, employee, org structure, payroll, identity, leave, and time data from Rippling's REST API.

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
entries, work locations, payroll runs, earning types, and more. Includes write
support for employee updates, department/title/work-location management, job
assignments/codes/dimensions, leave request creation and updates, time entry
create/update/delete, and payroll earnings.

Authenticates with a bearer API token.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
