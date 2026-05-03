# Vanta

Security, compliance, and trust management API.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  vanta:
    source: github.com/valon-technologies/gestalt-providers/plugins/vanta
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on a local OpenAPI specification. Exposes operations
for managing security and compliance data including:

- **Audits** — controls, frameworks, documents, tests
- **Vendors** — vendor management, security reviews, findings
- **Vulnerabilities** — vulnerability tracking, remediations, assets
- **People & Computers** — employees, groups, monitored devices
- **Customer Trust** — questionnaires, trust centers, questionnaires
- **Policies & Contracts** — policy listing, contract management

Supports cursor-based pagination.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
