# Vanta

Security, compliance, and trust management API.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  vanta:
    source: github.com/valon-technologies/gestalt-providers/app/vanta
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on the official Vanta OpenAPI specification. Exposes operations
for managing security and compliance data including:

- **Audits** — controls, frameworks, documents, tests
- **Vendors** — vendor management, security reviews, findings
- **Vulnerabilities** — vulnerability tracking, remediations, assets
- **People & Computers** — employees, groups, monitored devices
- **Customer Trust** — questionnaires, trust centers, questionnaires
- **Policies & Contracts** — policy listing, contract management

Supports cursor-based pagination.

The OpenAPI document is vendored in this package so provider packaging and
lockfile validation do not depend on Vanta's hosted raw OpenAPI URL. The
vendored file was generated from the official Postman collection linked from
Vanta's Postman setup documentation.

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
