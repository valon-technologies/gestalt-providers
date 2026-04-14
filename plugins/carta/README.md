# Carta

Access Carta API Platform endpoints for Launch, Investor, Issuer, Portfolio,
corporation, compensation, open cap tables, file uploads, and current-user
lookups.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  carta:
    source: github.com/valon-technologies/gestalt-providers/plugins/carta
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

OpenAPI-backed provider using Carta's official API Platform specification,
vendored locally as a condensed OpenAPI 3.2 YAML document for stability. The
shipped spec keeps all Launch, Investor, Issuer, Portfolio, and related API
Platform operations and is pinned to the production API server for deterministic
routing.

Covered interface groups include:

- Launch / Draft Issuer
- Investor firms, funds, investments, securities, capitalization, accounting,
  partner, and performance endpoints
- Issuer companies, capitalization tables, stakeholders, securities, share
  classes, valuations, and draft security write endpoints
- Portfolio holdings, transactions, valuations, securities, and fund investment
  document endpoints
- Corporation, compensation benchmarking, file upload, open cap table, and
  current-user endpoints

The default connection uses Carta's OAuth2 authorization-code flow. An identity
fallback is also available for pre-minted bearer tokens. In either case, the
token must carry the scopes required by the endpoints you invoke.

## Documentation

- [Carta API Platform Introduction](https://docs.carta.com/api-platform/docs/introduction)
- [Carta API Platform Scopes](https://docs.carta.com/api-platform/docs/scopes)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
