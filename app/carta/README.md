# Carta

Access Carta API Platform endpoints for Launch, Investor, Issuer, Portfolio,
corporation, compensation, open cap tables, file uploads, and current-user
lookups.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  carta:
    source: github.com/valon-technologies/gestalt-providers/app/carta
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

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  carta:
    source: github.com/valon-technologies/gestalt-providers/app/carta
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses OAuth 2.0; mode `subject`.
  - Requested scopes: `read_compensation_benchmarks`, `read_corporation_info`, `read_draftissuers`, `read_investor_capitalizationtables`, `read_investor_cashbalances`, `read_investor_firms`, `read_investor_fundperformance`, `read_investor_funds`, `read_investor_investments`, `read_investor_partners`, `read_investor_securities`, `read_investor_stakeholdercapitalizationtable`, `read_issuer_capitalizationtablesummary`, `read_issuer_draftsecurities`, `read_issuer_info`, `read_issuer_interests`, `read_issuer_securities`, `read_issuer_securitiestemplates`, `read_issuer_shareclasses`, `read_issuer_stakeholdercapitalizationtable`, `read_issuer_stakeholders`, `read_issuer_valuations`, `read_opencaptables`, `read_portfolio_fundinvestmentdocuments`, `read_portfolio_info`, `read_portfolio_issuervaluations`, `read_portfolio_securities`, `read_portfolio_transactions`, `read_user_info`, `readwrite_draftissuers`, `readwrite_file_upload`, `readwrite_issuer_draftsecurities`, `readwrite_issuer_securities`, `readwrite_opencaptables`.

Operation surfaces: OpenAPI.

Representative operations include:

- `v1alpha1.corporations.listCorporations`
- `v1alpha1.corporations.listCorporations`
- `v1alpha1.compensation.GetCompensationBenchmarkAttributes`
- `v1alpha1.compensation.GetCompensationBenchmarks`
- `v1alpha1.draftIssuers.createDraftIssuer`
- `v1alpha1.draftIssuers.getDraftIssuer`
- `v1alpha1.files.uploadFile`
- `v1alpha1.investors.listFirms`
- `v1alpha1.investors.listFunds`

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
apps:
  example_consumer:
    invokes:
      - plugin: carta
        operation: v1alpha1.corporations.listCorporations
```

Example `v1alpha1.corporations.listCorporations` call:

```ts
await app.invoke("carta", "v1alpha1.corporations.listCorporations", {});
```

## Documentation
- [Carta API Platform Introduction](https://docs.carta.com/api-platform/docs/introduction)
- [Carta API Platform Scopes](https://docs.carta.com/api-platform/docs/scopes)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
