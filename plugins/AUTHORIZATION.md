# Plugin Authorization Inventory

This inventory separates three concepts that are easy to conflate:

- caller subject type: user or workload
- provider connection mode: `user`, `identity`, or `none`
- plugin-local authorization logic

The rule for this repository is:

- providers should remain subject-neutral unless the provider itself truly needs
  to distinguish user from workload
- workload support comes from exposing a compatible `identity` or `none`
  connection, not from hard-coding provider logic around subject type
- plugin-local authorization is only for provider-specific resource semantics,
  not for replacing `gestaltd` host authorization

## Ready Now

These providers are already compatible with workload authorization v1 using
their current manifest:

| Provider | Why it works |
| --- | --- |
| `httpbin` | `connectionMode: none` |

## Additive Identity Support In This Change

These providers now expose both:

- `default` with `mode: user`
- `identity` with `mode: identity`

That keeps current per-user behavior intact while allowing workloads to bind to
the `identity` connection when the deployment config supplies shared credentials.

| Provider | Identity credential shape |
| --- | --- |
| `incident_io` | API key |
| `intercom` | API access token |
| `launchdarkly` | access token or service token |
| `modern_treasury` | organization ID + API key |
| `rippling` | bearer API token |
| `twilio` | account SID + auth token |

## Candidate For Future Identity Support

These providers look capable of supporting workloads later, but not by a safe
manifest-only flip today. They need a different auth surface, an explicit
service-account story, or both.

| Provider | Why not now |
| --- | --- |
| `bigquery` | current provider uses user OAuth; deployment identity would need a service-account auth path |
| `clickhouse` | current MCP connection uses user `mcp_oauth`; shared MCP credentials need a separate connection model |
| `datadog` | likely service-friendly, but the manifest does not yet declare an explicit auth connection shape |
| `extend` | likely service-friendly, but the manifest does not yet declare an explicit auth connection shape |
| `gitlab` | current manifest lacks an explicit service-auth connection; service accounts or PATs should be modeled deliberately |
| `github` | current provider is user OAuth; workload support likely wants a GitHub App or separate service credential model |
| `hex` | current README points to personal API keys; workspace-token support should be modeled explicitly before enabling identity |
| `linear` | current manifest does not declare a separate service-auth connection |
| `pagerduty` | current provider is user OAuth; service credentials would need a different auth story |
| `ramp` | current provider is user OAuth |
| `slack` | current provider uses user OAuth and `authed_user.access_token`; workload support likely needs bot/app tokens |
| `vercel` | current provider is user OAuth; shared deployment credentials would need a separate auth model |

## Keep User-Centric For Now

These providers are intentionally left on their existing user-centric auth
models. Many could support workloads eventually, but doing so would require a
different security model than the one currently encoded in the manifest.

- `asana`
- `ashby`
- `confluence`
- `dbt_cloud`
- `figma`
- `gmail`
- `google_calendar`
- `google_docs`
- `google_drive`
- `google_forms`
- `google_sheets`
- `google_slides`
- `jira`
- `notion`

## Guidance For Future Changes

When evaluating a provider for workload support, prefer this order:

1. Add an `identity` connection alongside the current `default` user
   connection when the upstream already supports stable shared credentials.
2. Keep the existing `default` connection unchanged so current user flows do not
   silently switch credential ownership.
3. Only add plugin-local authorization checks when the provider owns
   resource-level semantics that `gestaltd` cannot evaluate itself.
