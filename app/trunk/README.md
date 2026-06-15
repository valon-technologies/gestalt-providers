# Trunk

Manage Trunk Merge Queue and Flaky Tests APIs.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
apps:
  trunk:
    source: github.com/valon-technologies/gestalt-providers/app/trunk
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Declarative provider built on a vendored OpenAPI 3.2 document for Trunk's
public APIs. It exposes Merge Queue operations for queue management and pull
request submission, plus Flaky Tests operations for test details, unhealthy
test discovery, quarantined tests, and failing-test queries.

The default connection uses a manually provided Trunk API token and sends it as
the `x-api-token` header.

Available interface groups include:

- `mergeQueue.*` for PR submission, cancellation, testing details, and metrics
- `queues.*` for queue lifecycle and configuration
- `flakyTests.*` for flaky-test and quarantine queries

## Limitations

`mergeQueue.setImpactedTargets` models the documented token-authenticated
workflow using `x-api-token`. Trunk also documents a forked-PR path that uses
`x-forked-workflow-run-id` instead of an API token; that alternate auth flow is
not currently modeled by this plugin connection and should be called directly
against Trunk from the workflow when needed.

## Example Operations

```yaml
tool_calls:
  - plugin: trunk
    operation: mergeQueue.submitPullRequest
    input:
      repo:
        host: github.com
        owner: my-org
        name: my-repo
      pr:
        number: 123
      targetBranch: main
      priority: 10
      noBatch: true
```

```yaml
tool_calls:
  - plugin: trunk
    operation: flakyTests.listUnhealthyTests
    input:
      repo:
        host: github.com
        owner: my-org
        name: my-repo
      org_url_slug: my-trunk-org-slug
      page_query:
        page_size: 50
        page_token: ""
      status: FLAKY
```

```yaml
tool_calls:
  - plugin: trunk
    operation: mergeQueue.setImpactedTargets
    input:
      repo:
        host: github.com
        owner: my-org
        name: my-repo
      pr:
        number: 123
        sha: 0123456789abcdef0123456789abcdef01234567
      targetBranch: main
      impactedTargets:
        - backend
        - docs
```

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
apps:
  trunk:
    source: github.com/valon-technologies/gestalt-providers/app/trunk
    version: ...
```

This provider does not define provider-level config fields in its config schema. Configure credentials through the connection described below.

Connections and authentication:

- `default` uses manual credentials; mode `subject`.
  - Credential fields: `token`.
  - `token`: Create a Trunk API token and use it as the `x-api-token` header. See [Trunk APIs](https://docs.trunk.io/setup-and-administration/apis).

Operation surfaces: OpenAPI.

Representative operations include:

- `flakyTests.listUnhealthyTests`
- `mergeQueue.submitPullRequest`
- `mergeQueue.cancelPullRequest`
- `mergeQueue.getSubmittedPullRequest`
- `mergeQueue.restartTestsOnPullRequest`
- `mergeQueue.setImpactedTargets`
- `mergeQueue.getTestingDetails`
- `mergeQueue.getMetrics`
- `queues.create`
- `queues.delete`

## Usage Examples

Hosted apps call this provider with `app.invoke`. Pass `runAs` or `credentialMode` in the invoke options when an operation needs a service-account identity or managed credentials instead of the caller's OAuth token.

Example `flakyTests.listUnhealthyTests` call:

```ts
await app.invoke("trunk", "flakyTests.listUnhealthyTests", { orgSlug: "acme", repoSlug: "widgets" });
```

## Documentation
- [Trunk APIs](https://docs.trunk.io/setup-and-administration/apis)
- [Trunk Merge Queue API reference](https://docs.trunk.io/merge-queue/reference/merge)
- [Trunk Flaky Tests API](https://docs.trunk.io/flaky-tests/flaky-tests)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
