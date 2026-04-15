# Trunk

Manage Trunk Merge Queue and Flaky Tests APIs.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  trunk:
    source: github.com/valon-technologies/gestalt-providers/plugins/trunk
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

## Documentation

- [Trunk APIs](https://docs.trunk.io/setup-and-administration/apis)
- [Trunk Merge Queue API reference](https://docs.trunk.io/merge-queue/reference/merge)
- [Trunk Flaky Tests API](https://docs.trunk.io/flaky-tests/flaky-tests)
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
