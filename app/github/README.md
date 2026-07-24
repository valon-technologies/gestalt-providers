# GitHub App Provider

The GitHub app provider connects GitHub App webhooks and generic GitHub bot
operations to Gestalt. Webhooks are accepted at `/github/event`, filtered, and
delivered as canonical workflow events. The provider does not run built-in
agent behavior from webhook delivery.

## Configuration

```yaml
apps:
  github:
    source: github.com/valon-technologies/gestalt-providers/app/github
    config:
      clientId: github-oauth-client-id
      clientSecret: github-oauth-client-secret
      appId: "12345"
      appPrivateKeyEnv: GITHUB_APP_PRIVATE_KEY
      workflow:
        provider: indexeddb
      webhookEvents:
        - pull_request
        - check_run
      ignoreBotSender: true
      cacheEnabled: false
      cacheTtlSeconds: 60
```

Supported config fields:

- `clientId`: GitHub OAuth app client ID for subject-owned user identity
  connections.
- `clientSecret`: GitHub OAuth app client secret for subject-owned user
  identity connections.
- `appId`: GitHub App ID. `GITHUB_APP_ID` is also supported.
- `appPrivateKey`: PEM-encoded GitHub App private key.
- `appPrivateKeyEnv`: environment variable containing the PEM private key.
- `appPrivateKeyPath`: filesystem path to the PEM private key.
- `apiBaseUrl`: GitHub REST API base URL. Defaults to `https://api.github.com`.
- `graphqlBaseUrl`: GitHub GraphQL API URL. Defaults to
  `https://api.github.com/graphql`; derived from Enterprise `apiBaseUrl` when
  omitted.
- `webBaseUrl`: GitHub web URL used for generated links. Defaults to
  `https://github.com`.
- `webhookEvents`: allowlist of GitHub event names to deliver. Defaults to
  `check_run`, `check_suite`, `deployment`, `deployment_status`,
  `issue_comment`, `issues`, `pull_request`, `pull_request_review`,
  `pull_request_review_comment`, and `workflow_run`.
  These are GitHub webhook event names, not action-qualified workflow event
  trigger types.
- `workflow.provider`: workflow provider that receives delivered events.
- `ignoreBotSender`: ignore events sent by the configured GitHub App bot login.
  Defaults to `true`.
- `cacheEnabled`: enable the webhook-fed read-through cache. Defaults to
  `false`; enabling it requires the ambient IndexedDB binding.
- `cacheTtlSeconds`: maximum age of a cached response, from 1 through 3600
  seconds. Defaults to `60`.

## Webhook Events

`events.handle` validates generic webhook conditions and then calls
`req.workflows().deliver_event(...)` once per accepted delivery.

The GitHub App webhook subscription and any explicit environment
`webhookEvents` lists must also include `deployment` and `deployment_status`;
changing this provider's default does not alter the GitHub App subscription.

Ignored deliveries return `{"ok": true, "ignored": "<reason>"}` for:

- GitHub `ping` callbacks.
- Missing installation IDs.
- Missing or unsupported event types.
- Configured bot senders when `ignoreBotSender` is enabled.

Delivered workflow events use this shape:

```json
{
  "id": "github:<X-GitHub-Delivery>",
  "source": "github",
  "spec_version": "1.0",
  "type": "github.<X-GitHub-Event>.<action>",
  "subject": "repo:owner/repo",
  "datacontenttype": "application/json",
  "data": {
    "github": {
      "event_type": "pull_request",
      "action": "opened",
      "installation_id": 99,
      "repository": "owner/repo",
      "repository_owner": "owner",
      "repository_name": "repo",
      "sender": "octocat",
      "delivery_id": "delivery-123"
    },
    "raw": {}
  }
}
```

When `X-GitHub-Delivery` is missing, the event ID is `github:<sha256 digest>`.
When `X-GitHub-Event` is missing, the event type is inferred from the payload.
When the payload does not include `action`, the event type falls back to
`github.<event>`.
When a repository is absent, the subject is `installation:<id>`.

If delivery fails, `events.handle` returns an internal server error so GitHub
can retry delivery.

## Read Cache

The cache is disabled by default. When enabled, it stores exact, successful
responses for these target operations:

- `bot.getPullRequest`, `bot.getCheckRun`, and `bot.getWorkflowRun`
- `bot.listCheckSuiteCheckRuns`, `bot.listCommitCheckRuns`,
  `bot.listWorkflowRuns`, `bot.listIssueComments`, and
  `bot.listPullRequestsForCommit`
- `bot.searchPullRequests` and `bot.compareRefs`

Authorization, repository-installation lookup, and installation-token minting
remain live. A cache hit avoids the target REST or GraphQL request; it does not
mean the operation makes no GitHub requests at all. Exact paths, filters,
pagination parameters, GraphQL variables, API host, app ID, and installation
ID are part of the cache key.

Accepted webhooks and successful mutations invalidate affected generations.
Webhook projections are versioned so delayed deliveries cannot overwrite newer
state. Cache errors fail open to GitHub and never replace a successful GitHub
response with an error.

`maintenance.reconcileCache` is a hidden operation that replays up to 25
expired requests per run, refreshes drift, and prunes entries
older than 24 hours. Production rollout requires a dedicated service-account
subject, repository allowlist, `allowedOperations` entry, and a Gestalt
Workflow schedule every 15 minutes.

Structured logs use `github_cache_outcome` values `hit`, `miss`, `stale`,
`bypass`, `invalidate`, `reconcile`, and `error`. Logs include operation and
repository identifiers where applicable, but never tokens or cached payloads.

Roll out by deploying the provider default-off, configuring the IndexedDB
binding and reconciliation in development, then enabling the cache in staging.
Before production, verify IndexedDB tenant isolation/encryption, row and
payload volume against the 5,000-per-repository and 50,000-total response caps,
deployment webhook delivery IDs, and target-endpoint request reduction.
Rollback is `cacheEnabled: false`, which also disables reconciliation and
performs no cache storage calls.

## Operations

- `events.handle`
- `maintenance.reconcileCache` (hidden)
- `identity.linkSelf`
- `bot.getRepository`
- `bot.searchCode`
- `bot.getContent`
- `bot.commitFiles`
- `bot.openPullRequest`
- `bot.closePullRequest`
- `bot.createPullRequest`
- `bot.createPullRequestReview`
- `bot.listPullRequestReviews`
- `bot.listPullRequestReviewThreads`
- `bot.resolvePullRequestReviewThread`
- `bot.createPullRequestConversationComment`
- `bot.createIssueComment`
- `bot.addReaction`
- `bot.addLabels`
- `bot.removeLabels`
- `bot.requestReviewers`
- `bot.getPullRequest`
- `bot.listPullRequestFiles`
- `bot.getCheckRun`
- `bot.createCheckRun`
- `bot.updateCheckRun`
- `bot.listCheckSuiteCheckRuns`
- `bot.listCheckRunAnnotations`
- `bot.getWorkflowRun`
- `bot.listWorkflowRunJobs`
