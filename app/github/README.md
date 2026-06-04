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
  `check_run`, `check_suite`, `issue_comment`, `issues`, `pull_request`,
  `pull_request_review`, `pull_request_review_comment`, and `workflow_run`.
- `workflow.provider`: workflow provider that receives delivered events.
- `ignoreBotSender`: ignore events sent by the configured GitHub App bot login.
  Defaults to `true`.

## Webhook Events

`events.handle` validates generic webhook conditions and then calls
`req.workflows().deliver_event(...)` once per accepted delivery.

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
  "type": "github.<X-GitHub-Event>",
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
When a repository is absent, the subject is `installation:<id>`.

If delivery fails, `events.handle` returns an internal server error so GitHub
can retry delivery.

## Operations

- `events.handle`
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
