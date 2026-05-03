# GitHub

Repository, issue, pull request, workflow, and code search operations.

## Configuration

Reference this provider in your Gestalt configuration:

```yaml
plugins:
  github:
    source: github.com/valon-technologies/gestalt-providers/plugins/github
    version: ...
```

See [Getting Started](https://gestaltd.ai/getting-started) and
[Configuration](https://gestaltd.ai/configuration).

## Capabilities

Hybrid provider with GitHub's OpenAPI and GraphQL surfaces plus GitHub App bot
operations. The OpenAPI surface exposes GitHub's REST API for repositories,
issues, pull requests, workflows, and code search. The GraphQL surface provides
access to GitHub's GraphQL API.

The OpenAPI and GraphQL surfaces authenticate with GitHub OAuth 2.0.

The source-backed bot operations use a configured GitHub App instead of a user
connection:

- `events.handle` receives signed GitHub App webhooks at `/github/event` and
  signals or starts a Workflow run for the webhook installation.
- `bot.commitFiles` creates a commit on a branch using an installation access
  token.
- `bot.openPullRequest` opens a pull request using an installation access token.
- `bot.createPullRequest` commits file changes and opens a pull request in one
  operation.
- `bot.createPullRequestConversationComment` creates a pull request conversation
  comment.
- `bot.createIssueComment` creates an issue comment.
- `bot.getCheckRun`, `bot.listCheckRunAnnotations`, `bot.getWorkflowRun`, and
  `bot.listWorkflowRunJobs` inspect CI failures using GitHub's Checks and
  Actions REST interfaces.

The bot operations do not require a GitHub user OAuth connection. The GitHub App
must be installed on the target repository and must have the permissions needed
for the action, typically Contents write for commits and Pull requests write for
pull requests.

## GitHub App Bot Configuration

Configure the GitHub App fields needed by the bot operations:

```yaml
plugins:
  github:
    source: github.com/valon-technologies/gestalt-providers/plugins/github
    version: ...
    config:
      appId: "123456"
      appPrivateKeyEnv: GITHUB_APP_PRIVATE_KEY
      workflow:
        provider: local
      agent:
        provider: simple
        model: gpt-5.4
        systemPrompt: Keep pull request changes small and explain what changed.
      webhookEvents:
        - pull_request
        - issue_comment
        - pull_request_review
      ignoreBotSender: true
```

`clientId` and `clientSecret` are only needed when using the OAuth-backed
OpenAPI or GraphQL surfaces. They are not required for webhook-triggered agent
turns or bot operations.

The private key can also be supplied with `appPrivateKey`,
`appPrivateKeyPath`, `GITHUB_APP_PRIVATE_KEY`, or
`GITHUB_APP_PRIVATE_KEY_PATH`.

`workflow.provider` is required because GitHub App webhooks always dispatch
through the configured Workflow provider. Without `webhookPolicies`, the
workflow run target is an agent target built from the same `agent` configuration
and the legacy write-capable GitHub bot tool refs.

The provider derives the GitHub App bot identity from the configured app. It
uses `/app` to read the app name and slug, then resolves `{slug}[bot]` to the
bot account when a no-reply co-author email is needed. You do not need to
configure the bot login, user ID, name, or email.

Webhook signature validation is configured separately from the regular
connection and reads the shared webhook secret from `GITHUB_WEBHOOK_SECRET`.
Configure the GitHub App webhook URL to the mounted provider endpoint:

```text
https://<gestalt-host>/api/v1/github/event
```

For GitHub Enterprise Server, set `apiBaseUrl` and `webBaseUrl`.

By default, webhook-triggered agents are started for `check_run`, `check_suite`,
`issue_comment`, `issues`, `pull_request`, `pull_request_review`,
`pull_request_review_comment`, and `workflow_run`. `push` is intentionally not
enabled by default so commits created by the bot do not recursively start new
agent turns. Set `webhookEvents` to override the allowlist.

Use `webhookPolicies` when webhook behavior should depend on the event. Policies
are evaluated in order and the first match selects the workflow provider
override, agent override, action mode, and exact bot operations exposed to the
agent. If `webhookPolicies` is present and no policy matches, the webhook is
acknowledged and ignored. If `webhookEvents` is also configured, it remains a
coarse app-level allowlist before policy selection; if it is omitted, policy
`match.events` controls event types.

```yaml
plugins:
  github:
    config:
      appId: "123456"
      appPrivateKeyEnv: GITHUB_APP_PRIVATE_KEY
      workflow:
        provider: local
      agent:
        provider: simple
        model: gpt-5.4
      webhookPolicies:
        - id: failed-ci-comment
          match:
            events: [check_run, workflow_run]
            actions: [completed]
            conclusions: [failure, timed_out, action_required]
            repositories: [acme/widgets]
            branches: [main]
            checkNames: ["Build Gestalt"]
            workflowNames: ["CI"]
          agent:
            model: gpt-5.4
            systemPrompt: Investigate failed CI and leave a concise PR comment.
          action:
            mode: comment
        - id: failed-ci-pr
          match:
            events: [check_run]
            conclusions: [failure]
          action:
            mode: pull_request
            allowedOperations:
              - bot.getCheckRun
              - bot.listCheckRunAnnotations
              - bot.createPullRequestConversationComment
              - bot.createPullRequest
```

Policy match fields are GitHub-shaped. Empty fields are wildcards, values within
a field are ORed, and fields are ANDed. Event matching prefers the
`X-GitHub-Event` header when present. `branches` matches PR head/base refs, CI
`head_branch`, and push refs. `action.mode` defaults operations as follows:
`observe` grants read-only CI tools, `comment` adds
`bot.createPullRequestConversationComment` and `bot.createIssueComment`,
`branch_commit` adds `bot.commitFiles`, and `pull_request` adds the comment,
commit, and pull request tools. Use the pull request conversation operation for
PRs and the issue comment operation for Issues. `allowedOperations` can narrow
or replace those defaults; an explicit empty list grants no tools.

After signature validation, the hosted HTTP binding invokes `events.handle`
before acknowledging the GitHub delivery. `events.handle` filters the event and calls
`WorkflowManager.SignalOrStartRun(provider_name=workflow.provider,
workflow_key="github:${installation_id}:${owner}/${repo}:${number}",
signal.name="github.app.webhook")` and returns from the webhook request after the
workflow signal has been durably enqueued. The agent still starts from the
workflow provider, not inline in the webhook handler. If enqueueing fails,
`events.handle` returns a retryable error so GitHub can redeliver the webhook.
The signal payload has this interface:

GitHub's delivery timeout is 10 seconds, so webhook handlers must keep the
enqueue path small and avoid starting agents inline. Treat non-2xx delivery
responses as retryable enqueue failures.

```json
{
  "github_event": "pull_request",
  "github_action": "opened",
  "delivery_id": "<x-github-delivery>",
  "installation": {"id": 99},
  "repository": {"full_name": "acme/widgets"},
  "sender": {"login": "octocat"},
  "webhook_policy": {
    "id": "failed-ci-comment",
    "mode": "comment",
    "tool_refs": ["bot.getCheckRun", "bot.createPullRequestConversationComment"]
  },
  "summary": {"repository": "acme/widgets", "number": 7},
  "agent_request": {
    "user_prompt": "GitHub App webhook:\n...",
    "subject": {"repository": "acme/widgets", "number": 7},
    "pull_request": {
      "number": 7,
      "title": "Update widgets",
      "state": "open",
      "html_url": "https://github.com/acme/widgets/pull/7",
      "head_ref": "feature",
      "base_ref": "main"
    }
  },
  "check_run": {
    "id": 123,
    "name": "Build Gestalt",
    "status": "completed",
    "conclusion": "failure",
    "html_url": "https://github.com/acme/widgets/runs/123"
  },
  "payload_sha256": "<payload digest>",
  "payload_omitted": true
}
```

Configure the agent's bot operation dependencies with plugin `invokes`:

```yaml
plugins:
  github:
    invokes:
      - plugin: github
        operation: bot.commitFiles
        credentialMode: none
      - plugin: github
        operation: bot.openPullRequest
        credentialMode: none
      - plugin: github
        operation: bot.createPullRequest
        credentialMode: none
      - plugin: github
        operation: bot.createPullRequestConversationComment
        credentialMode: none
      - plugin: github
        operation: bot.createIssueComment
        credentialMode: none
      - plugin: github
        operation: bot.getCheckRun
        credentialMode: none
      - plugin: github
        operation: bot.listCheckRunAnnotations
        credentialMode: none
      - plugin: github
        operation: bot.getWorkflowRun
        credentialMode: none
      - plugin: github
        operation: bot.listWorkflowRunJobs
        credentialMode: none
```

## Bot Operation Interfaces

Bot write operations are scoped to the verified webhook service account subject:
`service_account:github_app_installation:<installation_id>:repo:<owner>/<repo>`. If
`installation_id` is omitted, the operation uses the service account subject
installation. If it is supplied, it must match the service account subject. The target
`owner` and `repo` must also match the repository that produced the webhook.

Create or update a branch commit with `bot.commitFiles`:

```json
{
  "owner": "acme",
  "repo": "widgets",
  "message": "Update README",
  "branch": "gestalt/update-readme",
  "base_branch": "main",
  "files": [
    {
      "path": "README.md",
      "content": "hello\n",
      "executable": false
    },
    {
      "path": "bin/tool",
      "content_base64": "IyEvYmluL3NoCg==",
      "executable": true
    },
    {
      "path": "old.txt",
      "delete": true
    }
  ],
  "coauthors": [
    {
      "name": "Ada Lovelace",
      "email": "ada@example.com"
    }
  ],
  "include_bot_coauthor": true
}
```

Open an existing branch as a pull request with `bot.openPullRequest`:

```json
{
  "owner": "acme",
  "repo": "widgets",
  "title": "Update README",
  "head": "gestalt/update-readme",
  "base": "main",
  "body": "Updates the README from a GitHub App bot.",
  "draft": true
}
```

Commit files and open the pull request in one call with
`bot.createPullRequest`:

```json
{
  "owner": "acme",
  "repo": "widgets",
  "title": "Update README",
  "message": "Update README",
  "branch": "gestalt/update-readme",
  "base": "main",
  "body": "Updates the README from a GitHub App bot.",
  "draft": true,
  "files": [
    {
      "path": "README.md",
      "content": "hello\n"
    }
  ],
  "coauthors": [
    {
      "name": "Ada Lovelace",
      "email": "ada@example.com"
    }
  ]
}
```

Create a pull request conversation comment with
`bot.createPullRequestConversationComment`:

```json
{
  "owner": "acme",
  "repo": "widgets",
  "pull_number": 42,
  "body": "The failed check points at a snapshot mismatch in README.md."
}
```

Create an issue comment with `bot.createIssueComment`:

```json
{
  "owner": "acme",
  "repo": "widgets",
  "issue_number": 13,
  "body": "I can reproduce this issue."
}
```

Read CI state with the GitHub-shaped read operations:

```json
{"owner": "acme", "repo": "widgets", "check_run_id": 123}
```

```json
{"owner": "acme", "repo": "widgets", "run_id": 456, "filter": "all"}
```

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
