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
  starts a Gestalt agent as a workload subject for the webhook installation, or
  publishes a Workflow event when `webhook.dispatch: workflow` is configured.
- `events.runAgentFromWorkflowEvent` is a hidden Workflow target that starts the
  webhook agent from a published `github.app.webhook` event.
- `bot.commitFiles` creates a commit on a branch using an installation access
  token.
- `bot.openPullRequest` opens a pull request using an installation access token.
- `bot.createPullRequest` commits file changes and opens a pull request in one
  operation.

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
      webhook:
        dispatch: direct
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

`webhook.dispatch` controls what happens after signature validation and event
filtering:

- `direct` starts the agent during the webhook HTTP request. This is the default
  for backward compatibility.
- `workflow` publishes one Workflow event and returns from the webhook request
  without starting the agent inline. Publish failures return a retryable 503.

Workflow dispatch publishes events with this interface:

```json
{
  "type": "github.app.webhook",
  "source": "github",
  "subject": "acme/widgets",
  "data": {
    "github_event": "pull_request",
    "github_action": "opened",
    "delivery_id": "<x-github-delivery>",
    "installation": {"id": 99},
    "repository": {"full_name": "acme/widgets"},
    "sender": {"login": "octocat"},
    "payload": {"action": "opened"}
  }
}
```

Configure one generic Workflow trigger to run the hidden wrapper:

```yaml
workflows:
  eventTriggers:
    github_app_webhook:
      provider: indexeddb
      match:
        type: github.app.webhook
        source: github
      target:
        plugin:
          name: github
          operation: events.runAgentFromWorkflowEvent
          input:
            _gestalt:
              eventRunPermissions:
                - plugin: github
                  operations:
                    - bot.commitFiles
                    - bot.openPullRequest
                    - bot.createPullRequest
```

## Bot Operation Interfaces

Bot write operations are scoped to the verified webhook workload subject:
`workload:github_app_installation:<installation_id>:repo:<owner>/<repo>`. If
`installation_id` is omitted, the operation uses the workload subject
installation. If it is supplied, it must match the workload subject. The target
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

## Documentation

- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
