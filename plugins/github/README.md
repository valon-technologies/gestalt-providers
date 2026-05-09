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
`user.createPullRequest` also uses the caller's GitHub OAuth connection, but
keeps the file-change-to-pull-request protocol inside the provider so agents do
not need to compose low-level Git REST operations themselves.

The GitHub App bot operations use a configured GitHub App instead of a user
connection:

- `events.handle` receives signed GitHub App webhooks at `/github/event` and
  signals or starts a Workflow run for the webhook installation.
- `bot.commitFiles` creates a commit on a branch using an installation access
  token.
- `bot.openPullRequest` opens a pull request using an installation access token.
- `bot.closePullRequest` closes a pull request using an installation access
  token.
- `bot.createPullRequest` commits file changes and opens a pull request in one
  operation.
- `bot.createPullRequestReview` creates a pull request review with inline
  file/line comments.
- `bot.listPullRequestReviewThreads` lists PR review threads and their first
  comments.
- `bot.resolvePullRequestReviewThread` resolves a PR review thread after
  verifying it belongs to the requested pull request.
- `bot.createPullRequestConversationComment` creates a pull request conversation
  comment.
- `bot.createIssueComment` creates an issue comment.
- `bot.addReaction` adds a reaction to an issue, pull request, issue comment,
  or pull request review comment.
- `bot.addLabels` and `bot.removeLabels` update issue or pull request labels.
- `bot.requestReviewers` requests GitHub users or team slugs as PR reviewers.
- `bot.getPullRequest` and `bot.listPullRequestFiles` inspect pull request
  metadata and changed-file patches for inline review work.
- `bot.getCheckRun`, `bot.listCheckSuiteCheckRuns`,
  `bot.listCheckRunAnnotations`, `bot.getWorkflowRun`, and
  `bot.listWorkflowRunJobs` inspect CI failures using GitHub's Checks and
  Actions REST interfaces. Use `bot.listCheckSuiteCheckRuns` to expand a
  `check_suite` webhook into specific failed check runs.
- `bot.createCheckRun` and `bot.updateCheckRun` publish GitHub Checks status
  for deterministic reviewer workflows.

The bot operations do not require a GitHub user OAuth connection. The GitHub App
must be installed on the target repository and must have the permissions needed
for the action, typically Contents write for commits, Pull requests write for
pull requests and pull request reviews, and Checks write for check runs.

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

For GitHub Enterprise Server, set `apiBaseUrl` and `webBaseUrl`. The GitHub App
GraphQL URL used by review-thread operations defaults to
`https://api.github.com/graphql`; when `apiBaseUrl` ends in `/api/v3`, the
provider derives `/api/graphql`. Set `graphqlBaseUrl` only when that derivation
does not match the server.

By default, webhook-triggered agents are started for `check_run`, `check_suite`,
`issue_comment`, `issues`, `pull_request`, `pull_request_review`,
`pull_request_review_comment`, and `workflow_run`. `push` is intentionally not
enabled by default so commits created by the bot do not recursively start new
agent turns. Set `webhookEvents` to override the allowlist.

Use `webhookPolicies` when webhook behavior should depend on the event. Policies
are evaluated in order and the first match selects the workflow provider
override, optional workflow target, agent override, action mode, and exact bot
operations exposed to fallback agent targets. If `webhookPolicies` is present
and no policy matches, the webhook is acknowledged and ignored. If
`webhookEvents` is also configured, it remains a coarse app-level allowlist
before policy selection; if it is omitted, policy `match.events` controls event
types.

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
        - id: pr-review-workflow
          match:
            events: [pull_request]
            actions: [opened, synchronize, reopened, ready_for_review]
          workflow:
            provider: temporal
            target:
              plugin:
                plugin: github
                operation: reviewPullRequest
                input:
                  maxComments: 10
                  changedLinesOnly: true
        - id: failed-ci-comment
          match:
            events: [check_run, check_suite, workflow_run]
            actions: [completed]
            conclusions: [failure, timed_out, action_required]
            repositories: [acme/widgets]
            branches: [main]
            checkNames: ["Build Gestalt"]
            workflowNames: ["CI"]
          agent:
            model: gpt-5.4
            systemPrompt: Investigate failed CI and leave a concise PR comment.
          comments:
            suppressStaleHead: true
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
              - bot.listCheckSuiteCheckRuns
              - bot.listCheckRunAnnotations
              - bot.createPullRequestReview
              - bot.createPullRequestConversationComment
              - bot.createPullRequest
```

Policy match fields are GitHub-shaped. Empty fields are wildcards, values within
a field are ORed, and fields are ANDed. Event matching prefers the
`X-GitHub-Event` header when present. `branches` matches PR head/base refs, CI
`head_branch`, and push refs. `action.mode` defaults operations as follows:
`observe` grants read-only pull request and CI tools, `comment` adds
`bot.createPullRequestReview`, `bot.createPullRequestConversationComment`, and
`bot.createIssueComment`, `branch_commit` may add `bot.commitFiles` for
same-PR branch commits, and `pull_request` may add the comment, commit, and
pull request tools for follow-up PRs. Use
`bot.createPullRequestReview` for inline file/line PR review comments, the pull
request conversation operation for PR timeline comments, and the issue comment
operation for Issues. `allowedOperations` can narrow or replace those defaults;
an explicit empty list grants no tools. Reaction, label, reviewer-request, and
review-thread resolution tools are explicit opt-ins through `allowedOperations`.
Set `action.allowCodeReviewComments: false` to remove PR review comment tools
even when `mode` or `allowedOperations` would otherwise expose them. Set
`action.selfFixMode` to choose the maximum self-fix behavior allowed by static
configuration: `disabled` exposes no code-changing tools, `suggest` exposes no
code-changing tools but allows the agent to describe a patch, `branch_commit`
can expose `bot.commitFiles` for committing directly to the original PR branch
without pull request creation, and `pull_request` can expose commit and pull
request tools for opening follow-up PRs. `action.selfFixMode` defaults to
`disabled`; the older `action.allowSelfFix` remains a deprecated ceiling.

`actionPreferences` optionally layers per-subject preferences over those static
gates. The static config values remain hard ceilings: a stored preference can
disable inline review comments or self-fix for one identity, but it cannot grant
tools that `mode`, `allowedOperations`, `allowCodeReviewComments`, or
`selfFixMode` do not already allow. When `actionPreferences` is omitted, no
IndexedDB or authorization lookup is performed and the policy behaves exactly as
configured.

```yaml
providers:
  indexeddb:
    main:
      source: /absolute/path/to/gestalt-providers/indexeddb/relationaldb/manifest.yaml
      config:
        dsn: sqlite:///var/lib/gestalt/github-preferences.db

  ui:
    github:
      path: /github
      source: /absolute/path/to/gestalt-providers/ui/github/manifest.yaml

plugins:
  github:
    indexeddb:
      provider: main
    config:
      actionPreferences: {}
      webhookPolicies:
        - id: pr-review
          match:
            events: [pull_request, check_run]
          action:
            mode: pull_request
            allowCodeReviewComments: true
            selfFixMode: branch_commit
            preferenceSubject: pull_request_author
```

`actionPreferences: {}` enables the preference store and uses the provider's
default IndexedDB binding plus the provider-name-derived object store. Configure
`actionPreferences.indexeddb` only when the provider process has a non-default
named IndexedDB SDK binding.

`action.preferenceSubject` chooses which GitHub identity is used for the lookup:
`pull_request_author`, `comment_author`, or `sender`. The provider keys GitHub
identities by numeric user ID as `github_identity:user:<id>` and then
best-effort resolves a Gestalt subject through authorization. Lookup precedence
is repository + policy + GitHub external identity, then repository + policy +
Gestalt subject ID, then config defaults. If the store cannot be read with
`failureMode: config_default`, the webhook still dispatches with static config.

Callers can manage their own preference records with:

```json
POST github.actionPreferences.set
{
  "repository": "valon-technologies/gestalt-providers",
  "policy_id": "pr-review",
  "identity_kind": "external_subject_id",
  "allow_code_review_comments": false,
  "self_fix_mode": "suggest"
}
```

`actionPreferences.get` and `actionPreferences.delete` use the same identity
selection. These operations require a linked GitHub external identity for
`external_subject_id` or a human agent subject for `subject_id`; service-account
only calls are rejected.

Use `trigger`, `dedupe`, and `comments` to make webhook-triggered agents less
noisy:

```yaml
webhookPolicies:
  - id: failed-ci-review
    match:
      events: [check_run, check_suite, workflow_run]
      actions: [completed]
      conclusions: [failure, timed_out, action_required]
    trigger:
      frequency: once_per_ci_incident
      includeDrafts: false
    dedupe:
      scope: ci_incident
    comments:
      timelinePolicy: actionable_only
      inlinePolicy: findings_only
      suppressStaleHead: true
    action:
      mode: comment
      allowCodeReviewComments: false
      selfFixMode: disabled
  - id: manual-pr-review
    match:
      events: [issue_comment, pull_request_review_comment]
      actions: [created]
    trigger:
      frequency: manual_only
      manualCommands: ["gestalt review"]
      manualCommandMatch: exact
    dedupe:
      scope: pr_head
    comments:
      timelinePolicy: never
      inlinePolicy: findings_only
    action:
      mode: comment
```

`trigger.frequency` controls the signal idempotency key:
`every_delivery` preserves the legacy per-delivery behavior, `once_per_pr`
coalesces equivalent turns for the same PR, `once_per_head_sha` coalesces for a
PR and head SHA, `once_per_ci_incident` coalesces failed CI events for the same
PR and head SHA, and `manual_only` only matches configured comment commands.
`trigger.manualCommandMatch: exact` normalizes whitespace and case but rejects
suffixes such as `gestalt review verbose=true`; `contains` keeps the legacy
substring behavior.
`dedupe.scope` controls the workflow key independently, with the same PR/head/CI
shapes plus the legacy `delivery` scope. If a requested PR or head SHA is absent
from a payload, the provider falls back to the legacy event-shaped key so it
does not accidentally merge unrelated PRs or treat a plain issue as a PR.

`comments.timelinePolicy: never` removes PR conversation and issue comment tools
from the effective tool list. `comments.inlinePolicy: never` removes
`bot.createPullRequestReview`. `actionable_only` and `findings_only` keep the
tools available but add stricter agent guidance to avoid acknowledgement-only
timeline comments and unanchored inline comments. These policy values are local
provider semantics rather than GitHub SDK enum names; GitHub's REST and GraphQL
APIs do not expose matching configuration enums for bot review noisiness. For
built-in GitHub workflow targets, the provider rejects contradictory
configuration such as `comments.inlinePolicy: never` with
`workflow.target.plugin.operation: reviewPullRequest`. It also rejects
`action.allowCodeReviewComments: false` for that built-in review target because
the target's purpose is to post validated inline review comments.
For CI webhook events, `comments.suppressStaleHead: true` fetches the current PR
head before dispatch and ignores the event when the failed CI SHA is no longer
the PR head SHA.

Compatibility note: existing policies that use `action.mode` without explicit
`allowedOperations` now expose `bot.getPullRequest`,
`bot.listPullRequestFiles`, `bot.listCheckSuiteCheckRuns`, and, for
comment-capable modes, `bot.createPullRequestReview`. Add an explicit
`allowedOperations` list to keep previous CI-read-only or timeline-only comment
behavior.

`reviewPullRequest` adds hidden provider markers only to inline comments it
creates itself. Before posting, it lists unresolved bot-owned marked threads and
suppresses exact or materially identical findings that are already present, so
it does not repost the same issue on every delivery. By default, after a
successful current review, or after an agent run produces zero valid findings,
it resolves older marked threads whose fingerprint is no longer present in the
current findings. Set `autoResolveStaleFindings: false` on the
`reviewPullRequest` input to leave old marked threads open. `reviewPullRequest`
always publishes a GitHub check run and completes it as success, skipped, or
failure with the review result. For webhook-backed built-in review targets, the
provider creates the in-progress check run before enqueueing the workflow so the
GitHub pull request shows a pending review job while the workflow waits or runs.
The low-level `bot.createPullRequestReview` operation never adds hidden markers
unless the caller supplies them explicitly, and
`bot.resolvePullRequestReviewThread` is not included in any default policy mode;
expose it with `allowedOperations` only for workflows that should be able to
resolve threads directly.

Set `workflow.target.plugin` on a policy to dispatch the matched webhook to a
deterministic workflow/plugin target instead of the generated agent target. The
target `input` is static configuration only; webhook event details are delivered
through the workflow signal payload and are not merged into `input`. Use
`plugin: github` with `operation: reviewPullRequest` for the built-in
workflow-backed PR reviewer; it fetches the PR, validates agent findings against
changed RIGHT-side diff lines, and posts one inline review only when it has
line-anchored findings. It rejects draft pull requests and only supports exact
manual `gestalt review`-style comment triggers when invoked from
`issue_comment` signals. Workflow providers derive plugin-target access from the
target plugin plus optional `_gestalt.eventRunPermissions` entries in target
input; include extra permissions only when a target operation calls other
plugins through the host invoker.

After signature validation, the hosted HTTP binding invokes `events.handle`
before acknowledging the GitHub delivery. `events.handle` filters the event and
calls `WorkflowManager.SignalOrStartRun(provider_name=workflow.provider,
workflow_key="github:${installation_id}:${owner}/${repo}:${number}",
signal.name="github.app.webhook")` and returns from the webhook request after
the workflow signal has been durably enqueued. The configured workflow target
starts from the workflow provider; when no policy target is configured, the
generated agent target is used. If enqueueing fails, `events.handle` returns a
retryable error so GitHub can redeliver the webhook. The signal payload has this
interface:

GitHub's delivery timeout is 10 seconds, so webhook handlers must keep the
enqueue path small and avoid starting agents inline. Treat non-2xx delivery
responses as retryable enqueue failures.

For `workflow.target.plugin.operation: reviewPullRequest`, `events.handle`
creates or reuses the review check run before calling
`WorkflowManager.SignalOrStartRun`. If the check run cannot be created, the
workflow is not enqueued. If enqueueing fails after creating the check run, the
provider best-effort completes that check as failure before returning the
retryable error.

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
    "tool_refs": ["bot.getCheckRun", "bot.createPullRequestReview"]
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
  "review_check_run": {
    "id": 456,
    "name": "Gestalt Review",
    "status": "in_progress",
    "external_id": "gestalt-review:<hash>",
    "head_sha": "abc123",
    "html_url": "https://github.com/acme/widgets/runs/456"
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
        operation: bot.createPullRequestReview
        credentialMode: none
      - plugin: github
        operation: bot.createPullRequestConversationComment
        credentialMode: none
      - plugin: github
        operation: bot.createIssueComment
        credentialMode: none
      - plugin: github
        operation: bot.addReaction
        credentialMode: none
      - plugin: github
        operation: bot.addLabels
        credentialMode: none
      - plugin: github
        operation: bot.removeLabels
        credentialMode: none
      - plugin: github
        operation: bot.requestReviewers
        credentialMode: none
      - plugin: github
        operation: bot.getPullRequest
        credentialMode: none
      - plugin: github
        operation: bot.listPullRequestFiles
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

Bot write operations prefer the invocation external identity
`{ "type": "github_app_installation", "id": "repo:<owner>/<repo>" }` and resolve the
GitHub App installation from the repository. During migration they still accept the
legacy webhook service account subject
`service_account:github_app_installation:<installation_id>:repo:<owner>/<repo>` when
no external identity is present. If `installation_id` is supplied, it must match the
resolved installation. The target `owner` and `repo` must also match the scoped
repository identity.

This requires a Gestalt host/SDK that forwards `Request.external_identity`. The
provider intentionally does not use `Request.agent_external_identity` for these
operations because that field identifies the original agent caller, not the
GitHub App installation that the run-as subject is authorized to assume.

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

Commit files and open the pull request as the connected GitHub user with
`user.createPullRequest`. The provider resolves the commit author and committer
from `/user`, falls back to GitHub's no-reply email format when the account has
no public email, and appends the configured GitHub App bot as a co-author by
default:

```json
{
  "owner": "acme",
  "repo": "widgets",
  "title": "Update README",
  "message": "Update README",
  "branch": "gestalt/update-readme",
  "base": "main",
  "body": "Updates the README from the connected GitHub user.",
  "draft": true,
  "files": [
    {
      "path": "README.md",
      "content": "hello\n"
    }
  ],
  "include_bot_coauthor": true
}
```

Create a pull request review with inline file/line comments using
`bot.createPullRequestReview`:

```json
{
  "owner": "acme",
  "repo": "widgets",
  "pull_number": 42,
  "body": "Bugbot review: I found two concrete issues.",
  "commit_id": "ecdd80bb57125d7ba9641ffaa4d7d2c19d3f3091",
  "comments": [
    {
      "path": "src/widget.py",
      "line": 27,
      "side": "RIGHT",
      "body": "This branch can throw when config is missing."
    },
    {
      "path": "src/widget.py",
      "start_line": 41,
      "start_side": "RIGHT",
      "line": 45,
      "side": "RIGHT",
      "body": "This loop skips empty inputs."
    }
  ]
}
```

Inspect a pull request before creating inline comments with
`bot.getPullRequest` and `bot.listPullRequestFiles`:

```json
{"owner": "acme", "repo": "widgets", "pull_number": 42}
```

```json
{"owner": "acme", "repo": "widgets", "pull_number": 42, "per_page": 100, "page": 1}
```

`bot.listPullRequestFiles` returns each changed file's `filename`, `status`,
optional `previous_filename`, change counts, file URLs, exact bounded `patch`,
`patch_truncated`, and `patch_limit`.

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

React to an issue, PR, issue comment, or PR review comment with
`bot.addReaction`. PR reactions use `subject_type: "pull_request"` with
`pull_number`:

```json
{
  "owner": "acme",
  "repo": "widgets",
  "subject_type": "pull_request_review_comment",
  "comment_id": 123456,
  "content": "eyes"
}
```

Add and remove labels with `bot.addLabels` and `bot.removeLabels`. Use
`subject_type: "issue"` with `issue_number`, or `subject_type: "pull_request"`
with `pull_number`:

```json
{
  "owner": "acme",
  "repo": "widgets",
  "subject_type": "pull_request",
  "pull_number": 42,
  "labels": ["needs-review", "bug"]
}
```

Request reviewers with `bot.requestReviewers`. `team_reviewers` values are team
slugs:

```json
{
  "owner": "acme",
  "repo": "widgets",
  "pull_number": 42,
  "reviewers": ["octocat"],
  "team_reviewers": ["backend"]
}
```

Resolve an inline review thread with `bot.resolvePullRequestReviewThread` when a
stale thread should be resolved. `thread_id` is the GitHub GraphQL
`PullRequestReviewThread.id`, not a REST review comment `node_id`:

```json
{
  "owner": "acme",
  "repo": "widgets",
  "pull_number": 42,
  "thread_id": "PRRT_kwDOQ5WrUs5_h4lx"
}
```

Read CI state with the GitHub-shaped read operations:

```json
{"owner": "acme", "repo": "widgets", "check_run_id": 123}
```

```json
{"owner": "acme", "repo": "widgets", "run_id": 456, "filter": "all"}
```

## Configuration Reference

Use this provider from a Gestalt configuration entry like:

```yaml
plugins:
  github:
    source: github.com/valon-technologies/gestalt-providers/plugins/github
    version: ...
    config:
      clientId: ${GITHUB_CLIENT_ID}
      clientSecret: ${GITHUB_CLIENT_SECRET}
      appId: ${GITHUB_APP_ID}
      appPrivateKey: ${GITHUB_APP_PRIVATE_KEY}
      appPrivateKeyEnv: ${GITHUB_APP_PRIVATE_KEY}
      appPrivateKeyPath: ${GITHUB_APP_PRIVATE_KEY_PATH}
      apiBaseUrl: api.example.com
      graphqlBaseUrl: api.example.com
      webBaseUrl: api.example.com
      webhookEvents: ...
      webhookPolicies: ...
      workflow: ...
      actionPreferences: ...
      ignoreBotSender: ...
      agent: ...
```

Provider config fields:

- `clientId` (optional): GitHub OAuth client ID.
- `clientSecret` (optional): GitHub OAuth client secret.
- `appId` (optional): GitHub App ID used for bot webhooks and installation-token API calls.
- `appPrivateKey` (optional): PEM-encoded GitHub App private key. GITHUB_APP_PRIVATE_KEY is also supported.
- `appPrivateKeyEnv` (optional): Environment variable containing the PEM-encoded GitHub App private key.
- `appPrivateKeyPath` (optional): Filesystem path to the PEM-encoded GitHub App private key. GITHUB_APP_PRIVATE_KEY_PATH is also supported.
- `apiBaseUrl` (optional): GitHub REST API base URL. Defaults to https://api.github.com.
- `graphqlBaseUrl` (optional): GitHub GraphQL API URL for GitHub App bot operations. Defaults to https://api.github.com/graphql. For GitHub Enterprise Server, it is derived from apiBaseUrl when omitted.
- `webBaseUrl` (optional): GitHub web base URL used to build commit links. Defaults to https://github.com.
- `webhookEvents` (optional): GitHub webhook event types that should signal workflow runs. Defaults to check_run, check_suite, issue_comment, issues, pull_request, pull_request_review, pull_request_review_comment, and workflow_run.
- `webhookPolicies` (optional): Ordered webhook policies. When present, the first matching policy selects the workflow provider, optional workflow target, and GitHub bot operations exposed to fallback agent targets. If no policy matches, the webhook is acknowledged and ignored.
- `workflow` (required): Workflow provider used for GitHub App webhook dispatch.
- `actionPreferences` (optional): Optional IndexedDB-backed per-subject action preferences for GitHub webhook policies. When omitted, config-only policy gates are used.
- `ignoreBotSender` (optional): Ignore webhook payloads sent by the derived GitHub App bot login. Defaults to true.
- `agent` (optional): Agent configuration for GitHub App webhook-triggered turns.

Connections and authentication:

- `default` uses OAuth 2.0.
  - Requested scopes: `repo`.

Operation surfaces: OpenAPI, GraphQL.

Representative operations include:

- `bot.getPullRequest`
- `bot.resolveInstallation`
- `events.handle`
- `reviewPullRequest`
- `actionPreferences.get`
- `actionPreferences.listTargets`
- `actionPreferences.set`
- `actionPreferences.delete`
- `bot.commitFiles`
- `bot.openPullRequest`
- `user.createPullRequest`

- Bot operations run as the configured GitHub App installation and are scoped to the repository that produced or resolved the installation subject.
- `user.createPullRequest` runs as the connected GitHub OAuth user and should not be exposed through `credentialMode: none` run-as service accounts or GitHub App webhook self-fix policies.

## Usage Examples

Grant another provider or workflow permission to invoke this plugin before calling it:

```yaml
plugins:
  example_consumer:
    invokes:
      - plugin: github
        operation: bot.getPullRequest
```

Example `bot.getPullRequest` call:

```ts
await invoker.invoke("github", "bot.getPullRequest", { owner: "acme", repo: "widgets", pull_number: 42 });
```

Example `bot.resolveInstallation` call:

```ts
await invoker.invoke("github", "bot.resolveInstallation", { owner: "acme", repo: "widgets" });
```

## Documentation
- [Provider Development](https://gestaltd.ai/providers)
- [Manifest Reference](https://gestaltd.ai/reference/plugin-manifests)
