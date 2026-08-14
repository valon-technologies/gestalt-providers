/**
 * Seeded Slack + workflow API fixtures for /local-dev (Vite) and optional
 * static preview (`serve-workflows-local.mjs`).
 *
 * One handler owns the mock routes so Vite DEV and any static server stay aligned.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const SLACK_APP = "slack";
export const WORKFLOWS_LOCAL_MOCK_ENV = "GESTALT_WORKFLOWS_LOCAL_MOCK";
export const GESTALT_THEME_FILE_ENV = "GESTALT_THEME_FILE";
export const GESTALT_THEME_ASSETS_DIR_ENV = "GESTALT_THEME_ASSETS_DIR";

/**
 * Resolve a live tenant stylesheet for /theme.css (same contract as gestaltd).
 * Prefer GESTALT_THEME_FILE, then an optional gitignored local mirror at
 * `.local/tenant-theme/theme.css` (materialize from your deployment repo).
 */
export function resolveTenantThemeFile(env = process.env) {
  const explicit = env[GESTALT_THEME_FILE_ENV]?.trim();
  if (explicit && fs.existsSync(explicit) && fs.statSync(explicit).isFile()) {
    return path.resolve(explicit);
  }

  const worktreeRoot = path.resolve(
    path.dirname(path.dirname(fileURLToPath(import.meta.url))),
    "../..",
  );
  const synced = path.join(worktreeRoot, ".local", "tenant-theme", "theme.css");
  if (fs.existsSync(synced) && fs.statSync(synced).isFile()) {
    return synced;
  }
  return null;
}

export function resolveTenantThemeAssetsDir(
  themeFile = resolveTenantThemeFile(),
  env = process.env,
) {
  const explicit = env[GESTALT_THEME_ASSETS_DIR_ENV]?.trim();
  if (explicit && fs.existsSync(explicit) && fs.statSync(explicit).isDirectory()) {
    return path.resolve(explicit);
  }
  if (!themeFile) return null;
  const dir = path.dirname(themeFile);
  return fs.existsSync(dir) ? dir : null;
}

function contentTypeForThemeAsset(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  switch (ext) {
    case ".css":
      return "text/css; charset=utf-8";
    case ".woff2":
      return "font/woff2";
    case ".woff":
      return "font/woff";
    case ".ttf":
      return "font/ttf";
    case ".otf":
      return "font/otf";
    case ".svg":
      return "image/svg+xml";
    case ".png":
      return "image/png";
    default:
      return "application/octet-stream";
  }
}

/**
 * Serve production-shaped tenant theme routes for local-dev.
 * @returns {boolean} true when handled
 */
export function handleTenantThemeRequest(req, res, pathname, env = process.env) {
  const method = req.method || "GET";
  if (method !== "GET" && method !== "HEAD") return false;

  if (pathname === "/theme.css") {
    const themeFile = resolveTenantThemeFile(env);
    if (!themeFile) {
      res.statusCode = 200;
      res.setHeader("Content-Type", "text/css; charset=utf-8");
      res.end(
        "/* local-dev: no GESTALT_THEME_FILE found */\n",
      );
      return true;
    }
    res.statusCode = 200;
    res.setHeader("Content-Type", "text/css; charset=utf-8");
    if (method === "HEAD") {
      res.end();
      return true;
    }
    res.end(fs.readFileSync(themeFile));
    return true;
  }

  if (!pathname.startsWith("/theme/")) return false;

  const assetsDir = resolveTenantThemeAssetsDir(undefined, env);
  if (!assetsDir) {
    res.statusCode = 404;
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    res.end("Not Found");
    return true;
  }

  const relative = decodeURIComponent(pathname.slice("/theme/".length));
  if (!relative || relative.includes("\0") || path.isAbsolute(relative)) {
    res.statusCode = 400;
    res.end("Bad Request");
    return true;
  }

  const resolvedAssets = path.resolve(assetsDir);
  const candidate = path.resolve(resolvedAssets, relative);
  if (
    candidate !== resolvedAssets &&
    !candidate.startsWith(resolvedAssets + path.sep)
  ) {
    res.statusCode = 403;
    res.end("Forbidden");
    return true;
  }
  if (!fs.existsSync(candidate) || !fs.statSync(candidate).isFile()) {
    res.statusCode = 404;
    res.setHeader("Content-Type", "text/plain; charset=utf-8");
    res.end("Not Found");
    return true;
  }

  res.statusCode = 200;
  res.setHeader("Content-Type", contentTypeForThemeAsset(candidate));
  if (method === "HEAD") {
    res.end();
    return true;
  }
  res.end(fs.readFileSync(candidate));
  return true;
}

const session = {
  subjectId: "user:test@gestalt.dev",
  email: "test@gestalt.dev",
  displayName: "Test User",
};

const authInfo = {
  provider: "test-sso",
  displayName: "Test SSO",
  loginSupported: true,
  features: { workflowDefaultProvider: "basic" },
};

const integrations = [
  {
    name: SLACK_APP,
    displayName: "Slack",
    managementPath: `/apps/${SLACK_APP}/admin`,
    sourceTreeUrl:
      "https://github.com/example-org/example-app/tree/main/apps/slack",
  },
];

const registry = {
  app: SLACK_APP,
  registry: "example-registry",
  knownVersions: [],
  publishedVersions: [],
  selectionDisabled: false,
  autoDeploy: { enabled: false },
};

/** Seeded app-admin members roster for /apps/slack/admin/members. */
const members = [
  {
    email: "alice@example.com",
    role: "admin",
    source: "static",
    mutable: false,
    effective: true,
    selectorKind: "subject_id",
    selectorValue: "user:alice-example",
    subjectId: "user:alice-example",
  },
  {
    email: "bob@example.com",
    role: "admin",
    source: "static",
    mutable: false,
    effective: true,
    selectorKind: "subject_id",
    selectorValue: "user:bob-example",
    subjectId: "user:bob-example",
  },
  {
    email: "carol@example.com",
    role: "viewer",
    source: "static",
    mutable: false,
    effective: true,
    selectorKind: "subject_id",
    selectorValue: "user:carol-example",
    subjectId: "user:carol-example",
  },
  {
    email: "shadowed@example.com",
    role: "viewer",
    source: "dynamic",
    mutable: true,
    effective: false,
    shadowedBy: "static viewer grant",
    selectorKind: "subject_id",
    selectorValue: "user:shadowed-example",
    subjectId: "user:shadowed-example",
  },
];

/**
 * Seeded service-account grants for /apps/slack/admin/identities.
 * Kept separate from members — matches gestaltd members/identities partition.
 */
const identities = [
  {
    subjectId: "service_account:slack-bot",
    displayName: "slack-bot",
    role: "viewer",
    source: "dynamic",
    mutable: true,
    effective: true,
  },
];

/** Mirrors WorkflowDefinition + WorkflowRun from gestalt workflow.proto. */
const definitions = [
  {
    id: "app_slack_notify",
    provider: "basic",
    generation: 3,
    paused: false,
    runAs: "service_account:slack-bot",
    createdAt: "2026-07-15T10:00:00Z",
    updatedAt: "2026-08-01T12:00:00Z",
    target: {
      steps: [
        {
          id: "post",
          app: {
            name: SLACK_APP,
            operation: "chat.postMessage",
            connection: "default",
          },
          inputs: {
            channel: { literal: "#ops" },
            text: { template: "Morning standup reminder" },
          },
          timeoutSeconds: 30,
        },
      ],
    },
    activations: [
      {
        id: "morning",
        paused: false,
        trigger: { kind: "schedule", cron: "0 9 * * 1-5", timezone: "America/New_York" },
      },
      {
        id: "manual_only",
        paused: true,
        trigger: { kind: "schedule", cron: "0 0 1 1 *", timezone: "UTC" },
      },
    ],
  },
  {
    id: "app_slack_incident_triage",
    provider: "basic",
    generation: 7,
    paused: false,
    runAs: "service_account:workflow-runner",
    createdAt: "2026-06-01T08:00:00Z",
    updatedAt: "2026-08-02T09:00:00Z",
    target: {
      steps: [
        {
          id: "diagnose",
          app: {
            name: "datadog",
            operation: "monitors.get",
            connection: "prod",
          },
          inputs: {
            monitorId: { input: { path: "monitor_id" } },
          },
          timeoutSeconds: 60,
        },
        {
          id: "notify",
          app: {
            name: SLACK_APP,
            operation: "chat.postMessage",
            connection: "default",
          },
          inputs: {
            channel: { literal: "#incidents" },
            text: {
              template:
                "Monitor {{ steps.diagnose.output.name }} is alerting",
            },
          },
          when: {
            value: { stepOutput: { stepId: "diagnose", path: "overall_state" } },
            equals: "Alert",
          },
          timeoutSeconds: 30,
        },
      ],
    },
    activations: [
      {
        id: "datadog_alert",
        paused: false,
        trigger: {
          kind: "event",
          eventType: "datadog.monitor.alert",
          eventSource: "datadog",
          eventSubject: "monitor",
        },
      },
    ],
  },
  {
    id: "app_slack_welcome_dm",
    provider: "basic",
    generation: 1,
    paused: true,
    runAs: "service_account:slack-bot",
    createdAt: "2026-08-03T11:00:00Z",
    updatedAt: "2026-08-03T11:00:00Z",
    target: {
      steps: [
        {
          id: "dm",
          app: {
            name: SLACK_APP,
            operation: "chat.postMessage",
            connection: "default",
          },
          inputs: {
            channel: { input: { path: "user_id" } },
            text: { literal: "Welcome to the workspace!" },
          },
        },
      ],
    },
    activations: [],
  },
];

const runs = [
  {
    id: "run_incident_1",
    provider: "basic",
    status: "succeeded",
    definitionId: "app_slack_incident_triage",
    definitionGeneration: 7,
    workflowKey: "incident:monitor-42",
    targetApp: SLACK_APP,
    target: {
      steps: [
        {
          id: "diagnose",
          app: { name: "datadog", operation: "monitors.get", connection: "prod" },
        },
        {
          id: "notify",
          app: {
            name: SLACK_APP,
            operation: "chat.postMessage",
            connection: "default",
          },
        },
      ],
    },
    trigger: {
      kind: "event",
      activationId: "datadog_alert",
      event: {
        id: "evt_dd_1",
        type: "datadog.monitor.alert",
        source: "datadog",
        subject: "monitor",
        time: "2026-08-03T17:59:50Z",
        data: { monitor_id: "42", overall_state: "Alert" },
      },
    },
    input: { monitor_id: "42" },
    createdBy: { subjectId: "service_account:workflow-runner" },
    runAs: "service_account:workflow-runner",
    createdAt: "2026-08-03T18:00:00Z",
    startedAt: "2026-08-03T18:00:05Z",
    completedAt: "2026-08-03T18:01:12Z",
    statusMessage: "completed",
    output: { notified: true, channel: "#incidents" },
    steps: [
      {
        stepId: "diagnose",
        status: "succeeded",
        startedAt: "2026-08-03T18:00:05Z",
        completedAt: "2026-08-03T18:00:40Z",
        output: { name: "API latency", overall_state: "Alert" },
        attempts: [
          {
            id: "att_1",
            status: "succeeded",
            startedAt: "2026-08-03T18:00:05Z",
            completedAt: "2026-08-03T18:00:40Z",
            output: { name: "API latency", overall_state: "Alert" },
          },
        ],
      },
      {
        stepId: "notify",
        status: "succeeded",
        startedAt: "2026-08-03T18:00:41Z",
        completedAt: "2026-08-03T18:01:12Z",
        output: { ok: true, ts: "1722704472.000100" },
      },
    ],
  },
  {
    id: "run_pending_1",
    provider: "basic",
    status: "pending",
    definitionId: "app_slack_notify",
    definitionGeneration: 3,
    targetApp: SLACK_APP,
    target: {
      steps: [
        {
          id: "post",
          app: {
            name: SLACK_APP,
            operation: "chat.postMessage",
            connection: "default",
          },
        },
      ],
    },
    trigger: {
      kind: "schedule",
      activationId: "morning",
      scheduledFor: "2026-08-03T13:00:00Z",
    },
    input: {},
    runAs: "service_account:slack-bot",
    createdAt: "2026-08-03T19:00:00Z",
  },
  {
    id: "run_failed_1",
    provider: "basic",
    status: "failed",
    definitionId: "app_slack_notify",
    definitionGeneration: 3,
    targetApp: SLACK_APP,
    target: {
      steps: [
        {
          id: "post",
          app: {
            name: SLACK_APP,
            operation: "chat.postMessage",
            connection: "default",
          },
        },
      ],
    },
    trigger: { kind: "manual" },
    input: { channel: "#ops" },
    runAs: "service_account:slack-bot",
    createdAt: "2026-08-03T17:30:00Z",
    startedAt: "2026-08-03T17:30:01Z",
    completedAt: "2026-08-03T17:30:08Z",
    statusMessage: "chat.postMessage returned 429",
    currentStepId: "post",
    steps: [
      {
        stepId: "post",
        status: "failed",
        startedAt: "2026-08-03T17:30:01Z",
        completedAt: "2026-08-03T17:30:08Z",
        statusMessage: "rate_limited",
        attempts: [
          {
            id: "att_fail_1",
            status: "failed",
            statusMessage: "HTTP 429",
            startedAt: "2026-08-03T17:30:01Z",
            completedAt: "2026-08-03T17:30:08Z",
          },
        ],
      },
    ],
  },
  {
    id: "run_running_1",
    provider: "basic",
    status: "running",
    definitionId: "app_slack_incident_triage",
    definitionGeneration: 7,
    workflowKey: "incident:monitor-99",
    targetApp: SLACK_APP,
    target: {
      steps: [
        {
          id: "diagnose",
          app: { name: "datadog", operation: "monitors.get", connection: "prod" },
        },
        {
          id: "notify",
          app: {
            name: SLACK_APP,
            operation: "chat.postMessage",
            connection: "default",
          },
        },
      ],
    },
    trigger: {
      kind: "event",
      activationId: "datadog_alert",
      event: {
        type: "datadog.monitor.alert",
        source: "datadog",
        data: { monitor_id: "99" },
      },
    },
    input: { monitor_id: "99" },
    runAs: "service_account:workflow-runner",
    createdAt: "2026-08-03T19:10:00Z",
    startedAt: "2026-08-03T19:10:02Z",
    currentStepId: "diagnose",
    steps: [
      {
        stepId: "diagnose",
        status: "running",
        startedAt: "2026-08-03T19:10:02Z",
      },
      { stepId: "notify", status: "pending" },
    ],
  },
  {
    id: "run_parallel_preview",
    provider: "basic",
    status: "succeeded",
    definitionId: "app_slack_incident_triage",
    definitionGeneration: 7,
    targetApp: SLACK_APP,
    target: {
      steps: [
        {
          id: "diagnose",
          app: { name: "datadog", operation: "monitors.get", connection: "prod" },
        },
        {
          id: "notify",
          app: {
            name: SLACK_APP,
            operation: "chat.postMessage",
            connection: "default",
          },
        },
      ],
    },
    trigger: { kind: "manual" },
    createdAt: "2026-08-04T14:00:00Z",
    startedAt: "2026-08-04T14:00:01Z",
    completedAt: "2026-08-04T14:02:10Z",
    statusMessage: "UI preview: parallel job group (not used by backend yet)",
    steps: [
      { stepId: "diagnose", status: "succeeded" },
      { stepId: "notify", status: "succeeded" },
    ],
    /** Forward-compat graph: sequential gate → parallel checks → sequential notify. */
    stages: [
      {
        id: "gate",
        kind: "sequential",
        jobs: [
          {
            id: "changes",
            name: "changes",
            status: "succeeded",
            durationMs: 10000,
            steps: [
              {
                id: "detect",
                name: "Detect changed paths",
                status: "succeeded",
                durationMs: 3000,
              },
              {
                id: "plan",
                name: "Plan jobs",
                status: "succeeded",
                durationMs: 7000,
              },
            ],
          },
        ],
      },
      {
        id: "checks",
        kind: "parallel",
        jobs: [
          {
            id: "ui-tests",
            name: "UI Driver Tests",
            status: "skipped",
            durationMs: 0,
            steps: [
              {
                id: "skip",
                name: "Skip (no UI changes)",
                status: "skipped",
                durationMs: 0,
              },
            ],
          },
          {
            id: "validate",
            name: "Validate source artifacts",
            status: "succeeded",
            durationMs: 111000,
            steps: [
              {
                id: "checkout",
                name: "Checkout",
                status: "succeeded",
                durationMs: 4000,
              },
              {
                id: "validate",
                name: "Validate",
                status: "succeeded",
                durationMs: 107000,
              },
            ],
          },
          {
            id: "lockfile",
            name: "Check deploy lockfile",
            status: "succeeded",
            durationMs: 56000,
            steps: [
              {
                id: "check",
                name: "Check lockfile",
                status: "succeeded",
                durationMs: 56000,
              },
            ],
          },
        ],
      },
      {
        id: "notify-stage",
        kind: "sequential",
        jobs: [
          {
            id: "notify",
            name: "notify",
            status: "succeeded",
            durationMs: 5000,
            steps: [
              {
                id: "post",
                name: "slack.chat.postMessage",
                status: "succeeded",
                durationMs: 5000,
              },
            ],
          },
        ],
      },
    ],
  },
];

/** GHA-style collapsible stdout groups keyed by run/job/step. */
const stepLogs = {
  "run_incident_1/workflow/diagnose": {
    groups: [
      {
        id: "setup",
        name: "Set up step",
        status: "succeeded",
        durationMs: 1200,
        defaultCollapsed: true,
        lines: [
          { number: 1, text: "Current runner: workflow-indexeddb" },
          { number: 2, text: "Preparing app invocation datadog.monitors.get" },
        ],
      },
      {
        id: "run",
        name: "Run datadog.monitors.get",
        status: "succeeded",
        durationMs: 34000,
        lines: [
          { number: 1, text: "##[group]Invoke monitors.get" },
          { number: 2, text: "monitor_id=42" },
          { number: 3, text: "overall_state=Alert" },
          { number: 4, text: "name=API latency" },
          { number: 5, text: "##[endgroup]" },
          { number: 6, text: "Step completed with status succeeded" },
        ],
      },
      {
        id: "complete",
        name: "Complete step",
        status: "succeeded",
        durationMs: 400,
        defaultCollapsed: true,
        lines: [
          { number: 1, text: "Persisted step projection" },
          { number: 2, text: "Cleaning up" },
        ],
      },
    ],
  },
  "run_incident_1/workflow/notify": {
    groups: [
      {
        id: "run",
        name: "Run slack.chat.postMessage",
        status: "succeeded",
        durationMs: 31000,
        lines: [
          { number: 1, text: "channel=#incidents" },
          { number: 2, text: "text=Monitor API latency is alerting" },
          { number: 3, text: "ok=true ts=1722704472.000100" },
        ],
      },
    ],
  },
  "run_failed_1/workflow/post": {
    groups: [
      {
        id: "run",
        name: "Run slack.chat.postMessage",
        status: "failed",
        durationMs: 7000,
        lines: [
          { number: 1, text: "channel=#ops" },
          { number: 2, text: "POST https://slack.com/api/chat.postMessage" },
          {
            number: 3,
            text: "##[error]HTTP 429 rate_limited",
            level: "error",
          },
          { number: 4, text: "Step failed after 1 attempt" },
        ],
      },
    ],
  },
  "run_parallel_preview/changes/detect": {
    groups: [
      {
        id: "run",
        name: "Detect changed paths",
        status: "succeeded",
        durationMs: 3000,
        lines: [
          { number: 1, text: "Scanning git diff…" },
          { number: 2, text: "Changed: workflow UI surfaces" },
        ],
      },
    ],
  },
  "run_parallel_preview/validate/checkout": {
    groups: [
      {
        id: "setup",
        name: "Set up job",
        status: "succeeded",
        durationMs: 1000,
        defaultCollapsed: true,
        lines: [
          { number: 1, text: "Current runner version: 'local-mock'" },
        ],
      },
      {
        id: "checkout",
        name: "Run actions/checkout",
        status: "succeeded",
        durationMs: 3000,
        lines: [
          { number: 1, text: "Syncing repository: example-org/example-app" },
          { number: 2, text: "Working directory is ready" },
        ],
      },
    ],
  },
};

function defaultStepLogs(stepId) {
  return {
    groups: [
      {
        id: "run",
        name: `Run ${stepId}`,
        status: "succeeded",
        durationMs: 1000,
        lines: [
          { number: 1, text: `##[notice]Mock stdout for step ${stepId}` },
          {
            number: 2,
            text: "Backend log streaming is not wired yet; this UI is ready for it.",
          },
        ],
      },
    ],
  };
}

function sendJson(res, status, body) {
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.end(JSON.stringify(body));
}

function matchPath(pathname, pattern) {
  // Escape regex metacharacters, then turn `:param` into a named capture.
  // Do not escape `:` first — param markers must remain recognizable.
  const regexSource = pattern
    .split("/")
    .map((segment) => {
      if (segment.startsWith(":")) {
        const name = segment.slice(1);
        if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
          throw new Error(`invalid path param: ${segment}`);
        }
        return `(?<${name}>[^/]+)`;
      }
      return segment.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    })
    .join("/");
  const match = pathname.match(new RegExp(`^${regexSource}$`));
  return match ? match.groups || {} : null;
}

/**
 * @param {import("node:http").IncomingMessage} req
 * @param {import("node:http").ServerResponse} res
 * @param {string} pathname
 * @param {URL} url
 * @returns {boolean} true when the request was handled
 */
export function handleWorkflowsLocalMock(req, res, pathname, url) {
  const method = req.method || "GET";

  if (handleTenantThemeRequest(req, res, pathname)) {
    return true;
  }

  if (pathname === "/api/v1/auth/session" && method === "GET") {
    sendJson(res, 200, session);
    return true;
  }
  if (pathname === "/api/v1/auth/info" && method === "GET") {
    sendJson(res, 200, authInfo);
    return true;
  }
  if (pathname === "/api/v1/apps" && method === "GET") {
    sendJson(res, 200, integrations);
    return true;
  }
  if (
    pathname === `/api/v1/apps/${SLACK_APP}/admin/registry` &&
    method === "GET"
  ) {
    sendJson(res, 200, registry);
    return true;
  }
  if (
    pathname === `/api/v1/apps/${SLACK_APP}/admin/members` &&
    method === "GET"
  ) {
    sendJson(res, 200, members);
    return true;
  }
  if (
    pathname === `/api/v1/apps/${SLACK_APP}/admin/identities` &&
    method === "GET"
  ) {
    sendJson(res, 200, identities);
    return true;
  }
  if (pathname === "/api/v2/workflow/runs" && method === "GET") {
    const targetApp = url.searchParams.get("targetApp")?.trim();
    const filtered = targetApp
      ? runs.filter((run) => {
          const apps = run.target.steps
            .map((step) => step.app?.name)
            .filter(Boolean);
          return (
            apps.includes(targetApp) ||
            run.definitionId?.startsWith(`app_${targetApp}_`)
          );
        })
      : runs;
    sendJson(res, 200, { runs: filtered, nextPageToken: "" });
    return true;
  }
  const runMatch = matchPath(pathname, "/api/v2/workflow/runs/:runId");
  if (runMatch && method === "GET") {
    const run = runs.find((item) => item.id === runMatch.runId);
    if (!run) {
      sendJson(res, 404, { error: "not found" });
      return true;
    }
    sendJson(res, 200, run);
    return true;
  }
  const stepLogsMatch = matchPath(
    pathname,
    "/api/v2/workflow/runs/:runId/jobs/:jobId/steps/:stepId/logs",
  );
  if (stepLogsMatch && method === "GET") {
    const key = `${stepLogsMatch.runId}/${stepLogsMatch.jobId}/${stepLogsMatch.stepId}`;
    const payload = stepLogs[key] || defaultStepLogs(stepLogsMatch.stepId);
    sendJson(res, 200, {
      runId: stepLogsMatch.runId,
      jobId: stepLogsMatch.jobId,
      stepId: stepLogsMatch.stepId,
      ...payload,
    });
    return true;
  }
  if (pathname === "/api/v2/workflow/definitions" && method === "GET") {
    sendJson(res, 200, { definitions });
    return true;
  }
  const definitionMatch = matchPath(
    pathname,
    "/api/v2/workflow/definitions/:definitionId",
  );
  if (definitionMatch && method === "GET") {
    const definition = definitions.find(
      (item) => item.id === definitionMatch.definitionId,
    );
    if (!definition) {
      sendJson(res, 404, { error: "not found" });
      return true;
    }
    sendJson(res, 200, definition);
    return true;
  }
  if (pathname.startsWith("/api/")) {
    sendJson(res, 404, { error: "not found" });
    return true;
  }
  return false;
}

/** Connect middleware for Vite `configureServer`. */
export function workflowsLocalMockMiddleware(req, res, next) {
  try {
    const host = req.headers.host || "127.0.0.1";
    const url = new URL(req.url || "/", `http://${host}`);
    const pathname = decodeURIComponent(url.pathname);
    if (handleWorkflowsLocalMock(req, res, pathname, url)) {
      return;
    }
    next();
  } catch (error) {
    next(error);
  }
}

export function resolveWorktreeDisplayName(
  fromEnv = process.env.VITE_GESTALT_WORKTREE_NAME,
  worktreeRoot = path.resolve(
    path.dirname(path.dirname(fileURLToPath(import.meta.url))),
    "../..",
  ),
) {
  const fromExplicit = typeof fromEnv === "string" ? fromEnv.trim() : "";
  if (fromExplicit) return fromExplicit;
  return path.basename(worktreeRoot).trim();
}

export function workflowsLocalMockEnabled(
  env = process.env,
) {
  const value = env[WORKFLOWS_LOCAL_MOCK_ENV]?.trim().toLowerCase();
  return value === "1" || value === "true" || value === "yes";
}
