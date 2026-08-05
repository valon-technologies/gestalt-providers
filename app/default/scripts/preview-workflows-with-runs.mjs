/**
 * Headed preview: mock SPA + seeded workflow runs for visual inspection.
 * Usage: node scripts/preview-workflows-with-runs.mjs
 */
import { chromium } from "@playwright/test";
import { spawn } from "node:child_process";
import net from "node:net";
import path from "node:path";
import { fileURLToPath } from "node:url";

const projectDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const SLACK_APP = "slack";

async function allocatePort() {
  return await new Promise((resolve, reject) => {
    const server = net.createServer();
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") {
        server.close();
        reject(new Error("Unable to allocate a free port"));
        return;
      }
      const { port } = address;
      server.close((error) => {
        if (error) reject(error);
        else resolve(port);
      });
    });
    server.on("error", reject);
  });
}

const runs = [
  {
    id: "run_incident_1",
    provider: "basic",
    status: "succeeded",
    definitionId: "app_slack_incident_triage",
    target: {
      steps: [
        {
          id: "diagnose",
          app: { name: "datadog", operation: "monitors.get" },
        },
        {
          id: "notify",
          app: { name: SLACK_APP, operation: "chat.postMessage" },
        },
      ],
    },
    trigger: {
      kind: "event",
      activationId: "datadog_alert",
      event: { type: "datadog.monitor.alert", source: "datadog" },
    },
    createdBy: { subjectId: "service_account:workflow-runner" },
    createdAt: "2026-08-03T18:00:00Z",
    startedAt: "2026-08-03T18:00:05Z",
    completedAt: "2026-08-03T18:01:12Z",
    statusMessage: "completed",
    steps: [
      { stepId: "diagnose", status: "succeeded" },
      { stepId: "notify", status: "succeeded", output: { ok: true } },
    ],
  },
  {
    id: "run_pending_1",
    provider: "basic",
    status: "pending",
    definitionId: "app_slack_notify",
    target: {
      steps: [
        {
          id: "run",
          app: { name: SLACK_APP, operation: "chat.postMessage" },
        },
      ],
    },
    trigger: { kind: "schedule", activationId: "morning" },
    createdAt: "2026-08-03T19:00:00Z",
  },
  {
    id: "run_failed_1",
    provider: "basic",
    status: "failed",
    definitionId: "app_slack_notify",
    target: {
      steps: [
        {
          id: "run",
          app: { name: SLACK_APP, operation: "chat.postMessage" },
        },
      ],
    },
    trigger: { kind: "manual" },
    createdAt: "2026-08-03T17:30:00Z",
    completedAt: "2026-08-03T17:30:08Z",
    statusMessage: "chat.postMessage returned 429",
  },
  {
    id: "run_running_1",
    provider: "basic",
    status: "running",
    definitionId: "app_slack_incident_triage",
    target: {
      steps: [
        {
          id: "diagnose",
          app: { name: "datadog", operation: "monitors.get" },
        },
        {
          id: "notify",
          app: { name: SLACK_APP, operation: "chat.postMessage" },
        },
      ],
    },
    trigger: { kind: "event", activationId: "datadog_alert" },
    createdAt: "2026-08-03T19:10:00Z",
    startedAt: "2026-08-03T19:10:02Z",
    currentStepId: "diagnose",
  },
];

const definitions = [
  {
    id: "app_slack_notify",
    provider: "basic",
    paused: false,
    runAs: "service_account:slack-bot",
    updatedAt: "2026-08-01T12:00:00Z",
    target: {
      steps: [
        {
          id: "run",
          app: { name: SLACK_APP, operation: "chat.postMessage" },
        },
      ],
    },
    activations: [
      {
        id: "morning",
        trigger: { kind: "schedule", cron: "0 9 * * *", timezone: "UTC" },
      },
    ],
  },
  {
    id: "app_slack_incident_triage",
    provider: "basic",
    paused: false,
    runAs: "service_account:workflow-runner",
    updatedAt: "2026-08-02T09:00:00Z",
    target: {
      steps: [
        {
          id: "diagnose",
          app: { name: "datadog", operation: "monitors.get" },
        },
        {
          id: "notify",
          app: { name: SLACK_APP, operation: "chat.postMessage" },
        },
      ],
    },
    activations: [
      {
        id: "datadog_alert",
        trigger: {
          kind: "event",
          eventType: "datadog.monitor.alert",
          eventSource: "datadog",
        },
      },
    ],
  },
];

function startMockServer(port) {
  return new Promise((resolve, reject) => {
    const child = spawn(
      process.execPath,
      [path.join(projectDir, "scripts/serve-mock.mjs")],
      {
        cwd: projectDir,
        env: { ...process.env, API_PORT: String(port), PORT: String(port) },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    let settled = false;
    let logs = "";
    const onData = (buf) => {
      logs += String(buf);
      if (settled) return;
      if (logs.includes("listening")) {
        settled = true;
        resolve(child);
      }
    };
    child.stdout.on("data", onData);
    child.stderr.on("data", onData);
    child.on("error", reject);
    child.on("exit", (code) => {
      if (!settled) {
        reject(new Error(`mock server exited early (${code}): ${logs.trim()}`));
      }
    });
    setTimeout(() => {
      if (!settled) {
        settled = true;
        resolve(child);
      }
    }, 3000);
  });
}

async function main() {
  const port = process.env.API_PORT
    ? Number(process.env.API_PORT)
    : await allocatePort();
  if (!Number.isFinite(port) || port <= 0) {
    throw new Error(`Invalid port: ${String(process.env.API_PORT)}`);
  }
  const baseURL = `http://127.0.0.1:${port}`;
  const server = await startMockServer(port);
  const browser = await chromium.launch({
    headless: false,
    channel: "chrome",
  });
  const page = await browser.newPage();

  await page.addInitScript(() => {
    localStorage.setItem(
      "gestalt.auth.session",
      JSON.stringify({
        subjectId: "user:test@gestalt.dev",
        email: "test@gestalt.dev",
      }),
    );
  });

  await page.route("**/api/v1/auth/session", (route) =>
    route.fulfill({
      json: {
        subjectId: "user:test@gestalt.dev",
        email: "test@gestalt.dev",
      },
    }),
  );
  await page.route("**/api/v1/auth/info", (route) =>
    route.fulfill({
      json: {
        provider: "test-sso",
        displayName: "Test SSO",
        features: { workflowDefaultProvider: "basic" },
      },
    }),
  );
  await page.route("**/api/v1/apps", (route) => {
    if (route.request().method() !== "GET") return route.fallback();
    return route.fulfill({
      json: [
        {
          name: SLACK_APP,
          displayName: "Slack",
          managementPath: `/apps/${SLACK_APP}/admin`,
        },
      ],
    });
  });
  await page.route(`**/api/v1/apps/${SLACK_APP}/admin/registry**`, (route) =>
    route.fulfill({
      json: {
        app: SLACK_APP,
        registry: "example-registry",
        knownVersions: [],
        publishedVersions: [],
        selectionDisabled: false,
      },
    }),
  );
  await page.route(`**/api/v1/apps/${SLACK_APP}/admin/members**`, (route) =>
    route.fulfill({ json: { members: [] } }),
  );
  await page.route(/\/api\/v2\/workflow\/runs(?:\?.*)?$/, (route) => {
    if (route.request().method() !== "GET") return route.fallback();
    return route.fulfill({ json: { runs, nextPageToken: "" } });
  });
  await page.route("**/api/v2/workflow/runs/**", (route) => {
    const id = new URL(route.request().url()).pathname.split("/").pop();
    const run = runs.find((item) => item.id === id);
    if (!run) return route.fulfill({ status: 404, json: { error: "not found" } });
    return route.fulfill({ json: run });
  });
  await page.route(/\/api\/v2\/workflow\/definitions(?:\?.*)?$/, (route) => {
    if (route.request().method() !== "GET") return route.fallback();
    return route.fulfill({ json: { definitions } });
  });
  await page.route("**/api/v2/workflow/definitions/**", (route) => {
    const id = decodeURIComponent(
      new URL(route.request().url()).pathname.split("/").pop() || "",
    );
    const definition = definitions.find((item) => item.id === id);
    if (!definition) {
      return route.fulfill({ status: 404, json: { error: "not found" } });
    }
    return route.fulfill({ json: definition });
  });

  const url = `${baseURL}/apps/${SLACK_APP}/admin/workflows`;
  console.log(`Opening ${url}`);
  console.log("Close the browser window when finished.");
  await page.goto(url);
  await page.getByTestId("app-workflow-run-list").waitFor({ timeout: 20000 });

  await new Promise((resolve) => {
    browser.on("disconnected", resolve);
  });
  server.kill("SIGTERM");
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
