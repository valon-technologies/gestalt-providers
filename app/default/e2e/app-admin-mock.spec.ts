import {
  expect,
  mockAppAdminRegistry,
  mockAppAdminRegistryHistory,
  mockAppAuthorizationMembers,
  mockAuthSession,
  mockIntegrations,
  test,
} from "./fixtures";
import type { AppAdminRegistryResponse, Integration } from "../src/lib/api";

const APP = "example-app";
const EXAMPLE_REPO = "https://github.com/example-org/example-app";

const PUBLISHED_NEW: AppAdminRegistryResponse["publishedVersions"][number] = {
  version: "0.0.0-snapshot.gdef456",
  publishedAt: "2026-07-22T15:00:00Z",
  platforms: ["linux/amd64"],
  sourceRef: "def456def456def456def456def456def456def4",
  sourceUrl: `${EXAMPLE_REPO}/commit/def456def456def456def456def456def456def4`,
  publication: {
    workflowRunUrl: `${EXAMPLE_REPO}/actions/runs/123456789`,
    triggerPullRequest: {
      number: 3251,
      url: `${EXAMPLE_REPO}/pull/3251`,
      title: "Add registry deploy banner",
    },
  },
};

const PUBLISHED_LEGACY: AppAdminRegistryResponse["publishedVersions"][number] = {
  version: "0.0.0-snapshot.gabc123",
  publishedAt: "2026-07-21T12:00:00Z",
  platforms: ["linux/amd64"],
  sourceRef: "abc123abc123abc123abc123abc123abc123ab",
  sourceUrl: `${EXAMPLE_REPO}/commit/abc123abc123abc123abc123abc123abc123ab`,
};

const PENDING_VERSION: NonNullable<AppAdminRegistryResponse["pendingVersions"]>[number] = {
  version: "0.0.0-snapshot.gpending01",
  startedAt: "2026-07-23T14:56:00Z",
  updatedAt: "2026-07-23T14:56:00Z",
  phase: "publishing",
  publishingForSeconds: 240,
  publication: {
    workflowRunUrl: `${EXAMPLE_REPO}/actions/runs/223456789`,
    triggerPullRequest: {
      number: 3740,
      url: `${EXAMPLE_REPO}/pull/3740`,
      title: "Publish pending snapshot",
    },
  },
};

const FAILED_VERSION: NonNullable<AppAdminRegistryResponse["failedVersions"]>[number] = {
  version: "0.0.0-snapshot.gfailed01",
  startedAt: "2026-07-24T18:00:00Z",
  failedAt: "2026-07-24T18:35:00Z",
  reason: "stale",
  publishDurationSeconds: 2100,
  publication: {
    workflowRunUrl: `${EXAMPLE_REPO}/actions/runs/323456789`,
    triggerPullRequest: {
      number: 3788,
      url: `${EXAMPLE_REPO}/pull/3788`,
      title: "Retry registry publish",
    },
  },
};

const MANAGED_INTEGRATION: Integration = {
  name: APP,
  displayName: "Example App",
  mountedPath: `/${APP}`,
  managementPath: `/apps/${APP}/admin`,
  status: "ready",
  credentialState: "connected",
  connections: [
    {
      name: "plugin",
      authTypes: ["oauth"],
      status: "ready",
      credentialState: "connected",
      actions: ["disconnect"],
      instances: [{ name: "default", connection: "plugin" }],
    },
  ],
};

const UNMANAGED_INTEGRATION: Integration = {
  name: "slack",
  displayName: "Slack",
};

function installedRegistryState(): AppAdminRegistryResponse {
  return {
    app: APP,
    registry: "example-registry",
    desiredVersion: PUBLISHED_LEGACY.version,
    knownVersions: [
      {
        version: PUBLISHED_LEGACY.version,
        installedAt: "2026-07-21T13:00:00Z",
        installedBy: "user:alice",
      },
    ],
    publishedVersions: [PUBLISHED_NEW, PUBLISHED_LEGACY],
    rollout: {
      version: PUBLISHED_LEGACY.version,
      state: "complete",
    },
    autoDeploy: {
      enabled: false,
    },
    selectionDisabled: false,
  };
}

const APP_ADMIN_FIXED_TIME = new Date("2026-07-23T15:00:00Z");
const SOURCE_VERSION = "4f71afddf31d2c452ecd248779a04c905a7b9988";

function fleetState(
  overrides: Partial<NonNullable<AppAdminRegistryResponse["fleetState"]>> = {},
): NonNullable<AppAdminRegistryResponse["fleetState"]> {
  return {
    state: "healthy",
    sourceVersion: SOURCE_VERSION,
    desiredVersion: PUBLISHED_LEGACY.version,
    minimumHealthyInstances: 5,
    liveInstances: 5,
    runningDesiredVersion: 5,
    mismatched: 0,
    errors: 0,
    heartbeatTtlSeconds: 45,
    evaluatedAt: "2026-07-23T14:59:50Z",
    ...overrides,
  };
}

test.describe("app admin registry UI", () => {
  test.beforeEach(async ({ page }) => {
    await page.clock.install({ time: APP_ADMIN_FIXED_TIME });
    await page.addInitScript(() => {
      localStorage.setItem(
        "gestalt.auth.session",
        JSON.stringify({
          subjectId: "user:test@gestalt.dev",
          email: "test@gestalt.dev",
        }),
      );
    });
    await mockAuthSession(page);
  });

  test("keeps healthy fleet recovery separate from a failed rollout", async ({ page }) => {
    const recovery = {
      recoveredAt: "2026-07-23T14:58:00Z",
      sourceVersion: SOURCE_VERSION,
      liveInstances: 5,
      minimumHealthyInstances: 5,
    };
    await mockAppAdminRegistry(page, APP, {
      ...installedRegistryState(),
      rollout: {
        version: PUBLISHED_LEGACY.version,
        state: "failed",
        targetSourceVersion: SOURCE_VERSION,
      },
      fleetState: fleetState(),
      recovery,
    });
    await mockAppAdminRegistryHistory(page, APP, {
      app: APP,
      fleetState: fleetState(),
      revisions: [
        {
          id: "rev-recovered",
          version: PUBLISHED_LEGACY.version,
          previousVersion: PUBLISHED_NEW.version,
          deployedAt: "2026-07-23T14:30:00Z",
          deployedBy: "user:alice@example.com",
          rolloutState: "failed",
          rolloutDurationSeconds: 900,
          rolloutFailedAt: "2026-07-23T14:45:00Z",
          recovery,
        },
      ],
    });

    await page.goto(`/apps/${APP}/versions`);

    const fleet = page.getByTestId("app-admin-fleet-state");
    await expect(fleet.getByTestId("fleet-state-badge")).toHaveText("Healthy");
    await expect(fleet).toHaveAttribute("data-fleet-density", "quiet");
    await expect(fleet.getByTestId("fleet-live-instances")).toHaveCount(0);
    await expect(fleet.getByTestId("fleet-metrics")).toHaveCount(0);
    await expect(fleet.getByTestId("fleet-minimum-instances")).toHaveCount(0);
    await expect(fleet.getByTestId("fleet-running-desired")).toHaveCount(0);
    await expect(fleet.getByTestId("fleet-desired-version")).toHaveCount(0);
    await expect(fleet.getByTestId("fleet-source-version")).toHaveCount(0);
    await expect(fleet.getByTestId("fleet-evaluated-at")).toHaveCount(0);
    await expect(fleet).not.toContainText("Last evaluated");
    await expect(fleet).not.toContainText("Fresh report window");
    await expect(fleet.getByTestId("recovered-after-failed-rollout")).toContainText(
      "Recovered after failed rollout",
    );
    await expect(fleet.getByTestId("recovered-after-failed-rollout")).toContainText(
      "Runtime commit",
    );
    await expect(fleet.getByTestId("recovered-after-failed-rollout")).not.toContainText(
      "5 live / 5 minimum",
    );

    const snapshotRow = page.getByTestId("snapshot-row-published").filter({
      hasText: PUBLISHED_LEGACY.version.slice(0, 20),
    });
    await expect(
      snapshotRow.getByTestId(`deploy-version-${PUBLISHED_LEGACY.version}`),
    ).toHaveCount(0);
  });

  test("shows unknown when live capacity is below the minimum", async ({ page }) => {
    await mockAppAdminRegistry(page, APP, {
      ...installedRegistryState(),
      fleetState: fleetState({
        state: "unknown",
        liveInstances: 4,
        runningDesiredVersion: 4,
      }),
    });

    await page.goto(`/apps/${APP}/versions`);

    const fleet = page.getByTestId("app-admin-fleet-state");
    await expect(fleet.getByTestId("fleet-state-badge")).toHaveText("Unknown");
    await expect(fleet).toContainText(
      "Not enough fresh replica reports to determine fleet health",
    );
    await expect(fleet.getByTestId("fleet-live-instances")).toHaveCount(0);
    await expect(fleet.getByTestId("fleet-minimum-instances")).toHaveText("5");
    await expect(fleet).toContainText("Fresh report window: 45s");
    await expect(fleet).not.toContainText("Every live replica");
  });

  test("shows degraded mismatch and error observations", async ({ page }) => {
    await mockAppAdminRegistry(page, APP, {
      ...installedRegistryState(),
      fleetState: fleetState({
        state: "degraded",
        runningDesiredVersion: 3,
        mismatched: 1,
        errors: 1,
      }),
    });

    await page.goto(`/apps/${APP}/versions`);

    const fleet = page.getByTestId("app-admin-fleet-state");
    await expect(fleet.getByTestId("fleet-state-badge")).toHaveText("Degraded");
    await expect(fleet.getByTestId("fleet-running-desired")).toHaveText("3");
    await expect(fleet.getByTestId("fleet-mismatched")).toHaveText("1");
    await expect(fleet.getByTestId("fleet-errors")).toHaveText("1");
    await expect(fleet).toContainText(
      "at least one has a version mismatch or error",
    );
    await expect(fleet.getByTestId("fleet-desired-version")).toBeVisible();
    await expect(fleet.getByTestId("fleet-source-version")).toBeVisible();
    await expect(fleet).toContainText("Runtime commit");
  });

  test("treats absent heartbeat fields as unknown during mixed-version rollout", async ({
    page,
  }) => {
    await mockAppAdminRegistry(page, APP, installedRegistryState());

    await page.goto(`/apps/${APP}/versions`);

    const fleet = page.getByTestId("app-admin-fleet-state");
    await expect(fleet.getByTestId("fleet-state-badge")).toHaveText("Unknown");
    await expect(fleet).toContainText(
      "Replica report data is unavailable, so fleet health can't be determined",
    );
    await expect(fleet.getByTestId("fleet-live-instances")).toHaveCount(0);
    await expect(fleet).not.toContainText("Healthy");
    await expect(page.getByTestId("recovered-after-failed-rollout")).toHaveCount(0);
  });

  test("card click opens app detail for managed apps", async ({ page }) => {
    await mockIntegrations(page, [MANAGED_INTEGRATION, UNMANAGED_INTEGRATION]);
    await mockAppAdminRegistry(page, APP, installedRegistryState());
    await page.goto("/apps");

    await page.getByRole("link", { name: "Example App" }).click();
    await page.waitForURL("**/apps/example-app");
    await expect(
      page.getByRole("heading", { level: 1, name: "Example App" }),
    ).toBeVisible();
    await expect(page.getByTestId("app-admin-nav-versions")).toBeVisible();
    await expect(page.getByRole("button", { name: "Manage app" })).toHaveCount(0);
  });

  test("renders publishing and failed snapshot rows with duration labels", async ({ page }) => {
    await mockAppAdminRegistry(page, APP, {
      ...installedRegistryState(),
      pendingVersions: [PENDING_VERSION],
      failedVersions: [FAILED_VERSION],
    });
    await page.goto(`/apps/${APP}/admin`);

    const pendingRow = page.getByTestId("snapshot-row-pending");
    await expect(pendingRow).toContainText(PENDING_VERSION.version.slice(0, 20));
    await expect(pendingRow.getByTestId("snapshot-status-spinner")).toBeVisible();
    await expect(pendingRow.getByTestId("snapshot-status")).toHaveText("Publishing");
    await expect(pendingRow).toContainText("for 4m");
    await expect(pendingRow.getByTestId("snapshot-last-updated-at")).toHaveText("4 minutes ago");
    await expect(pendingRow).toContainText("PR #3740");
    await expect(pendingRow).toContainText("Publish pending snapshot");
    await expect(pendingRow.getByTestId(`deploy-version-${PENDING_VERSION.version}`)).toHaveCount(0);

    const failedRow = page.getByTestId("snapshot-row-failed");
    await expect(failedRow).toContainText(FAILED_VERSION.version.slice(0, 20));
    await expect(failedRow.getByTestId("snapshot-status")).toHaveText("Publish failed");
    await expect(failedRow).toContainText("Failed after 35m");
    await expect(failedRow).toContainText("stale");
    await expect(failedRow).toContainText("Retry registry publish");
    await expect(failedRow.getByTestId(`deploy-version-${FAILED_VERSION.version}`)).toHaveCount(0);
  });

  test("shows relative last update for pending rows younger than one minute", async ({
    page,
  }) => {
    await mockAppAdminRegistry(page, APP, {
      ...installedRegistryState(),
      pendingVersions: [
        {
          ...PENDING_VERSION,
          startedAt: "2026-07-23T14:59:43Z",
          updatedAt: "2026-07-23T14:59:43Z",
        },
      ],
    });
    await page.goto(`/apps/${APP}/admin`);

    await expect(
      page.getByTestId("snapshot-row-pending").getByTestId("snapshot-last-updated-at"),
    ).toHaveText("1 minute ago");
  });

  test("snapshot status badges use badge status surfaces in dark mode", async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.setItem("theme", "dark");
    });
    await page.route("**/theme.css", (route) =>
      route.fulfill({
        contentType: "text/css",
        body: `
          .dark {
            --success: oklch(48.204% 0.0881 144.06);
            --badge-success: oklch(28.2% 0.058 144);
            --badge-success-foreground: oklch(85.8% 0.075 144);
            --badge-warning: oklch(28.2% 0.058 82);
            --badge-warning-foreground: oklch(85.8% 0.075 82);
            --badge-info: oklch(28.2% 0.058 248);
            --badge-info-foreground: oklch(85.8% 0.07 248);
          }
        `,
      }),
    );
    await mockAppAdminRegistry(page, APP, {
      ...installedRegistryState(),
      desiredVersion: PUBLISHED_NEW.version,
      pendingVersions: [PENDING_VERSION],
    });
    await page.goto(`/apps/${APP}/admin`);

    const deployedBadge = page
      .getByTestId(`snapshot-row-published`)
      .filter({ hasText: PUBLISHED_NEW.version.slice(0, 20) })
      .getByTestId("deployed-version-badge");
    await expect(deployedBadge).toHaveText("Deployed version");
    await expect(deployedBadge).toHaveCSS("color", "oklch(0.858 0.07 248)");

    const publishingBadge = page.getByTestId("snapshot-row-pending").getByTestId("snapshot-status");
    await expect(publishingBadge).toHaveText("Publishing");
    await expect(publishingBadge).toHaveCSS("color", "oklch(0.858 0.075 82)");
  });

  test("available snapshots use registry status indicator surfaces", async ({ page }) => {
    await page.route("**/theme.css", (route) =>
      route.fulfill({
        contentType: "text/css",
        body: `
          :root {
            --success: oklch(48.204% 0.0881 144.06);
          }
        `,
      }),
    );
    await mockAppAdminRegistry(page, APP, {
      ...installedRegistryState(),
      publishedVersions: [
        {
          ...PUBLISHED_NEW,
          publishDurationSeconds: 111,
        },
        PUBLISHED_LEGACY,
      ],
    });
    await page.goto(`/apps/${APP}/admin`);

    const availableRow = page
      .getByTestId("snapshot-row-published")
      .filter({ hasText: PUBLISHED_NEW.version.slice(0, 20) });
    await expect(availableRow.getByTestId("snapshot-status")).toHaveCount(0);
    await expect(
      availableRow.locator('[data-slot="table-status-indicator"]'),
    ).toHaveAttribute("data-variant", "success");
    const availableIndicatorShell = availableRow
      .locator('[data-slot="table-status-indicator"]')
      .locator("> span");
    await expect(availableIndicatorShell).toHaveCSS(
      "background-color",
      "oklch(0.62 0.19 145)",
    );
    await expect(availableIndicatorShell).toHaveCSS("color", "rgb(255, 255, 255)");
    await expect(availableRow).toContainText("Published in");

    const deployedBadge = page
      .getByTestId("snapshot-row-published")
      .filter({ hasText: PUBLISHED_LEGACY.version.slice(0, 20) })
      .getByTestId("deployed-version-badge");
    await expect(deployedBadge).toHaveText("Deployed version");
    await expect(deployedBadge).toHaveAttribute("data-variant", "info");
    await expect(deployedBadge).toHaveCSS("color", "oklch(0.408 0.105 248)");
  });

  test("legacy gestalt-shell grove success override does not recolor status badges", async ({
    page,
  }) => {
    await page.route("**/theme.css", (route) =>
      route.fulfill({
        contentType: "text/css",
        body: `
          :root {
            --success: oklch(48.204% 0.0881 144.06);
          }
        `,
      }),
    );
    await mockAppAdminRegistry(page, APP, installedRegistryState());
    await page.goto(`/apps/${APP}/admin`);

    const deployedRow = page
      .getByTestId("snapshot-row-published")
      .filter({ hasText: PUBLISHED_LEGACY.version.slice(0, 20) });
    const deployedBadge = deployedRow.getByTestId("deployed-version-badge");
    await expect(deployedBadge).toHaveText("Deployed version");
    await expect(deployedBadge).toHaveCSS("background-color", "oklch(0.928 0.035 248)");
    await expect(deployedBadge).toHaveCSS("color", "oklch(0.408 0.105 248)");
    await expect(deployedBadge).not.toHaveAttribute("style", /./);
    await expect(deployedRow).not.toHaveClass(/bg-success\/10/);

    const liveIndicatorShell = page
      .getByTestId("snapshot-row-published")
      .filter({ hasText: PUBLISHED_LEGACY.version.slice(0, 20) })
      .locator('[data-slot="table-status-indicator"]')
      .locator("> span");
    await expect(liveIndicatorShell).toHaveCSS(
      "background-color",
      "oklch(0.62 0.19 145)",
    );
    await expect(liveIndicatorShell).toHaveCSS("color", "rgb(255, 255, 255)");
  });

  test("prefers published rows over pending and failed for the same version", async ({
    page,
  }) => {
    await mockAppAdminRegistry(page, APP, {
      ...installedRegistryState(),
      publishedVersions: [
        {
          ...PUBLISHED_NEW,
          publishStartedAt: "2026-07-22T14:55:28Z",
          publishDurationSeconds: 272,
        },
        PUBLISHED_LEGACY,
      ],
      pendingVersions: [{ ...PENDING_VERSION, version: PUBLISHED_NEW.version }],
      failedVersions: [{ ...FAILED_VERSION, version: PUBLISHED_LEGACY.version }],
    });
    await page.goto(`/apps/${APP}/admin`);

    await expect(page.getByTestId("snapshot-row-pending")).toHaveCount(0);
    await expect(page.getByTestId("snapshot-row-failed")).toHaveCount(0);
    await expect(page.getByTestId("snapshot-row-published")).toHaveCount(2);
    const publishedRow = page.getByTestId("snapshot-row-published").first();
    await expect(publishedRow).toContainText("Published in 4m 32s");
  });

  test("renders published snapshots newest first with PR titles", async ({ page }) => {
    await mockIntegrations(page, [MANAGED_INTEGRATION]);
    await mockAppAdminRegistry(page, APP, installedRegistryState());
    await page.goto(`/apps/${APP}/admin`);

    const rows = page.getByTestId("snapshot-row-published");
    await expect(rows).toHaveCount(2);
    await expect(rows.nth(0)).toContainText(PUBLISHED_NEW.version.slice(0, 20));
    await expect(rows.nth(0)).toContainText("PR #3251");
    await expect(rows.nth(0)).toContainText("Add registry deploy banner");
    await expect(rows.nth(0).getByTestId("snapshot-last-updated-at")).toHaveText("yesterday");
    await expect(rows.nth(1)).toContainText(PUBLISHED_LEGACY.version.slice(0, 20));
    await expect(rows.nth(1)).toContainText("Deployed version");
  });

  test("links to the mounted app page when mountedPath is available", async ({ page }) => {
    await mockIntegrations(page, [MANAGED_INTEGRATION]);
    await mockAppAdminRegistry(page, APP, installedRegistryState());
    await page.goto(`/apps/${APP}`);

    const openAppButton = page.getByTestId("open-app-detail");
    await expect(openAppButton).toBeVisible();
    await expect(openAppButton).toHaveText("Open app");
  });

  test("sorts published snapshots newest first even when API returns older first", async ({
    page,
  }) => {
    await mockAppAdminRegistry(page, APP, {
      ...installedRegistryState(),
      publishedVersions: [PUBLISHED_LEGACY, PUBLISHED_NEW],
    });
    await page.goto(`/apps/${APP}/admin`);

    const rows = page.getByTestId("snapshot-row-published");
    await expect(rows.nth(0)).toContainText(PUBLISHED_NEW.version.slice(0, 20));
    await expect(rows.nth(1)).toContainText(PUBLISHED_LEGACY.version.slice(0, 20));
  });

  test("renders first-install copy when no desired version exists", async ({ page }) => {
    await mockAppAdminRegistry(page, APP, {
      app: APP,
      registry: "example-registry",
      knownVersions: [],
      publishedVersions: [PUBLISHED_NEW],
      selectionDisabled: false,
    });
    await page.goto(`/apps/${APP}/admin`);

    await expect(page.getByText(/No version is serving on the fleet yet/i)).toBeVisible();
    await expect(page.getByTestId("snapshot-row-published")).toHaveCount(1);
  });

  test("disables deploy buttons during active rollout", async ({ page }) => {
    await mockAppAdminRegistry(page, APP, {
      ...installedRegistryState(),
      rollout: {
        version: PUBLISHED_NEW.version,
        state: "enrolling",
        createdAt: "2026-07-23T14:56:00Z",
      },
      selectionDisabled: true,
      disabledReason: "rollout in progress",
    });
    await page.goto(`/apps/${APP}/admin`);

    await expect(page.getByTestId("rollout-active-banner")).toBeVisible();
    const rolloutRow = page.getByTestId("snapshot-row-published").filter({
      hasText: PUBLISHED_NEW.version.slice(0, 20),
    });
    await expect(rolloutRow.getByTestId("rollout-phase-stepper")).toBeVisible();
    await expect(rolloutRow.getByTestId("rollout-phase-node-enrolling")).toBeVisible();
    await expect(page.getByTestId(`deploy-version-${PUBLISHED_NEW.version}`)).toBeDisabled();
    await expect(page.getByTestId(`deploy-version-${PUBLISHED_NEW.version}`)).toHaveText(
      "Deploying...",
    );
    await expect(page.getByTestId(`deploy-version-${PUBLISHED_LEGACY.version}`)).toHaveCount(0);
    await expect(page.getByTestId("selection-disabled-reason")).toHaveText(
      "Deploy paused while a rollout is in progress.",
    );
  });

  test("successful deploy shows the new active rollout", async ({ page }) => {
    await mockAppAdminRegistry(page, APP, installedRegistryState(), {
      onSelectVersion: (version) => ({
        app: APP,
        registry: "example-registry",
        fromVersion: PUBLISHED_LEGACY.version,
        desiredVersion: version,
        rollout: {
          version,
          state: "enrolling",
        },
      }),
    });
    await page.goto(`/apps/${APP}/admin`);

    await page.getByTestId(`deploy-version-${PUBLISHED_NEW.version}`).click();

    await expect(page.getByTestId("rollout-active-banner")).toBeVisible();
    const rolloutRow = page.getByTestId("snapshot-row-published").filter({
      hasText: PUBLISHED_NEW.version.slice(0, 20),
    });
    await expect(rolloutRow.getByTestId("rollout-phase-stepper")).toBeVisible();
    await expect(rolloutRow.getByTestId("rollout-phase-node-enrolling")).toBeVisible();
    await expect(page.getByTestId(`deploy-version-${PUBLISHED_NEW.version}`)).toBeDisabled();
    await expect(page.getByTestId(`deploy-version-${PUBLISHED_NEW.version}`)).toHaveText(
      "Deploying...",
    );
  });

  test("409 after stale page disables controls after refresh", async ({ page }) => {
    await mockAppAdminRegistry(page, APP, installedRegistryState(), {
      onSelectVersion: (version, currentState) => ({
        status: 409,
        json: { error: "rollout already active" },
        nextState: {
          ...currentState,
          selectionDisabled: true,
          disabledReason: "rollout in progress",
          rollout: {
            version,
            state: "enrolling",
          },
        },
      }),
    });
    await page.goto(`/apps/${APP}/admin`);

    await page.getByTestId(`deploy-version-${PUBLISHED_NEW.version}`).click();

    await expect(page.getByTestId(`deploy-version-${PUBLISHED_NEW.version}`)).toBeDisabled();
    await expect(page.getByTestId(`deploy-version-${PUBLISHED_NEW.version}`)).toHaveText(
      "Deploying...",
    );
    await expect(page.getByTestId(`deploy-version-${PUBLISHED_LEGACY.version}`)).toHaveCount(0);
  });

  test("polls for pending publish without manual refresh", async ({ page }) => {
    let registryCalls = 0;

    await page.route(`**/api/v1/apps/${APP}/admin/registry`, (route, request) => {
      if (request.method() !== "GET") {
        void route.fallback();
        return;
      }
      registryCalls += 1;
      const responseState =
        registryCalls >= 2
          ? {
              ...installedRegistryState(),
              pendingVersions: [PENDING_VERSION],
            }
          : installedRegistryState();
      void route.fulfill({ json: responseState });
    });
    await page.goto(`/apps/${APP}/admin`);

    await expect(page.getByTestId("snapshot-row-pending")).toHaveCount(0);
    await expect(page.getByTestId("snapshot-row-pending")).toBeVisible({ timeout: 15_000 });
    await expect(page.getByTestId("snapshot-row-pending").getByTestId("snapshot-status")).toHaveText(
      "Publishing",
    );
  });

  test("403 renders access denied without registry metadata", async ({ page }) => {
    await page.route(`**/api/v1/apps/${APP}/admin/registry`, (route) => {
      route.fulfill({ status: 403, json: { error: "app access denied" } });
    });
    await page.goto(`/apps/${APP}/admin`);

    await expect(page.getByTestId("app-admin-access-denied")).toBeVisible();
    await expect(page.getByText("Access denied")).toBeVisible();
    await expect(page.getByTestId("snapshots-table")).toHaveCount(0);
    await expect(page.getByText("example-registry")).toHaveCount(0);
  });

  test("combines version history into the versions table", async ({ page }) => {
    const revision = {
      id: "rev-3",
      version: PUBLISHED_NEW.version,
      previousVersion: PUBLISHED_LEGACY.version,
      deployedAt: "2026-07-25T09:10:00Z",
      deployedBy: "user:alice@example.com",
      deploymentState: "desired",
      current: true,
      publication: PUBLISHED_NEW.publication,
    };

    await mockAppAdminRegistry(page, APP, installedRegistryState());
    await mockAppAdminRegistryHistory(page, APP, { app: APP, revisions: [revision] });
    await page.goto(`/apps/${APP}/admin`);

    await expect(page.getByTestId("app-admin-nav-history")).toHaveCount(0);
    await expect(page.getByRole("columnheader", { name: "Deployed by" })).toBeVisible();
    const deployedByAvatar = page
      .getByTestId("snapshot-row-published")
      .filter({ hasText: PUBLISHED_NEW.version.slice(0, 20) })
      .getByTestId("deployed-by-avatar");
    await expect(deployedByAvatar).toHaveAttribute(
      "aria-label",
      "Deployed by alice@example.com",
    );
    await deployedByAvatar.hover();
    await expect(page.getByRole("tooltip")).toHaveText("alice@example.com");
    await page.waitForTimeout(750);
    await expect(page.getByRole("tooltip")).toBeVisible();
  });

  test("legacy version history route redirects to versions", async ({ page }) => {
    await mockAppAdminRegistry(page, APP, installedRegistryState());
    await page.goto(`/apps/${APP}/admin/history`);

    await expect(page).toHaveURL(new RegExp(`/apps/${APP}/versions$`));
    await expect(page.getByRole("heading", { name: "Versions" })).toBeVisible();
    await expect(page.getByTestId("app-admin-nav-history")).toHaveCount(0);
  });

  test("shows queued for deploy on auto-deploy pending snapshot during rollout", async ({
    page,
  }) => {
    await mockAppAdminRegistry(page, APP, {
      ...installedRegistryState(),
      autoDeploy: {
        enabled: true,
        pendingVersion: PUBLISHED_NEW.version,
      },
      rollout: {
        version: PUBLISHED_LEGACY.version,
        state: "enrolling",
      },
      selectionDisabled: true,
      disabledReason: "rollout in progress",
    });
    await page.goto(`/apps/${APP}/admin`);

    const legacyRow = page.getByTestId("snapshot-row-published").filter({
      hasText: PUBLISHED_LEGACY.version.slice(0, 20),
    });
    const newRow = page.getByTestId("snapshot-row-published").filter({
      hasText: PUBLISHED_NEW.version.slice(0, 20),
    });

    await expect(legacyRow.getByTestId("deploy-version-" + PUBLISHED_LEGACY.version)).toHaveText(
      "Deploying...",
    );
    await expect(newRow.getByTestId("snapshot-status")).toHaveText("Queued to deploy");
  });

  test("explains why manual deploy is unavailable during automatic deploy", async ({
    page,
  }) => {
    await mockAppAdminRegistry(page, APP, {
      ...installedRegistryState(),
      autoDeploy: { enabled: true },
    });
    await page.goto(`/apps/${APP}/admin`);

    await expect(page.getByTestId("manual-deploy-blocked-reason")).toHaveText(
      "To deploy a specific version, turn off automatic deploy.",
    );
    await expect(
      page.getByTestId(`deploy-version-${PUBLISHED_NEW.version}`),
    ).toHaveCount(0);
  });

  test("keeps the automatic-deploy mode note visible while the table refreshes", async ({
    page,
  }) => {
    await mockAppAdminRegistry(page, APP, {
      ...installedRegistryState(),
      autoDeploy: { enabled: true },
      pendingVersions: [PENDING_VERSION],
    });
    await page.goto(`/apps/${APP}/admin`);

    const modeNote = page.getByTestId("manual-deploy-blocked-reason");
    await expect(modeNote).toHaveText(
      "To deploy a specific version, turn off automatic deploy.",
    );

    for (const ms of [1_000, 1_000, 500] as const) {
      await page.clock.runFor(ms);
      await expect(modeNote).toBeVisible();
      await expect(modeNote).toHaveText(
        "To deploy a specific version, turn off automatic deploy.",
      );
    }
  });

  test("shows system:auto-deploy in the versions table", async ({ page }) => {
    await mockAppAdminRegistry(page, APP, installedRegistryState());
    await mockAppAdminRegistryHistory(page, APP, {
      app: APP,
      revisions: [
        {
          id: "rev-auto",
          version: PUBLISHED_NEW.version,
          previousVersion: PUBLISHED_LEGACY.version,
          deployedAt: "2026-07-25T09:10:00Z",
          deployedBy: "system:auto-deploy",
        },
      ],
    });
    await page.goto(`/apps/${APP}/admin`);

    const deployedRow = page
      .getByTestId("snapshot-row-published")
      .filter({ hasText: PUBLISHED_NEW.version.slice(0, 20) });
    await expect(deployedRow.getByTestId("auto-deployed-badge")).toHaveText(
      "Automatically deployed",
    );
    await expect(deployedRow.getByTestId("deployed-by-avatar")).toHaveCount(0);
  });

  test("shows per-app Metrics for app admins", async ({ page }) => {
    await mockIntegrations(page, [MANAGED_INTEGRATION]);
    await mockAppAdminRegistry(page, APP, installedRegistryState());
    await mockAppAuthorizationMembers(page, APP);
    await page.route(
      `**/api/v1/apps/${APP}/admin/metrics`,
      async (route, request) => {
        if (request.method() !== "GET") {
          await route.fallback();
          return;
        }
        await route.fulfill({
          json: {
            app: APP,
            available: true,
            requests: 12,
            errors: 1,
            durationSecondsSum: 0.24,
            durationSecondsCount: 12,
            operations: [
              {
                operation: "list",
                requests: 12,
                errors: 1,
                durationSecondsSum: 0.24,
                durationSecondsCount: 12,
              },
            ],
          },
        });
      },
    );

    await page.goto(`/apps/${APP}/metrics`);
    await expect(page.getByRole("heading", { name: "Metrics" })).toBeVisible();
    await expect(page.getByText("since it started")).toBeVisible();
    await expect(page.getByTestId("app-admin-nav-metrics")).toBeVisible();
    await expect(page.getByRole("cell", { name: "list" })).toBeVisible();
    await expect(page.getByRole("cell", { name: "12" }).first()).toBeVisible();
  });
});
