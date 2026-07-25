import {
  expect,
  mockAppAdminRegistry,
  mockAuthSession,
  mockIntegrations,
  test,
} from "./fixtures";
import type { AppAdminRegistryResponse, Integration } from "../src/lib/api";

const APP = "g-issues";

const PUBLISHED_NEW: AppAdminRegistryResponse["publishedVersions"][number] = {
  version: "0.0.0-snapshot.gdef456",
  publishedAt: "2026-07-22T15:00:00Z",
  platforms: ["linux/amd64"],
  sourceRef: "def456def456def456def456def456def456def4",
  sourceUrl:
    "https://github.com/valon-technologies/valon-tools/commit/def456def456def456def456def456def456def4",
  publication: {
    workflowRunUrl:
      "https://github.com/valon-technologies/valon-tools/actions/runs/123456789",
    triggerPullRequest: {
      number: 3251,
      url: "https://github.com/valon-technologies/valon-tools/pull/3251",
      title: "Add registry deploy banner",
    },
  },
};

const PUBLISHED_LEGACY: AppAdminRegistryResponse["publishedVersions"][number] = {
  version: "0.0.0-snapshot.gabc123",
  publishedAt: "2026-07-21T12:00:00Z",
  platforms: ["linux/amd64"],
  sourceRef: "abc123abc123abc123abc123abc123abc123ab",
  sourceUrl:
    "https://github.com/valon-technologies/valon-tools/commit/abc123abc123abc123abc123abc123abc123ab",
};

const PENDING_VERSION: NonNullable<AppAdminRegistryResponse["pendingVersions"]>[number] = {
  version: "0.0.0-snapshot.gpending01",
  startedAt: "2026-07-23T14:56:00Z",
  updatedAt: "2026-07-23T14:56:00Z",
  phase: "publishing",
  publishingForSeconds: 240,
  publication: {
    workflowRunUrl:
      "https://github.com/valon-technologies/valon-tools/actions/runs/223456789",
    triggerPullRequest: {
      number: 3740,
      url: "https://github.com/valon-technologies/valon-tools/pull/3740",
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
    workflowRunUrl:
      "https://github.com/valon-technologies/valon-tools/actions/runs/323456789",
    triggerPullRequest: {
      number: 3788,
      url: "https://github.com/valon-technologies/valon-tools/pull/3788",
      title: "Retry registry publish",
    },
  },
};

const MANAGED_INTEGRATION: Integration = {
  name: APP,
  displayName: "G Issues",
  mountedPath: `/${APP}`,
  managementPath: `/apps/${APP}/admin`,
};

const UNMANAGED_INTEGRATION: Integration = {
  name: "slack",
  displayName: "Slack",
};

function installedRegistryState(): AppAdminRegistryResponse {
  return {
    app: APP,
    registry: "toolshed",
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
    selectionDisabled: false,
  };
}

const APP_ADMIN_FIXED_TIME = new Date("2026-07-23T15:00:00Z");

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

  test("shows Manage app only when managementPath is returned", async ({ page }) => {
    await mockIntegrations(page, [MANAGED_INTEGRATION, UNMANAGED_INTEGRATION]);
    await page.goto("/apps");

    await expect(page.getByTestId(`manage-app-${APP}`)).toBeVisible();
    await expect(page.getByTestId("manage-app-slack")).toHaveCount(0);
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
    await expect(pendingRow).toContainText("Publishing");
    await expect(pendingRow).toContainText("for 4m");
    await expect(pendingRow).toContainText("PR #3740");
    await expect(pendingRow.getByTestId(`deploy-version-${PENDING_VERSION.version}`)).toHaveCount(0);

    const failedRow = page.getByTestId("snapshot-row-failed");
    await expect(failedRow).toContainText(FAILED_VERSION.version.slice(0, 20));
    await expect(failedRow).toContainText("Failed");
    await expect(failedRow).toContainText("after 35m");
    await expect(failedRow).toContainText("stale");
    await expect(failedRow.getByTestId(`deploy-version-${FAILED_VERSION.version}`)).toHaveCount(0);
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
    await expect(page.getByText("in 4m 32s")).toBeVisible();
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
    await expect(rows.nth(0)).toContainText("yesterday");
    await expect(rows.nth(1)).toContainText(PUBLISHED_LEGACY.version.slice(0, 20));
    await expect(rows.nth(1)).toContainText("Deployed");
  });

  test("links to the mounted app page when mountedPath is available", async ({ page }) => {
    await mockIntegrations(page, [MANAGED_INTEGRATION]);
    await mockAppAdminRegistry(page, APP, installedRegistryState());
    await page.goto(`/apps/${APP}/admin`);

    const openAppLink = page.getByTestId("open-app-link");
    await expect(openAppLink).toBeVisible();
    await expect(openAppLink).toHaveAttribute("href", `/${APP}`);
    await expect(openAppLink).toHaveText("Open app →");
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
      registry: "toolshed",
      knownVersions: [],
      publishedVersions: [PUBLISHED_NEW],
      selectionDisabled: false,
    });
    await page.goto(`/apps/${APP}/admin`);

    await expect(page.getByText(/No version is installed yet/i)).toBeVisible();
    await expect(page.getByTestId("snapshot-row-published")).toHaveCount(1);
  });

  test("disables deploy buttons during active rollout", async ({ page }) => {
    await mockAppAdminRegistry(page, APP, {
      ...installedRegistryState(),
      rollout: {
        version: PUBLISHED_NEW.version,
        state: "enrolling",
      },
      selectionDisabled: true,
      disabledReason: "rollout in progress",
    });
    await page.goto(`/apps/${APP}/admin`);

    await expect(page.getByTestId("rollout-active-banner")).toContainText(
      "Rollout enrolling",
    );
    await expect(page.getByTestId(`deploy-version-${PUBLISHED_NEW.version}`)).toBeDisabled();
    await expect(page.getByTestId(`deploy-version-${PUBLISHED_LEGACY.version}`)).toBeDisabled();
    await expect(page.getByTestId("selection-disabled-reason")).toHaveText(
      "rollout in progress",
    );
  });

  test("successful deploy shows the new active rollout", async ({ page }) => {
    await mockAppAdminRegistry(page, APP, installedRegistryState(), {
      onSelectVersion: (version) => ({
        app: APP,
        registry: "toolshed",
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

    await expect(page.getByTestId("rollout-active-banner")).toContainText(
      PUBLISHED_NEW.version,
    );
    await expect(page.getByTestId(`deploy-version-${PUBLISHED_NEW.version}`)).toBeDisabled();
    await expect(page.getByTestId("rollout-badge")).toHaveText("enrolling");
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
    await expect(page.getByTestId(`deploy-version-${PUBLISHED_LEGACY.version}`)).toBeDisabled();
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
    await expect(page.getByText("Publishing")).toBeVisible();
  });

  test("403 renders access denied without registry metadata", async ({ page }) => {
    await page.route(`**/api/v1/apps/${APP}/admin/registry`, (route) => {
      route.fulfill({ status: 403, json: { error: "app access denied" } });
    });
    await page.goto(`/apps/${APP}/admin`);

    await expect(page.getByTestId("app-admin-access-denied")).toBeVisible();
    await expect(page.getByText("Access denied")).toBeVisible();
    await expect(page.getByTestId("snapshots-table")).toHaveCount(0);
    await expect(page.getByText("toolshed")).toHaveCount(0);
  });
});
