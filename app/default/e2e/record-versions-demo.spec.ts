import { existsSync, mkdirSync, readdirSync, unlinkSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import {
  expect,
  mockAppAdminRegistry,
  mockAppAdminRegistryHistory,
  mockAuthSession,
  test,
} from "./fixtures";
import type { AppAdminRegistryResponse } from "../src/lib/api";

const SIGNAL_DIR = join(homedir(), ".cache", "gestalt-versions-demo");
const READY = join(SIGNAL_DIR, "READY");
const GO = join(SIGNAL_DIR, "GO");

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

function demoRegistryState(): AppAdminRegistryResponse {
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
    pendingVersions: [
      {
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
      },
    ],
    rollout: {
      version: PUBLISHED_LEGACY.version,
      state: "complete",
    },
    autoDeploy: {
      enabled: true,
    },
    selectionDisabled: false,
  };
}

function chromeForTestingPath() {
  const root = join(homedir(), "Library/Caches/ms-playwright");
  for (const v of readdirSync(root)
    .filter((n) => n.startsWith("chromium-"))
    .sort()
    .reverse()) {
    const bin = join(
      root,
      v,
      "chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
    );
    if (existsSync(bin)) return bin;
  }
  return undefined;
}

test.use({
  headless: false,
  launchOptions: {
    executablePath: chromeForTestingPath(),
    args: ["--window-size=1440,900", "--window-position=30,30"],
  },
});

test.describe("Record versions demo", () => {
  test.skip(!!process.env.CI, "Manual scap capture driver — not for CI");
  test.setTimeout(120_000);

  test("drive versions admin for scap capture", async ({ authenticatedPage: page }) => {
    mkdirSync(SIGNAL_DIR, { recursive: true });
    for (const f of [READY, GO]) {
      try {
        unlinkSync(f);
      } catch {}
    }

    await page.clock.install({ time: new Date("2026-07-23T15:00:00Z") });
    await mockAuthSession(page);
    await mockAppAdminRegistry(page, APP, demoRegistryState());
    await mockAppAdminRegistryHistory(page, APP, {
      app: APP,
      revisions: [
        {
          id: "rev-3",
          version: PUBLISHED_NEW.version,
          previousVersion: PUBLISHED_LEGACY.version,
          deployedAt: "2026-07-25T09:10:00Z",
          deployedBy: "user:alice@example.com",
          deploymentState: "desired",
          current: true,
          publication: PUBLISHED_NEW.publication,
        },
      ],
    });

    writeFileSync(READY, "1");

    for (let i = 0; i < 240; i += 1) {
      if (existsSync(GO)) break;
      await page.waitForTimeout(100);
    }

    await page.goto(`/apps/${APP}/versions`);
    await expect(page.getByRole("heading", { name: "Versions" })).toBeVisible();
    await expect(page.getByTestId("app-admin-fleet-state")).toBeVisible();
    await expect(page.getByTestId("snapshots-table")).toBeVisible();
    await expect(page.getByTestId("deployed-version-badge").first()).toHaveText(
      "Deployed Version",
    );

    await page.getByTestId("deployed-by-avatar").first().hover();
    await expect(page.getByRole("tooltip")).toBeVisible();

    await page.mouse.wheel(0, 220);
    await page.waitForTimeout(1200);
    await page.mouse.wheel(0, -120);
    await page.waitForTimeout(800);
  });
});
