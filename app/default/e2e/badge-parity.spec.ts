import {
  expect,
  mockAppAdminRegistry,
  mockAuthSession,
  test,
} from "./fixtures";

const APP = "g-issues";
const STORYBOOK_URL =
  process.env.BADGE_STORYBOOK_URL ??
  "http://127.0.0.1:6049/?path=/story/display-badge--variants";

const PUBLISHED_NEW = {
  version: "0.0.0-snapshot.gdef456",
  publishedAt: "2026-07-22T15:00:00Z",
  platforms: ["linux/amd64"],
  sourceRef: "def456def456def456def456def456def456def4",
};

const PUBLISHED_LEGACY = {
  version: "0.0.0-snapshot.gabc123",
  publishedAt: "2026-07-21T12:00:00Z",
  platforms: ["linux/amd64"],
  sourceRef: "abc123abc123abc123abc123abc123abc123ab",
};

async function badgeColors(locator: import("@playwright/test").Locator) {
  await expect(locator).toBeVisible({ timeout: 10_000 });
  return locator.evaluate((el) => {
    const style = getComputedStyle(el);
    return {
      backgroundColor: style.backgroundColor,
      color: style.color,
    };
  });
}

test.describe("badge parity with registry storybook", () => {
  test.skip(
    !process.env.BADGE_PARITY_CHECK,
    "Set BADGE_PARITY_CHECK=1 with storybook (6049) running",
  );

  test("current and available match storybook info/success colors", async ({ page }) => {
    await page.goto(STORYBOOK_URL);
    const frame = page.locator("#storybook-preview-iframe").contentFrame();
    if (!frame) throw new Error("Storybook preview iframe not found");

    const storybookInfo = await badgeColors(frame.getByText("info", { exact: true }).first());

    await mockAuthSession(page);
    await mockAppAdminRegistry(page, APP, {
      app: APP,
      registry: "toolshed",
      desiredVersion: PUBLISHED_LEGACY.version,
      knownVersions: [],
      publishedVersions: [PUBLISHED_NEW, PUBLISHED_LEGACY],
      rollout: { version: PUBLISHED_LEGACY.version, state: "complete" },
      selectionDisabled: false,
    });
    await page.goto(`/apps/${APP}/admin`);

    const current = await badgeColors(page.getByTestId("snapshot-status").filter({ hasText: "Current" }));

    expect(current).toEqual(storybookInfo);
    await expect(page.getByTestId("table-status-indicator").first()).toBeVisible();
  });
});
