import { existsSync, mkdirSync, readdirSync, unlinkSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import {
  expect,
  mockIntegrationOperations,
  mockIntegrations,
  test,
} from "./fixtures";
import type { Integration, IntegrationOperation } from "../src/lib/api";

const SIGNAL_DIR = join(homedir(), ".cache", "gestalt-operations-demo");
const READY = join(SIGNAL_DIR, "READY");
const GO = join(SIGNAL_DIR, "GO");

const APP = "g-issues";

const DEMO_OPERATIONS: IntegrationOperation[] = [
  {
    id: "drafts.create",
    description: "Create an email draft",
    method: "POST",
    visible: true,
  },
  {
    id: "drafts.delete",
    description:
      "Delete a draft by thread and message id (Gmail marks as trashed for default drafts)",
    method: "DELETE",
    visible: true,
  },
  {
    id: "drafts.get",
    description: "Get a draft by thread and message id",
    method: "GET",
    visible: true,
  },
  {
    id: "drafts.list",
    description: "List drafts in the mailbox",
    method: "GET",
    visible: true,
  },
  {
    id: "drafts.update",
    description: "Update an existing draft",
    method: "PUT",
    visible: true,
  },
  {
    id: "profile.get",
    description: "Get the authenticated user's profile",
    method: "GET",
    visible: true,
  },
  {
    id: "issues.create",
    description: "Create a new issue in the repository",
    method: "POST",
    visible: true,
  },
  {
    id: "issues.list",
    description: "List issues for a repository",
    method: "GET",
    visible: true,
  },
  {
    id: "issues.update",
    description: "Update issue fields",
    method: "PATCH",
    visible: true,
  },
  {
    id: "attachments.upload",
    description: "Upload a file attachment to an issue",
    method: "POST",
    visible: true,
  },
];

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

test.describe("Record operations demo", () => {
  test.skip(!!process.env.CI, "Manual scap capture driver — not for CI");
  test.setTimeout(120_000);

  test("drive operations reference for scap capture", async ({ authenticatedPage: page }) => {
    mkdirSync(SIGNAL_DIR, { recursive: true });
    for (const f of [READY, GO]) {
      try {
        unlinkSync(f);
      } catch {}
    }

    const integration: Integration = {
      name: APP,
      displayName: "G Issues",
      description: "Issue tracking integration",
    };
    await mockIntegrations(page, [integration]);
    await mockIntegrationOperations(page, { [APP]: DEMO_OPERATIONS });

    writeFileSync(READY, "1");

    const deadline = Date.now() + 120_000;
    while (!existsSync(GO) && Date.now() < deadline) {
      await page.waitForTimeout(200);
    }
    if (!existsSync(GO)) {
      throw new Error("Timed out waiting for GO before demo");
    }

    await page.addStyleTag({
      content: "nextjs-portal,[data-nextjs-dialog-overlay]{display:none!important}",
    });

    await page.goto(`/apps/${APP}/operations`);
    await expect(page.getByRole("heading", { name: "Operations" })).toBeVisible();
    await expect(page.getByTestId("app-operations-reference")).toBeVisible();
    await page.waitForTimeout(2500);

    await page.evaluate(() => window.scrollBy({ top: 360, behavior: "smooth" }));
    await page.waitForTimeout(2500);

    const search = page.getByTestId("app-operations-search");
    await search.click();
    await search.fill("draft");
    await page.waitForTimeout(2000);

    await search.fill("");
    await page.waitForTimeout(1500);

    await page.getByRole("button", { name: "Issues" }).click();
    await page.waitForTimeout(2000);

    await page.goto(`/apps/${APP}/operations#issues.list`);
    await page.waitForTimeout(2500);

    await page.evaluate(() => window.scrollTo({ top: 0, behavior: "smooth" }));
    await page.waitForTimeout(1500);
  });
});
