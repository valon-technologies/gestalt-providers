import { existsSync, mkdirSync, readdirSync, unlinkSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { expect, test } from "./fixtures";

const BASE_URL = process.env.LOCAL_DEV_URL || "http://127.0.0.1:3107";
const APP = process.env.RECORD_APP || "g-issues";
const SIGNAL_DIR = join(homedir(), ".cache", "gestalt-snapshots-rollout-demo");
const READY = join(SIGNAL_DIR, "READY");
const GO = join(SIGNAL_DIR, "GO");

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
  baseURL: BASE_URL,
  headless: false,
  viewport: { width: 1440, height: 900 },
  launchOptions: {
    executablePath: chromeForTestingPath(),
    args: ["--window-size=1440,900", "--window-position=30,30"],
  },
});

test.describe("Record snapshots rollout demo", () => {
  test.skip(!!process.env.CI, "Manual scap capture driver — not for CI");
  test.setTimeout(120_000);

  test("drive g-issues snapshots page for scap capture", async ({ page }) => {
    mkdirSync(SIGNAL_DIR, { recursive: true });
    for (const f of [READY, GO]) {
      try {
        unlinkSync(f);
      } catch {}
    }

    writeFileSync(READY, "1");

    if (!process.env.RECORD_DEMO_AUTO_GO) {
      const deadline = Date.now() + 120_000;
      while (!existsSync(GO) && Date.now() < deadline) {
        await page.waitForTimeout(200);
      }
      if (!existsSync(GO)) {
        throw new Error("Timed out waiting for GO before demo");
      }
    }

    await page.addStyleTag({
      content: "nextjs-portal,[data-nextjs-dialog-overlay]{display:none!important}",
    });

    await page.goto(`/apps/${APP}/admin/snapshots`);
    await expect(page.getByRole("heading", { name: "Published snapshots" })).toBeVisible({
      timeout: 30_000,
    });
    await expect(page.getByTestId("rollout-phase-stepper")).toBeVisible();
    await expect(page.getByTestId("app-admin-auto-deploy")).toBeVisible();
    await expect(page.getByTestId("snapshots-table")).toBeVisible();
    await page.waitForTimeout(3000);

    await page.evaluate(() => window.scrollBy({ top: 180, behavior: "smooth" }));
    await page.waitForTimeout(2500);

    await page.evaluate(() => window.scrollTo({ top: 0, behavior: "smooth" }));
    await page.waitForTimeout(2500);
  });
});
