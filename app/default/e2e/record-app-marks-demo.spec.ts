import { existsSync, mkdirSync, readdirSync, readFileSync, unlinkSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { expect, mockIntegrations, test } from "./fixtures";
import type { Integration } from "../src/lib/api";

const SIGNAL_DIR = join(homedir(), ".cache", "gestalt-app-marks-demo");
const READY = join(SIGNAL_DIR, "READY");
const GO = join(SIGNAL_DIR, "GO");

const APPS_DIR = join(import.meta.dirname, "..", "..", "..", "app");

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

function loadShippedIcons(limit = 24): Integration[] {
  const icons: Integration[] = [];
  for (const app of readdirSync(APPS_DIR, { withFileTypes: true })) {
    if (!app.isDirectory()) continue;
    let manifest: string;
    try {
      manifest = readFileSync(join(APPS_DIR, app.name, "manifest.yaml"), "utf8");
    } catch {
      continue;
    }
    const iconFile = /^iconFile:\s*(\S+)\s*$/m.exec(manifest)?.[1];
    if (!iconFile) continue;
    icons.push({
      name: app.name,
      displayName: app.name.replace(/_/g, " "),
      iconSvg: readFileSync(join(APPS_DIR, app.name, iconFile), "utf8"),
    });
    if (icons.length >= limit) break;
  }
  return icons;
}

test.use({
  headless: false,
  launchOptions: {
    executablePath: chromeForTestingPath(),
    args: ["--window-size=1440,900", "--window-position=30,30"],
  },
});

test.describe("Record app marks demo", () => {
  test.skip(!!process.env.CI, "Manual scap capture driver — not for CI");
  test.setTimeout(120_000);

  test("drive apps catalog for scap capture", async ({ authenticatedPage: page }) => {
    mkdirSync(SIGNAL_DIR, { recursive: true });
    for (const f of [READY, GO]) {
      try {
        unlinkSync(f);
      } catch {}
    }

    const integrations: Integration[] = [
      ...loadShippedIcons(28),
      { name: "acmeHub", displayName: "Acme Hub" },
      { name: "llm", displayName: "LLM" },
      { name: "dataRecordExplorer" },
    ];
    await mockIntegrations(page, integrations);

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

    await page.goto("/apps");
    await expect(page.getByRole("heading", { name: "Apps" })).toBeVisible();
    await page.waitForTimeout(2500);

    await page.evaluate(() => window.scrollBy({ top: 420, behavior: "smooth" }));
    await page.waitForTimeout(2500);

    await page.evaluate(() => window.scrollBy({ top: 420, behavior: "smooth" }));
    await page.waitForTimeout(2500);

    const themeToggle = page.getByRole("button", { name: /theme/i });
    if (await themeToggle.count()) {
      await themeToggle.first().click();
      await page.waitForTimeout(2000);
      await themeToggle.first().click();
      await page.waitForTimeout(1500);
    }

    await page.evaluate(() => window.scrollTo({ top: 0, behavior: "smooth" }));
    await page.waitForTimeout(2000);
  });
});
