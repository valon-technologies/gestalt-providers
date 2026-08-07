import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, test } from "vitest";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "DevWorktreeBanner.tsx"),
  "utf8",
);

const ROOT = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "../routes/__root.tsx"),
  "utf8",
);

const NAV = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "Nav.tsx"),
  "utf8",
);

describe("DevWorktreeBanner", () => {
  test("uses Registry Alert banner layout and semantic warning wash", () => {
    expect(SOURCE).toContain('layout="banner"');
    expect(SOURCE).toContain('variant="warning"');
    expect(SOURCE).toContain("rounded-none border-b border-border");
    expect(SOURCE).toContain('data-testid="dev-worktree-banner"');
    expect(SOURCE).toContain("readDevWorktreeName");
    expect(SOURCE).not.toMatch(/\b(amber|grove|ember)-/);
  });

  test("mounts in root chrome above Nav inside the sticky stack", () => {
    expect(ROOT).toContain("DevWorktreeBanner");
    expect(ROOT).toContain("<DevWorktreeBanner />");
    expect(ROOT).toContain("isLocalDevChrome");
    expect(ROOT).toContain("ThemeSwitcher");
    expect(ROOT).toContain("sticky top-0 z-50");
    expect(ROOT).toContain('data-slot="app-sticky-chrome"');
    expect(ROOT).toContain("useSyncStickyAppChromeHeight");
    expect(NAV).not.toContain("sticky top-0");
    const bannerIndex = ROOT.indexOf("<DevWorktreeBanner");
    const navIndex = ROOT.indexOf("<Nav");
    expect(bannerIndex).toBeGreaterThan(-1);
    expect(navIndex).toBeGreaterThan(bannerIndex);
  });
});
