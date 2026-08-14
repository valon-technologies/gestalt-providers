import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SRC = join(HERE, "..");

const CONTAINER = readFileSync(join(HERE, "Container.tsx"), "utf8");
const GLOBALS = readFileSync(join(SRC, "globals.css"), "utf8");

const PAGE_SHELLS = [
  "features/admin-access/admin-layout.tsx",
  "docs/DocsShell.tsx",
  "components/SettingsLayout.tsx",
  "components/AppsCatalogPageClient.tsx",
  "pages/build.tsx",
  "pages/app-workspace-layout.tsx",
] as const;

function extractCssTokenValue(css: string, token: string): string {
  const match = css.match(
    new RegExp(`${token.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}:\\s*([^;]+);`),
  );
  expect(match).not.toBeNull();
  return match![1].trim();
}

describe("Container page padding contract", () => {
  test("owns one vertical page inset for every route", () => {
    expect(CONTAINER).toContain('"w-full py-16"');
    expect(CONTAINER).toContain('"mx-auto w-full max-w-7xl px-6 py-16"');
    expect(CONTAINER).not.toMatch(/\b(pt|pb|py)-(8|12|24)\b/);
  });

  test("sticky Pane gap matches Container py-16 (4rem)", () => {
    expect(extractCssTokenValue(GLOBALS, "--page-layout-pane-gap")).toBe("4rem");
  });

  test("page shells do not re-pad Container", () => {
    for (const relative of PAGE_SHELLS) {
      const source = readFileSync(join(SRC, relative), "utf8");
      expect(source, relative).not.toMatch(
        /<Container\b[^>]*className="[^"]*\b(pt|pb|py)-\d+/,
      );
    }
  });
});
