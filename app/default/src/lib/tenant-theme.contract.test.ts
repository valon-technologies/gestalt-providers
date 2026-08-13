/**
 * Tenant theme contract — required explicit token overrides.
 *
 * Manifest: app/default/shared/tenant-theme-manifest.json
 *
 * Agents: after editing a deployment deploy/ui/theme.css, validate coverage:
 *
 *   cd app/default
 *   GESTALT_THEME_FILE=/path/to/deploy/ui/theme.css \
 *     bun test src/lib/tenant-theme.contract.test.ts
 *
 * CI always validates shared/fixtures/example-tenant-theme.css.
 * See app/default/THEMING.md and docs/agent/theme-boundary.md.
 */

import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";
import {
  THEME_SOURCE_STORAGE_KEY,
} from "@/lib/theme-source";

const appRoot = join(dirname(fileURLToPath(import.meta.url)), "../..");
const manifest = JSON.parse(
  readFileSync(join(appRoot, "shared/tenant-theme-manifest.json"), "utf8"),
);
const bundleTheme = readFileSync(join(appRoot, "shared/theme.css"), "utf8");
const globalsCss = readFileSync(join(appRoot, "src/globals.css"), "utf8");
const indexHtml = readFileSync(join(appRoot, "index.html"), "utf8");
const fixturePath = join(appRoot, manifest.fixture);

function extractCssTokenValue(css: string, token: string): string | null {
  const match = css.match(
    new RegExp(`${token.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}:\\s*([^;]+);`),
  );
  return match?.[1].trim() ?? null;
}

function extractBlock(css: string, selector: string): string {
  const pattern = new RegExp(
    `${selector.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\s*\\{([\\s\\S]*?)\\n\\}`,
  );
  return css.match(pattern)?.[1] ?? "";
}

function declaredTokenValues(
  css: string,
  selector: ":root" | ".dark",
): Map<string, string> {
  const block = extractBlock(css, selector);
  const values = new Map<string, string>();
  for (const match of block.matchAll(/(--[a-z0-9-]+)\s*:\s*([^;]+);/gi)) {
    values.set(match[1], match[2].trim());
  }
  return values;
}

function validateTenantTheme(css: string, themePath: string): string[] {
  const errors: string[] = [];

  for (const [selector, requiredTokens] of Object.entries(manifest.required)) {
    const declared = declaredTokenValues(css, selector as ":root" | ".dark");
    if (declared.size === 0 && extractBlock(css, selector) === "") {
      errors.push(`${themePath}: missing ${selector} block`);
      continue;
    }

    for (const token of requiredTokens as string[]) {
      const value = declared.get(token);
      if (!value) {
        const note = manifest.notes?.[token] ?? "";
        errors.push(
          `${themePath}: ${selector} must declare ${token}${note ? ` (${note})` : ""}`,
        );
        continue;
      }

      const forbidden = manifest.forbiddenValues?.[token] as string[] | undefined;
      if (forbidden?.includes(value)) {
        errors.push(
          `${themePath}: ${selector} ${token} must not alias bundle default (${value})`,
        );
      }
    }
  }

  return errors;
}

test("tenant manifest only lists tokens defined in shared/theme.css", () => {
  const allRequired = new Set(
    Object.values(manifest.required).flatMap((tokens: string[]) => tokens),
  );
  for (const token of allRequired) {
    expect(extractCssTokenValue(bundleTheme, token), token).not.toBeNull();
  }
});

test("secondary body ink is foreground at 80% in light and dark", () => {
  const mix = "color-mix(in oklab, var(--foreground) 80%, transparent)";
  const washedOut = "color-mix(in oklab, var(--foreground) 60%, transparent)";
  const root = extractBlock(bundleTheme, ":where(:root)");
  const dark = extractBlock(bundleTheme, ":where(.dark)");
  expect(root).toContain(`--muted-foreground: ${mix}`);
  expect(dark).toContain(`--muted-foreground: ${mix}`);
  expect(root).not.toContain(`--muted-foreground: ${washedOut}`);
  expect(dark).not.toContain(`--muted-foreground: ${washedOut}`);
  expect(globalsCss).toContain(`--muted-foreground: ${mix}`);
  expect(globalsCss).not.toContain(`--muted-foreground: ${washedOut}`);
});

test("bundle overlay surface defaults are alias traps (documented in manifest)", () => {
  expect(extractCssTokenValue(bundleTheme, "--card")).toBe("var(--surface)");
  expect(extractCssTokenValue(bundleTheme, "--popover")).toBe("var(--surface-raised)");
  expect(manifest.required[":root"]).toContain("--card");
  expect(manifest.required[":root"]).toContain("--popover");
});

test("bundle interaction defaults are alias traps (documented in manifest)", () => {
  expect(extractCssTokenValue(bundleTheme, "--accent-vivid")).toBe(
    "var(--brand-soft)",
  );
  expect(extractCssTokenValue(bundleTheme, "--accent-solid")).toBe("var(--brand)");
  expect(extractCssTokenValue(bundleTheme, "--accent-strong")).toBe("var(--brand)");
  expect(extractCssTokenValue(bundleTheme, "--ring")).toBe("var(--brand)");
  expect(extractCssTokenValue(bundleTheme, "--sidebar")).toBe("var(--surface)");
  expect(extractCssTokenValue(bundleTheme, "--neutral-hover")).toBe(
    "var(--surface-raised)",
  );
  expect(manifest.required[":root"]).toContain("--accent-vivid");
  expect(manifest.required[":root"]).toContain("--accent-solid");
  expect(manifest.required[":root"]).toContain("--ring");
  expect(manifest.required[":root"]).toContain("--sidebar");
});

test("index.html boot script stays aligned with theme-source constants", () => {
  expect(indexHtml).toContain('data-theme-source="tenant"');
  expect(indexHtml).toContain(THEME_SOURCE_STORAGE_KEY);
  expect(indexHtml).toContain('id="tenant-theme"');
});

test("example tenant theme fixture satisfies the contract", () => {
  const css = readFileSync(fixturePath, "utf8");
  const errors = validateTenantTheme(css, fixturePath);
  expect(errors, errors.join("\n")).toEqual([]);
});

test("deployment tenant theme declares required explicit overrides", () => {
  const themeFile = process.env.GESTALT_THEME_FILE?.trim();
  if (!themeFile) {
    return;
  }

  expect(existsSync(themeFile), `GESTALT_THEME_FILE not found: ${themeFile}`).toBe(
    true,
  );

  const css = readFileSync(themeFile, "utf8");
  const errors = validateTenantTheme(css, themeFile);
  expect(errors, errors.join("\n")).toEqual([]);
});
