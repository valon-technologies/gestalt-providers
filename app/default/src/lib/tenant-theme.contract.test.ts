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
 * Without GESTALT_THEME_FILE, only bundle/manifest integrity tests run.
 * See app/default/THEMING.md and docs/agent/theme-boundary.md.
 */

import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";

const appRoot = join(dirname(fileURLToPath(import.meta.url)), "../..");
const manifest = JSON.parse(
  readFileSync(join(appRoot, "shared/tenant-theme-manifest.json"), "utf8"),
);
const bundleTheme = readFileSync(join(appRoot, "shared/theme.css"), "utf8");

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

function declaredTokens(block: string): Set<string> {
  const tokens = new Set<string>();
  for (const match of block.matchAll(/(--[a-z0-9-]+)\s*:/gi)) {
    tokens.add(match[1]);
  }
  return tokens;
}

function validateTenantTheme(css: string, themePath: string): string[] {
  const errors: string[] = [];
  const blocks = {
    ":root": extractBlock(css, ":root"),
    ".dark": extractBlock(css, ".dark"),
  };

  for (const [selector, requiredTokens] of Object.entries(manifest.required)) {
    const block = blocks[selector as keyof typeof blocks];
    if (!block) {
      errors.push(`${themePath}: missing ${selector} block`);
      continue;
    }

    const declared = declaredTokens(block);
    for (const token of requiredTokens as string[]) {
      if (!declared.has(token)) {
        const note = manifest.notes?.[token] ?? "";
        errors.push(
          `${themePath}: ${selector} must declare ${token}${note ? ` (${note})` : ""}`,
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

test("bundle popover default is an alias trap (documented in manifest)", () => {
  expect(extractCssTokenValue(bundleTheme, "--popover")).toBe("var(--surface-raised)");
  expect(manifest.required[":root"]).toContain("--popover");
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
