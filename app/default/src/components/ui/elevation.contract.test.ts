import { expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const THEME = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "../../../shared/theme.css"),
  "utf8",
);
const GLOBALS = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "../../globals.css"),
  "utf8",
);

const ELEVATION_SHADOW_TOKENS = ["--elevation-shadow-md", "--elevation-shadow-lg"] as const;
const NEUTRALIZED_SHADOW_UTILITIES = [
  "--shadow-2xs",
  "--shadow-xs",
  "--shadow-sm",
  "--shadow",
  "--shadow-xl",
  "--shadow-2xl",
] as const;
const NONE_SHADOW = "0 0 #0000";
const WARM_INK = "color-mix(in oklab, var(--foreground)";

function extractCssTokenValue(css: string, token: string): string {
  const match = css.match(
    new RegExp(`${token.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}:\\s*([^;]+);`),
  );
  expect(match).not.toBeNull();
  return match![1].trim();
}

test("elevation shadow primitives use warm foreground ink in shared/theme.css", () => {
  for (const token of ELEVATION_SHADOW_TOKENS) {
    const value = extractCssTokenValue(THEME, token);
    expect(value).toContain(WARM_INK);
    expect(value).not.toMatch(/rgb\(0 0 0/);
  }
});

test("elevation shadow utilities alias semantic tokens in globals @theme inline", () => {
  expect(extractCssTokenValue(GLOBALS, "--shadow-md")).toBe("var(--elevation-shadow-md)");
  expect(extractCssTokenValue(GLOBALS, "--shadow-lg")).toBe("var(--elevation-shadow-lg)");
});

test("non-tier shadow utilities are neutralized in globals @theme inline", () => {
  for (const utility of NEUTRALIZED_SHADOW_UTILITIES) {
    expect(extractCssTokenValue(GLOBALS, utility)).toBe(NONE_SHADOW);
  }
});

test("segmented-control sliding pill stays flat on canvas", () => {
  const source = readFileSync(
    join(dirname(fileURLToPath(import.meta.url)), "segmented-control.tsx"),
    "utf8",
  );
  expect(source).toContain("No shadow — on-canvas sliding pill is flat");
  expect(source).not.toMatch(/shadow-sm/);
});
