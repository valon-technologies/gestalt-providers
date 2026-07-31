import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "vitest";

const SRC_DIR = dirname(fileURLToPath(import.meta.url));
const THEME = readFileSync(join(SRC_DIR, "../shared/theme.css"), "utf8");
const GLOBALS = readFileSync(join(SRC_DIR, "globals.css"), "utf8");

function tokenValues(css: string, token: string): string[] {
  const escapedToken = token.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return [...css.matchAll(new RegExp(`${escapedToken}:\\s*([^;]+);`, "g"))].map(
    (match) => match[1].trim(),
  );
}

test("promo stage has semantic light and dark theme definitions", () => {
  const definitions = tokenValues(THEME, "--promo-stage-background");
  expect(definitions).toHaveLength(2);
  expect(definitions[0]).toContain("var(--accent)");
  expect(definitions[0]).toContain("var(--background)");
  expect(definitions[1]).toContain("var(--accent-strong)");
  expect(definitions[1]).toContain("var(--card)");
  for (const definition of definitions) {
    expect(definition).not.toMatch(/#[0-9a-f]{3,8}|rgba?\(|oklch\(/i);
  }
});

test("promo stage owns one Tailwind bridge without legacy gradient aliases", () => {
  expect(tokenValues(GLOBALS, "--background-image-promo-stage")).toEqual([
    "var(--promo-stage-background)",
  ]);
  expect(THEME).not.toMatch(/--gradient-(?:hero|card-hover)/);
  expect(GLOBALS).not.toMatch(/(?:--background-image-|bg-)gradient-(?:hero|card-hover)/);
});
