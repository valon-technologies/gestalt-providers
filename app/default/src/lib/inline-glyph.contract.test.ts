import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const LIB = readFileSync(join(HERE, "inline-glyph.ts"), "utf8");
const CARET = readFileSync(join(HERE, "disclosure-caret.ts"), "utf8");
const THEME = readFileSync(join(HERE, "../globals.css"), "utf8");

describe("inline glyph tokens", () => {
  test("theme maps 1em size and 0.5em gap plus cap-band align", () => {
    expect(THEME).toContain("--inline-glyph-size: 1em;");
    expect(THEME).toContain("--inline-glyph-gap: 0.5em;");
    expect(THEME).toContain("--size-inline-glyph: var(--inline-glyph-size);");
    expect(THEME).toContain("--spacing-inline-glyph: var(--inline-glyph-gap);");
    expect(THEME).toContain("@utility align-inline-glyph");
    expect(THEME).toContain("vertical-align: calc((1cap - 1em) / 2);");
    expect(THEME).toContain("@utility stroke-inline-glyph");
    expect(THEME).toContain("--inline-glyph-stroke: 3.5;");
    expect(THEME).toContain("--inline-glyph-stroke-medium: 6;");
    expect(THEME).toContain("--inline-glyph-stroke-semibold: 6.5;");
    expect(THEME).toContain("--inline-glyph-stroke-bold: 7;");
    expect(THEME).toContain(".font-medium :where(.stroke-inline-glyph)");
    expect(THEME).toContain("color: inherit;");
    expect(THEME).toContain("stroke: currentColor;");
  });
});

describe("inlineGlyphClassName", () => {
  test("is the inline-block recipe (size + align + trailing gap)", () => {
    expect(LIB).toContain(
      "inline-block size-inline-glyph shrink-0 align-inline-glyph me-inline-glyph stroke-inline-glyph text-current",
    );
    expect(LIB).not.toContain("mr-1.5");
    expect(LIB).not.toContain("size-4");
  });
});

describe("disclosureCaretClassName", () => {
  test("is motion only — no rem size or margin", () => {
    expect(CARET).toContain("duration-overshoot");
    expect(CARET).toContain("ease-out-back");
    const exported = CARET.match(
      /export const disclosureCaretClassName =\s*"([^"]+)"/,
    )?.[1];
    expect(exported).toBeTruthy();
    expect(exported).not.toContain("size-4");
    expect(exported).not.toContain("ml-");
    expect(exported).not.toContain("opacity-50");
  });
});
