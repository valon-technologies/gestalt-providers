import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { SECTION_HEADER_ICON_STACK } from "./section-header";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "section-header.tsx"),
  "utf8",
);

/** Tailwind spacing scale: class → px at default 16px root. */
const TW_PX: Record<string, number> = {
  "gap-y-1.5": 6,
  "gap-y-2": 8,
  "gap-y-2.5": 10,
  "pt-5.5": 22,
  "pt-8": 32,
  "pt-10.5": 42,
};

const SVG_PX: Record<string, number> = {
  "[&_svg:not([class*='size-'])]:size-4": 16,
  "[&_svg:not([class*='size-'])]:size-6": 24,
  "[&_svg:not([class*='size-'])]:size-8": 32,
};

describe("SectionHeader", () => {
  test("uses createHeaderChrome with section title mode", () => {
    expect(SOURCE).toContain("createHeaderChrome");
    expect(SOURCE).toContain("SECTION_HEADER_TIERS");
    expect(SOURCE).toContain("createHeaderChromeScale");
    expect(SOURCE).toContain('title: { kind: "section"');
    expect(SOURCE).toContain('alignBetweenItems: "sm:items-baseline"');
    expect(SOURCE).toContain('title: "font-display text-heading-lg tracking-heading"');
    expect(SOURCE).toContain("SECTION_HEADER_ICON_STACK");
    expect(SOURCE).toContain(
      "[&:has([data-slot=section-header-content][data-size=default])]:gap-y-2.5",
    );
  });

  test("icon stack padding equals svg box + content gap per tier", () => {
    for (const tier of ["sm", "lg", "default", "md"] as const) {
      const { svg, gapY, textPad } = SECTION_HEADER_ICON_STACK[tier];
      expect(TW_PX[textPad]).toBe(SVG_PX[svg]! + TW_PX[gapY]!);
    }
  });
});
