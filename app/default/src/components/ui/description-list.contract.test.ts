import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(HERE, "description-list.tsx"), "utf8");

describe("DescriptionList", () => {
  test("exposes plain and outline surfaces via Card outline chrome", () => {
    expect(SOURCE).toContain("surface: {");
    expect(SOURCE).toContain('plain: ""');
    expect(SOURCE).toContain("outline: cn(");
    expect(SOURCE).toContain('cardVariants({ variant: "outline" })');
    expect(SOURCE).toContain("overflow-hidden rounded-lg");
    // Horizontal pad on rows; vertical rhythm is owned by density.
    expect(SOURCE).toContain("[&_[data-slot=description-item]]:px-4");
    expect(SOURCE).not.toContain(
      "[&_[data-slot=description-item]]:px-4 [&_[data-slot=description-item]]:py-3",
    );
    expect(SOURCE).toContain('data-surface={surface ?? "plain"}');
  });

  test("exposes default and condensed density on the list", () => {
    expect(SOURCE).toContain("density: {");
    expect(SOURCE).toContain(
      'default: "[&_[data-slot=description-item]]:py-3"',
    );
    expect(SOURCE).toContain(
      'condensed: "[&_[data-slot=description-item]]:py-1.5"',
    );
    expect(SOURCE).toContain('density: "default"');
    expect(SOURCE).toContain('data-density={density ?? "default"}');
  });

  test("DescriptionTerm uses scannable display caption type", () => {
    expect(SOURCE).toContain(
      "font-display text-sm italic tracking-wide text-muted-foreground",
    );
  });

  test("keeps semantic dl / dt / dd slots", () => {
    expect(SOURCE).toContain('data-slot="description-list"');
    expect(SOURCE).toContain('data-slot="description-item"');
    expect(SOURCE).toContain('data-slot="description-term"');
    expect(SOURCE).toContain('data-slot="description-details"');
    expect(SOURCE).toContain("<dl");
    expect(SOURCE).toContain("<dt");
    expect(SOURCE).toContain("<dd");
  });

  test("imports cn from @/lib/cn (console vendor convention)", () => {
    expect(SOURCE).toContain('from "@/lib/cn"');
    expect(SOURCE).not.toContain('from "@/lib/utils"');
  });

  test("DescriptionDetails uses text-pretty to avoid wrapping orphans", () => {
    expect(SOURCE).toContain("break-words text-pretty");
  });
});
