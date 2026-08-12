import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(HERE, "brand-spinner.tsx"), "utf8");
const GLOBALS = readFileSync(join(HERE, "../../globals.css"), "utf8");

describe("BrandSpinner", () => {
  test("lights whole V pieces via trail hooks (not path-dash snake)", () => {
    expect(SOURCE).toContain('className="brand-spinner__trail"');
    expect(SOURCE).toContain('className="brand-spinner__track"');
    expect(SOURCE).toContain('"--piece-index"');
    expect(SOURCE).toContain("BrandSpinner");
    expect(SOURCE).not.toContain("spinner__snake");
    expect(SOURCE).not.toMatch(/valon-spinner/);
  });

  test("imports cn from @/lib/cn and strips use client", () => {
    expect(SOURCE).toContain('from "@/lib/cn"');
    expect(SOURCE).not.toContain('from "@/lib/utils"');
    expect(SOURCE).not.toContain('"use client"');
  });

  test("stays tenant-neutral in comments (no org product brand)", () => {
    expect(SOURCE).not.toMatch(/Valon/i);
  });

  test("globals.css maps trail strokes to semantic tokens (not Registry palette)", () => {
    expect(GLOBALS).toContain("@keyframes brand-spinner-trail");
    expect(GLOBALS).toContain(".brand-spinner__track");
    expect(GLOBALS).toContain("stroke: var(--border)");
    expect(GLOBALS).toContain("stroke: var(--accent-strong)");
    expect(GLOBALS).not.toMatch(/--valon-neutral|--valon-gold/);
    expect(GLOBALS).not.toMatch(/valon-spinner/);
  });
});
