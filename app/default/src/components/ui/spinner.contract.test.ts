import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(HERE, "spinner.tsx"), "utf8");
const GLOBALS = readFileSync(join(HERE, "../../globals.css"), "utf8");

describe("Spinner", () => {
  test("is a routine busy span on spinner-trail (not Gestalt-mark SVG)", () => {
    expect(SOURCE).toContain('data-slot="spinner"');
    expect(SOURCE).toContain("spinner-trail");
    expect(SOURCE).toContain('ComponentProps<"span">');
    expect(SOURCE).not.toContain("brand-spinner__trail");
    expect(SOURCE).not.toContain("<svg");
    expect(SOURCE).not.toMatch(/valon-spinner/);
  });

  test("imports cn from @/lib/cn (console vendor convention)", () => {
    expect(SOURCE).toContain('from "@/lib/cn"');
    expect(SOURCE).not.toContain('from "@/lib/utils"');
  });

  test("globals.css ships fade-trail ring motion", () => {
    expect(GLOBALS).toContain("@keyframes spinner-trail-spin");
    expect(GLOBALS).toContain(".spinner-trail");
    expect(GLOBALS).toContain("conic-gradient");
    expect(GLOBALS).not.toMatch(/valon-spinner/);
  });
});
