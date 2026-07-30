import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "page-header.tsx"),
  "utf8",
);

describe("PageHeader", () => {
  test("bakes Season into tiers via createHeaderChrome config", () => {
    expect(SOURCE).toContain("createHeaderChrome");
    expect(SOURCE).toContain('default: "font-display text-heading-lg tracking-heading"');
    expect(SOURCE).not.toContain("display:");
    expect(SOURCE).toContain(
      "[&:has([data-slot=page-header-content][data-size=lg])]:gap-y-3",
    );
  });
});
