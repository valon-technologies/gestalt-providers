import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "page-header.tsx"),
  "utf8",
);

describe("PageHeader", () => {
  test("uses the display token for display tiers", () => {
    expect(SOURCE).toContain("createHeaderChrome");
    expect(SOURCE).toContain('title: "font-display text-heading-lg tracking-heading"');
    expect(SOURCE).toContain("PAGE_HEADER_TIERS");
    expect(SOURCE).toContain("createHeaderChromeScale");
    expect(SOURCE).toContain('title: "font-display text-display-sm tracking-display"');
    expect(SOURCE).not.toContain("Season");
    expect(SOURCE).not.toContain("display:");
    expect(SOURCE).toContain(
      "[&:has([data-slot=page-header-content][data-size=lg])]:gap-y-3",
    );
  });

  test("aligns between-mode actions to the content first baseline", () => {
    expect(SOURCE).toContain('alignBetweenItems: "sm:items-baseline"');
    expect(SOURCE).not.toContain('alignBetweenItems: "sm:items-end"');
    expect(SOURCE).not.toContain(
      'alignBetweenItems: "sm:[align-items:last_baseline]"',
    );
  });
});
