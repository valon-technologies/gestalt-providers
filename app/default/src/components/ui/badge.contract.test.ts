import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "badge.tsx"),
  "utf8",
);

describe("Badge paint contract", () => {
  test("base styles snap — no transition-colors on the chip (toolshed#4057)", () => {
    const baseMatch = SOURCE.match(
      /const badgeVariants = cva\(\s*(?:\/\/[^\n]*\n\s*)*"([^"]+)"/,
    );
    expect(baseMatch?.[1]).toBeTruthy();
    expect(baseMatch![1]).not.toContain("transition-colors");
  });

  test("status variants stay on --badge-* (not shell --success grove)", () => {
    expect(SOURCE).toContain("bg-badge-success text-badge-success-foreground");
    expect(SOURCE).toContain(
      "bg-badge-destructive text-badge-destructive-foreground",
    );
    expect(SOURCE).not.toMatch(/success:\s*"bg-success/);
  });
});
