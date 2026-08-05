import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "hover-card.tsx"),
  "utf8",
);

describe("HoverCard remount contract", () => {
  test("documents controlled open remount hazard (toolshed#4057)", () => {
    expect(SOURCE).toContain("Controlled open + remount");
    expect(SOURCE).toContain("onOpenChange(false)");
  });

  test("opens instantly — Radix default openDelay is 700ms", () => {
    expect(SOURCE).toContain("openDelay = 0");
  });

  test("strips use client and uses local cn", () => {
    expect(SOURCE).not.toContain('"use client"');
    expect(SOURCE).toContain('@/lib/cn');
  });
});
