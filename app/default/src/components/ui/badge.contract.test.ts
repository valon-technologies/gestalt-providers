import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "badge.tsx"),
  "utf8",
);

describe("Badge paint contract", () => {
  test("base styles snap — no color-transition utilities (toolshed#4057 / #4081)", () => {
    const baseMatch = SOURCE.match(
      /const badgeVariants = cva\(\s*(?:\/\/[^\n]*\n\s*)*"([^"]+)"/,
    );
    expect(baseMatch?.[1]).toBeTruthy();
    expect(baseMatch![1]).not.toContain("transition-colors");
    expect(baseMatch![1]).not.toContain("transition-[");
  });

  test("size owns type / pad / icon ladder", () => {
    expect(SOURCE).toContain(
      'sm: "gap-0.5 px-1 py-px text-2xs leading-none [&>svg]:size-2.5"',
    );
    expect(SOURCE).toContain(
      'default: "gap-1 px-1.5 py-0.5 text-xs leading-none [&>svg]:size-3"',
    );
    expect(SOURCE).toContain(
      'lg: "gap-1.5 px-2 py-1 text-sm leading-none [&>svg]:size-3.5"',
    );
  });

  test("ghost shares press-feedback quiet chrome; muted climbs neutral-dark", () => {
    expect(SOURCE).toContain("ghostQuietChromeClassName");
    expect(SOURCE).toContain("hover:bg-neutral-dark-hover");
    expect(SOURCE).not.toContain("hover:bg-muted/80");
    expect(SOURCE).not.toContain("hover:bg-accent");
  });

  test("status variants stay on --badge-* (not shell --success grove)", () => {
    expect(SOURCE).toContain("bg-badge-success text-badge-success-foreground");
    expect(SOURCE).toContain(
      "bg-badge-destructive text-badge-destructive-foreground",
    );
    expect(SOURCE).not.toMatch(/success:\s*"bg-success/);
  });
});
