import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "card.tsx"),
  "utf8",
);

describe("Card", () => {
  test("canvas panel radius is rounded-lg (Alert / CodeFence / DescriptionList)", () => {
    expect(SOURCE).toContain(
      '"rounded-lg text-card-foreground [--radius-in-panel:var(--radius-nested)]"',
    );
    expect(SOURCE).toContain('data-slot="card"');
    expect(SOURCE).not.toContain("rounded-xl");
  });

  test("CardTitle uses medium, not synthesized semibold", () => {
    expect(SOURCE).toContain("font-medium leading-none tracking-tight");
    expect(SOURCE).not.toContain("font-semibold leading-none tracking-tight");
  });
});
