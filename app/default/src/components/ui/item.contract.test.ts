import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "item.tsx"),
  "utf8",
);

describe("Item", () => {
  test("ItemContent keeps min-w-0 for truncated titles beside actions", () => {
    expect(SOURCE).toContain(
      "flex min-w-0 flex-1 flex-col gap-1 [&+[data-slot=item-content]]:flex-none",
    );
  });

  test("ItemGroup exposes list role for application list rows", () => {
    expect(SOURCE).toContain('role="list"');
    expect(SOURCE).toContain('data-slot="item-group"');
  });
});
