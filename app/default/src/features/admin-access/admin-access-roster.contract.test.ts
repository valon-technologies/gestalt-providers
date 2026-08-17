import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "admin-access-roster.tsx"),
  "utf8",
);

describe("AccessEntryRow", () => {
  test("ties config-locked copy to the Remove access control", () => {
    expect(SOURCE).toContain(
      "aria-describedby={locked ? lockedHintId : undefined}",
    );
    expect(SOURCE).toContain("flex min-w-0 items-baseline justify-between gap-4");
    expect(SOURCE).toContain('size="xs"');
    expect(SOURCE).toContain("CircleAlert");
    expect(SOURCE).toContain("{LOCKED_FROM_CONFIG}");
    expect(SOURCE).toMatch(
      /items-baseline[\s\S]*REMOVE_ACCESS_LABEL[\s\S]*CircleAlert[\s\S]*LOCKED_FROM_CONFIG/,
    );
  });
});
