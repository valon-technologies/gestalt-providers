import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "overlap-callout.tsx"),
  "utf8",
);

describe("setup overlap callout", () => {
  test("standing overlap help is a Callout, not a live region", () => {
    expect(SOURCE).toContain(
      '<Callout variant="info" data-testid="setup-overlap-callout">',
    );
    expect(SOURCE).toContain("assistantOverlapBody(agentId)");
    expect(SOURCE).not.toContain("live={false}");
    expect(SOURCE).toContain('from "@/components/ui/alert"');
    expect(SOURCE).not.toContain("@/components/Callout");
  });
});
