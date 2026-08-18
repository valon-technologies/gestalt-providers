import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "DocsAudienceCallout.tsx"),
  "utf8",
);

describe("DocsAudienceCallout", () => {
  test("uses Callout for stacked standing help, not a live Alert", () => {
    expect(SOURCE).toContain('<Callout variant="info">');
    expect(SOURCE).not.toMatch(/<Alert[\s>/]/);
    expect(SOURCE).not.toContain("live={false}");
    expect(SOURCE).not.toContain("AlertIcon");
    expect(SOURCE).toContain("<AlertTitle>For admins</AlertTitle>");
  });
});
