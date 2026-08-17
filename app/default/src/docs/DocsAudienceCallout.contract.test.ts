import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "DocsAudienceCallout.tsx"),
  "utf8",
);

describe("DocsAudienceCallout", () => {
  test("keeps stacked layout and turns the live region off", () => {
    expect(SOURCE).toContain('<Alert variant="info" live={false}>');
    expect(SOURCE).not.toMatch(/<Alert[^>]*layout="banner"/);
    expect(SOURCE).toContain("<AlertIcon>");
    expect(SOURCE).toContain("<AlertTitle>For admins</AlertTitle>");
  });
});
