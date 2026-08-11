import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "DocsAudienceCallout.tsx"),
  "utf8",
);

describe("DocsAudienceCallout", () => {
  test("uses banner layout so static admin orientation is not role=alert", () => {
    // Alert only asserts live regions for layout=default && variant!==outline.
    expect(SOURCE).toContain('layout="banner"');
    expect(SOURCE).toContain('variant="info"');
  });
});
