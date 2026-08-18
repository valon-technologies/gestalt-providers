import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "assistant-install.tsx"),
  "utf8",
);

describe("assistant install guidance", () => {
  test("persistent token and overlap callouts are not live regions", () => {
    expect(SOURCE).toContain(
      '<Alert variant="info" live={false} data-testid="build-install-token-needed">',
    );
    expect(SOURCE).toContain(
      '<Alert variant="info" live={false} data-testid="setup-overlap-callout">',
    );
  });

  test("install recipes require a session-minted token", () => {
    expect(SOURCE).not.toContain("gst_api_YOUR_TOKEN");
    expect(SOURCE).toContain("{hasMcpCredential && agent === \"cursor\" ? (");
    expect(SOURCE).toContain("{hasMcpCredential && agent === \"chatgpt\" ? (");
  });
});
