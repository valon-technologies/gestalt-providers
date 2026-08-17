import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(HERE, "TokenCreateForm.tsx"), "utf8");

describe("TokenCreateForm plaintext reveal", () => {
  test("shows the one-time secret as CopyableCode, not a full-width input", () => {
    expect(SOURCE).toContain("<CopyableCode");
    expect(SOURCE).toContain('tooltip="Copy token"');
    expect(SOURCE).toContain('className="w-fit max-w-full"');
    expect(SOURCE).not.toContain("InputGroup");
    expect(SOURCE).not.toContain("InputGroupInput");
    expect(SOURCE).not.toContain('aria-label="API token"');
  });
});
