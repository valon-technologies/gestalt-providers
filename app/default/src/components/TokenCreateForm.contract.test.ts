import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "TokenCreateForm.tsx"),
  "utf8",
);

describe("TokenCreateForm plaintext reveal", () => {
  test("composes TokenPlaintextReveal instead of an input", () => {
    expect(SOURCE).toContain("<TokenPlaintextReveal");
    expect(SOURCE).not.toContain("InputGroup");
    expect(SOURCE).not.toContain("InputGroupInput");
  });
});
