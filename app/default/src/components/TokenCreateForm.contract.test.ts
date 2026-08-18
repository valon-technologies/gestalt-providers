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

describe("TokenCreateForm actions row", () => {
  test("divider and submit row span the form width", () => {
    expect(SOURCE).toContain('cn("w-full space-y-6", actionsClassName)');
    expect(SOURCE).toContain(
      "flex w-full flex-row flex-nowrap items-center justify-end gap-3",
    );
  });
});
