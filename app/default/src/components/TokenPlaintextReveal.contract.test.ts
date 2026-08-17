import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(HERE, "TokenPlaintextReveal.tsx"), "utf8");

describe("TokenPlaintextReveal", () => {
  test("is a named, copy-only secret — not a text field or live alert", () => {
    expect(SOURCE).toContain('role="group"');
    expect(SOURCE).toContain('aria-label="API token"');
    expect(SOURCE).toContain("<CopyableCode");
    expect(SOURCE).toContain("<Alert live={false}>");
    expect(SOURCE).not.toContain("InputGroup");
    expect(SOURCE).not.toContain("role=\"textbox\"");
  });
});
