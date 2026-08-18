import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "Callout.tsx"),
  "utf8",
);

describe("Callout", () => {
  test("reuses Alert wash without becoming a live region", () => {
    expect(SOURCE).toContain("alertSurfaceVariants");
    expect(SOURCE).toContain("alertLayoutVariants");
    expect(SOURCE).toContain('data-slot="callout"');
    expect(SOURCE).toContain('Omit<ComponentProps<"div">, "role">');
    expect(SOURCE).not.toContain('role="alert"');
    expect(SOURCE).not.toMatch(/import \{[^}]*\bAlert\b/);
  });
});
