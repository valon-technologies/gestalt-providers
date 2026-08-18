import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, test } from "vitest";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "AppsCatalogPageClient.tsx"),
  "utf8",
);

describe("AppsCatalogPageClient notices", () => {
  test("connected-success is an in-page Alert, not shell Banner", () => {
    const start = SOURCE.indexOf("connectedSuccessLabel ?");
    expect(start).toBeGreaterThan(-1);
    const next = SOURCE.indexOf("needsAttentionCopy ?", start + 1);
    const body = SOURCE.slice(start, next === -1 ? undefined : next);
    expect(body).toContain("<Alert");
    expect(body).toContain('variant="success"');
    expect(body).toContain('data-testid="apps-connected-toast"');
    expect(body).not.toContain('layout="banner"');
    expect(body).not.toContain("rounded-none");
    expect(body).not.toContain("border-b");
    expect(body).not.toMatch(/\bBanner\b/);
    expect(body).not.toMatch(/\bCallout\b/);
  });
});
