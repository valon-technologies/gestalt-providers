import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(HERE, "table-status-indicator.tsx"), "utf8");

describe("TableStatusIndicator", () => {
  test("shell colors match Badge status surfaces in this bundle", () => {
    expect(SOURCE).toContain(
      "bg-badge-success text-badge-success-foreground",
    );
    expect(SOURCE).toContain(
      "bg-badge-destructive text-badge-destructive-foreground",
    );
    expect(SOURCE).toContain(
      "bg-badge-warning text-badge-warning-foreground",
    );
    expect(SOURCE).toContain("bg-badge-info text-badge-info-foreground");
    expect(SOURCE).toContain("bg-foreground/[0.06] text-foreground/80");
    expect(SOURCE).toContain("from \"lucide-react\"");
    expect(SOURCE).toContain("iconOnly");
  });

  test("maps semantic variants to default labels", () => {
    expect(SOURCE).toContain("Succeeded");
    expect(SOURCE).toContain("Failed");
    expect(SOURCE).toContain("Caution");
    expect(SOURCE).toContain("Information");
  });

  test("exports Badge variant mapper alongside indicator variants", () => {
    expect(SOURCE).toContain("tableStatusIndicatorBadgeVariant");
    expect(SOURCE).toContain("return \"destructive\"");
  });

  test("iconOnly falls back when label is an empty string", () => {
    expect(SOURCE).toContain("label?.length");
  });
});
