import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(HERE, "table-status-indicator.tsx"), "utf8");

describe("TableStatusIndicator", () => {
  test("is a thin variant adapter over OutcomeStatusIndicator", () => {
    expect(SOURCE).toContain("OutcomeStatusIndicator");
    expect(SOURCE).toContain("tableVariantToOutcome");
    expect(SOURCE).toContain('default: "unknown"');
    expect(SOURCE).toContain('danger: "failure"');
    expect(SOURCE).toContain('info: "info"');
  });

  test("keeps table vocabulary on data-variant", () => {
    expect(SOURCE).toContain('data-slot="table-status-indicator"');
    expect(SOURCE).toContain("data-variant={resolved}");
    expect(SOURCE).toContain("iconOnly={iconOnly}");
    expect(SOURCE).toContain('size ?? (iconOnly ? "sm" : undefined)');
    expect(SOURCE).toContain("data-status={undefined}");
    expect(SOURCE).not.toContain("data-status={resolved}");
    expect(SOURCE).not.toContain("data-testid");
  });

  test("re-exports tableStatusIndicatorVariants as a compatibility alias", () => {
    expect(SOURCE).toContain("tableStatusIndicatorVariants");
    expect(SOURCE).toContain("outcomeStatusIndicatorVariants");
  });

  test("badge helper delegates without remapping failure in this file", () => {
    expect(SOURCE).toContain("tableStatusIndicatorBadgeVariant");
    expect(SOURCE).toContain("outcomeStatusIndicatorBadgeVariant");
    expect(SOURCE).not.toContain('return "destructive"');
  });

  test("preserves table default labels and empty-string label guard", () => {
    expect(SOURCE).toContain('default: "Normal"');
    expect(SOURCE).toContain('success: "Succeeded"');
    expect(SOURCE).toContain('danger: "Failed"');
    expect(SOURCE).toContain('warning: "Caution"');
    expect(SOURCE).toContain('info: "Information"');
    expect(SOURCE).toContain("label != null && label.length > 0");
    expect(SOURCE).toContain("customLabel ?? VARIANT_DEFAULT_LABEL[resolved]");
  });
});
