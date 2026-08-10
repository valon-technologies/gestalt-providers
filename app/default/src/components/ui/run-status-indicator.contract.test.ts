import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  runStatusIndicatorBadgeVariant,
  runStatusToOutcome,
} from "./run-status-indicator";

const HERE = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(HERE, "run-status-indicator.tsx"), "utf8");

describe("RunStatusIndicator adapter", () => {
  test("is a thin vocabulary adapter over OutcomeStatusIndicator", () => {
    expect(SOURCE).toContain(
      'from "@/components/ui/outcome-status-indicator"',
    );
    expect(SOURCE).toContain("OutcomeStatusIndicator");
    expect(SOURCE).toContain("runStatusToOutcome");
    expect(SOURCE).not.toContain("bg-status-indicator-success");
  });

  test("maps run vocabulary onto domain-neutral outcomes", () => {
    expect(runStatusToOutcome("succeeded")).toBe("success");
    expect(runStatusToOutcome("failed")).toBe("failure");
    expect(runStatusToOutcome("running")).toBe("in_progress");
    expect(runStatusToOutcome("pending")).toBe("pending");
    expect(runStatusToOutcome("canceled")).toBe("canceled");
    expect(runStatusToOutcome("skipped")).toBe("skipped");
    expect(runStatusToOutcome("unknown")).toBe("unknown");
  });

  test("keeps run vocabulary on data-status (overrides outcome data-status)", () => {
    expect(SOURCE).toContain('data-slot="run-status-indicator"');
    expect(SOURCE).toContain("data-status={resolved}");
    expect(SOURCE).not.toContain("data-run-status");
  });

  test("re-exports runStatusIndicatorVariants as a compatibility alias", () => {
    expect(SOURCE).toContain("runStatusIndicatorVariants");
    expect(SOURCE).toContain("outcomeStatusIndicatorVariants");
  });

  test("maps run statuses to Badge variants via outcomes", () => {
    expect(runStatusIndicatorBadgeVariant("succeeded")).toBe("success");
    expect(runStatusIndicatorBadgeVariant("failed")).toBe("destructive");
    expect(runStatusIndicatorBadgeVariant("running")).toBe("warning");
    expect(runStatusIndicatorBadgeVariant("unknown")).toBe("muted");
  });

  test("preserves run-domain default labels and empty-string label guard", () => {
    expect(SOURCE).toContain('succeeded: "Succeeded"');
    expect(SOURCE).toContain('running: "Running"');
    expect(SOURCE).toContain("label != null && label.length > 0");
    expect(SOURCE).toContain("customLabel ?? RUN_DEFAULT_LABEL[resolved]");
  });
});
