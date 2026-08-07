import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { resolveSegmentedControlNameProps } from "./segmented-control";

const DIR = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(DIR, "segmented-control.tsx"), "utf8");

describe("SegmentedControl accessible-name contract", () => {
  test("uses Registry label XOR labelledBy ownership", () => {
    expect(SOURCE).toContain("export type SegmentedControlNameProps");
    expect(SOURCE).toContain("labelledBy: string");
    expect(SOURCE).toContain("label?: never");
    expect(SOURCE).toContain("labelledBy?: never");
    expect(SOURCE).toContain("export function resolveSegmentedControlNameProps");
  });

  test("resolves labelledBy to aria-labelledby only", () => {
    expect(resolveSegmentedControlNameProps({ labelledBy: "theme-label" })).toEqual({
      "aria-labelledby": "theme-label",
    });
  });

  test("resolves label to aria-label only", () => {
    expect(resolveSegmentedControlNameProps({ label: "Theme" })).toEqual({
      "aria-label": "Theme",
    });
  });

  test("rejects blank names", () => {
    expect(() =>
      resolveSegmentedControlNameProps({ labelledBy: "   " }),
    ).toThrow("SegmentedControl requires an accessible name");
    expect(() => resolveSegmentedControlNameProps({ label: "" })).toThrow(
      "SegmentedControl requires an accessible name",
    );
  });
});

describe("SegmentedControl track variants contract", () => {
  test("owns default (borderless) and outline (bordered) VARIANT_STYLES", () => {
    expect(SOURCE).toContain("const VARIANT_STYLES");
    expect(SOURCE).toMatch(
      /default:\s*\n?\s*"bg-muted p-0\.5 forced-colors:border forced-colors:border-\[ButtonText\] forced-colors:p-px"/,
    );
    expect(SOURCE).toMatch(
      /outline:\s*"border border-border bg-muted p-px forced-colors:border-\[ButtonText\]"/,
    );
  });

  test("sliding pill uses an inset layout-stable hairline ring, not a drop shadow", () => {
    expect(SOURCE).toContain("shadow-[inset_0_0_0_1px_var(--border)]");
    expect(SOURCE).not.toContain("shadow-[0_0_0_1px_var(--border)]");
    expect(SOURCE).not.toMatch(
      /pointer-events-none absolute rounded-md bg-background shadow-sm/,
    );
    expect(SOURCE).not.toMatch(
      /pointer-events-none absolute rounded-md bg-background shadow-md/,
    );
  });

  test("forced-colors remaps pill chrome when box-shadow is discarded", () => {
    expect(SOURCE).toMatch(
      /forced-colors:border forced-colors:border-\[Highlight\]/,
    );
    expect(SOURCE).toContain("forced-colors:shadow-none");
  });

  test("enables pill transitions only after the first measured rect (not on mount)", () => {
    expect(SOURCE).not.toContain("React.useEffect(() => setAnimate(true), [])");
    expect(SOURCE).toContain("if (pill == null || animate) return");
    expect(SOURCE).toContain("setAnimate(true)");
  });

  test("exposes variant on the radiogroup and keeps focus preventScroll", () => {
    expect(SOURCE).toContain('data-variant={variant}');
    expect(SOURCE).toContain("variant?: SegmentedControlVariant");
    expect(SOURCE).toContain("focus({ preventScroll: true })");
  });

  test("track uses control-radius rounded-md (not rounded-lg)", () => {
    expect(SOURCE).toContain('"relative inline-flex rounded-md"');
    expect(SOURCE).not.toContain('"relative inline-flex rounded-lg"');
  });
});
