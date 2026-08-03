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
