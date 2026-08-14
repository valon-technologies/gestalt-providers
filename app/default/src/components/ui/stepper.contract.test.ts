import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(HERE, "stepper.tsx"), "utf8");
const SOURCE_RAIL = readFileSync(join(HERE, "step-rail.tsx"), "utf8");

describe("Stepper", () => {
  test("horizontal labels center under the indicator", () => {
    expect(SOURCE).toContain(
      'horizontal: "flex w-full min-w-0 flex-col text-center"',
    );
    expect(SOURCE).toContain('vertical: "flex-row text-left"');
    expect(SOURCE).not.toMatch(/rounded-md text-left outline-none/);
  });

  test("horizontal hit area fills the step column instead of hugging the label", () => {
    expect(SOURCE).toContain('horizontal: "flex-1 flex-col items-stretch"');
    expect(SOURCE).toContain("w-full min-w-0");
  });

  test("step chrome sits above the rail so connectors tuck under discs", () => {
    expect(SOURCE).toContain("relative z-10 inline-flex items-center gap-2");
    expect(SOURCE).not.toContain("relative z-0 inline-flex items-center gap-2");
    expect(SOURCE_RAIL).toContain("pointer-events-none absolute z-[1] overflow-hidden");
  });

  test("step titles use text-pretty for wrapped labels", () => {
    expect(SOURCE).toContain("text-pretty text-sm font-medium leading-snug");
    expect(SOURCE).not.toContain("text-pretty text-sm font-medium leading-none");
  });

  test("horizontal titles wrap inside the column instead of overflowing as max-content", () => {
    expect(SOURCE).toContain(
      "group-data-[orientation=horizontal]/step:block group-data-[orientation=horizontal]/step:w-full group-data-[orientation=horizontal]/step:min-w-0",
    );
    expect(SOURCE).toContain("inline-flex items-center gap-2");
    expect(SOURCE).not.toContain(
      'orientation === "horizontal" && "block w-full min-w-0"',
    );
  });

  test("completedChrome remaps rail tokens on the root, not per-indicator", () => {
    expect(SOURCE).toContain("completedChrome");
    expect(SOURCE).toContain("stepRailCompletedChromeAccentClassName");
    expect(SOURCE).toContain("stepRailCompletedChromeOutcomeClassName");
    expect(SOURCE).toContain('completedChrome = "outcome"');
    expect(SOURCE).toContain('completedChrome: "outcome"');
    expect(SOURCE).toContain("data-completed-chrome");
    expect(SOURCE).toContain("stepperVariants({ orientation, completedChrome })");
    expect(SOURCE).not.toContain("resolvedCompletedChrome");
    expect(SOURCE).not.toContain('from "@/components/ui/outcome-status-indicator"');
  });

  test("vertical connectors stagger skip-ahead as a tail and meet the next circle", () => {
    expect(SOURCE).toContain("stepRailLineStaggerSteps");
    expect(SOURCE).toContain("stepRailAdvanceTransition");
    expect(SOURCE).toContain("stepRailFromActiveIndex");
    expect(SOURCE).not.toContain("prevActiveIndexRef");
    expect(SOURCE).toContain("staggerSteps={staggerSteps}");
    expect(SOURCE_RAIL).toContain("-bottom-[var(--step-rail-trigger-pad,0px)]");
    expect(SOURCE_RAIL).toContain("[clip-path:inset(0_0_100%_0)]");
  });

  test("indicator chrome delay is sampled at the state transition, not on callback identity", () => {
    expect(SOURCE).toContain("resolveChromeDelayMs");
    expect(SOURCE).not.toContain(
      "getChromeDelayMs={() => getChromeDelayMs(index, dataState)}",
    );
    expect(SOURCE_RAIL).toContain("resolveDelayRef");
    expect(SOURCE_RAIL).not.toContain("[animate, isCompleted, resolveDelay]");
    expect(SOURCE_RAIL).not.toContain("[animate, dataState, resolveDelay]");
  });
});
