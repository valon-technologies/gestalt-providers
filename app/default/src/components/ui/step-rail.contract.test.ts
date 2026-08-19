import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  stepRailIndicatorArriveStaggerSteps,
  stepRailLineStaggerSteps,
  stepRailAdvanceTransition,
  stepRailFromActiveIndex,
  stepRailResolvedGlyph,
} from "@/components/ui/step-rail";
import {
  timelineConnectorLineState,
  timelineItemStatusToRailState,
} from "@/components/ui/timeline-steps-status";

const HERE = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(HERE, "step-rail.tsx"), "utf8");
const TIMELINE_SOURCE = readFileSync(join(HERE, "timeline-steps.tsx"), "utf8");
const TIMELINE_STATUS_SOURCE = readFileSync(
  join(HERE, "timeline-steps-status.ts"),
  "utf8",
);

describe("timeline status mappers (owned by TimelineSteps)", () => {
  test("maps progress and semantic statuses to rail paint states", () => {
    expect(timelineItemStatusToRailState("completed")).toBe("completed");
    expect(timelineItemStatusToRailState("current")).toBe("active");
    expect(timelineItemStatusToRailState("upcoming")).toBe("pending");
    expect(timelineItemStatusToRailState("success")).toBe("success");
    expect(timelineItemStatusToRailState("warning")).toBe("warning");
    expect(timelineItemStatusToRailState("error")).toBe("error");
    expect(timelineItemStatusToRailState("danger")).toBe("error");
    expect(timelineItemStatusToRailState(null)).toBe("pending");
  });

  test("only completed and success advance the connector fill", () => {
    expect(timelineConnectorLineState("completed")).toBe("completed");
    expect(timelineConnectorLineState("success")).toBe("completed");
    expect(timelineConnectorLineState("warning")).toBe("pending");
    expect(timelineConnectorLineState("error")).toBe("pending");
    expect(timelineConnectorLineState("danger")).toBe("pending");
    expect(timelineConnectorLineState("current")).toBe("pending");
  });

  test("xs coerces glyph auto to none", () => {
    expect(stepRailResolvedGlyph("xs", "auto")).toBe("none");
    expect(stepRailResolvedGlyph("sm", "auto")).toBe("auto");
    expect(stepRailResolvedGlyph("default", "none")).toBe("none");
  });
});

describe("step-rail line motion", () => {
  test("vertical skip-ahead fills origin to destination as a tail", () => {
    expect(stepRailLineStaggerSteps("vertical", 0, 0, 2)).toBe(0);
    expect(stepRailLineStaggerSteps("vertical", 1, 0, 2)).toBe(1);
    expect(stepRailLineStaggerSteps("vertical", 2, 0, 2)).toBe(0);
  });

  test("vertical rewind retracts from the tip", () => {
    expect(stepRailLineStaggerSteps("vertical", 1, 2, 0)).toBe(0);
    expect(stepRailLineStaggerSteps("vertical", 0, 2, 0)).toBe(1);
  });

  test("horizontal skip-ahead fills destination-owned segments as a tail", () => {
    expect(stepRailLineStaggerSteps("horizontal", 1, 0, 2)).toBe(0);
    expect(stepRailLineStaggerSteps("horizontal", 2, 0, 2)).toBe(1);
    expect(stepRailLineStaggerSteps("horizontal", 0, 0, 2)).toBe(0);
  });

  test("horizontal rewind retracts from the tip", () => {
    expect(stepRailLineStaggerSteps("horizontal", 2, 2, 0)).toBe(0);
    expect(stepRailLineStaggerSteps("horizontal", 1, 2, 0)).toBe(1);
    expect(stepRailLineStaggerSteps("horizontal", 0, 2, 0)).toBe(0);
  });

  test("indicator chrome waits for the arriving segment", () => {
    expect(
      stepRailIndicatorArriveStaggerSteps("vertical", 0, 0, 2, "completed"),
    ).toBe(0);
    expect(
      stepRailIndicatorArriveStaggerSteps("vertical", 1, 0, 2, "completed"),
    ).toBe(1);
    expect(
      stepRailIndicatorArriveStaggerSteps("vertical", 2, 0, 2, "active"),
    ).toBe(1);
  });

  test("jump origin stays latched until the next active index change", () => {
    let transition = {
      origin: null as number | null,
      current: null as number | null,
    };
    transition = stepRailAdvanceTransition(0, transition);
    expect(stepRailFromActiveIndex(transition)).toBe(0);
    transition = stepRailAdvanceTransition(0, transition);
    expect(stepRailFromActiveIndex(transition)).toBe(0);
    transition = stepRailAdvanceTransition(2, transition);
    expect(stepRailFromActiveIndex(transition)).toBe(0);
    expect(
      stepRailLineStaggerSteps(
        "vertical",
        1,
        stepRailFromActiveIndex(transition),
        2,
      ),
    ).toBe(1);
    transition = stepRailAdvanceTransition(2, transition);
    expect(stepRailFromActiveIndex(transition)).toBe(0);
    expect(
      stepRailLineStaggerSteps(
        "vertical",
        1,
        stepRailFromActiveIndex(transition),
        2,
      ),
    ).toBe(1);
    transition = stepRailAdvanceTransition(3, transition);
    expect(stepRailFromActiveIndex(transition)).toBe(2);
    transition = stepRailAdvanceTransition(0, transition);
    expect(stepRailFromActiveIndex(transition)).toBe(3);
  });
});

describe("step-rail timeline semantics", () => {
  test("timeline domain status type lives on TimelineSteps, not shared step-rail", () => {
    expect(TIMELINE_STATUS_SOURCE).toContain("export type TimelineItemStatus");
    expect(TIMELINE_STATUS_SOURCE).toContain(
      "export function timelineItemStatusToRailState",
    );
    expect(TIMELINE_STATUS_SOURCE).toContain(
      "export function timelineConnectorLineState",
    );
    expect(TIMELINE_SOURCE).toContain("timeline-steps-status");
    expect(SOURCE).not.toContain("TimelineItemStatus");
    expect(SOURCE).not.toContain("timelineItemStatusToRailState");
    expect(SOURCE).not.toContain("timelineConnectorLineState");
  });

  test("timeline connectors use ink-alpha tokens, not accent fill", () => {
    expect(SOURCE).toContain("stepRailTimelineRootClassName");
    expect(SOURCE).toContain("--step-rail-timeline-track");
    expect(SOURCE).toContain("--step-rail-timeline-fill");
    expect(SOURCE).toContain(
      "--step-rail-timeline-track:var(--step-rail-pending)",
    );
    expect(SOURCE).toContain(
      "--step-rail-timeline-fill:var(--muted-foreground)",
    );
    expect(SOURCE).toContain("bg-[var(--step-rail-timeline-track)]");
    expect(SOURCE).toContain("bg-[var(--step-rail-timeline-fill)]");
    expect(SOURCE).toContain('layout: "timeline"');
    expect(SOURCE).not.toMatch(/timeline:\s*"bg-accent-solid"/);
    expect(SOURCE).not.toContain("disabled-foreground");
  });

  test("semantic paint uses role solid tokens, not bare ramp steps or destructive", () => {
    expect(SOURCE).toContain(
      "border-success-solid bg-success-solid text-success-solid-foreground",
    );
    expect(SOURCE).toContain(
      "border-warning-solid bg-warning-solid text-warning-solid-foreground",
    );
    expect(SOURCE).toContain(
      "border-error-solid bg-error-solid text-error-solid-foreground",
    );
    expect(SOURCE).not.toContain("border-green-400 bg-green-400");
    expect(SOURCE).not.toContain("border-yellow-400 bg-yellow-400");
    expect(SOURCE).not.toContain("border-red-400 bg-red-400");
    expect(SOURCE).not.toContain(
      "border-destructive bg-destructive text-destructive-foreground",
    );
  });

  test("semantic failure paint state is error, not danger", () => {
    expect(SOURCE).toContain(
      'StepRailSemanticState = "success" | "warning" | "error"',
    );
    expect(SOURCE).toContain('state: "error"');
    expect(SOURCE).not.toMatch(/state:\s*"danger"/);
    expect(TIMELINE_STATUS_SOURCE).toContain('"error"');
    expect(TIMELINE_STATUS_SOURCE).toContain('case "danger":');
  });

  test("progress completion check is not conflated with semantic success", () => {
    expect(SOURCE).toContain('const isCompleted = dataState === "completed"');
    expect(SOURCE).not.toContain('dataState === "success"');
  });

  test("completed chrome is tokenized on the rail root", () => {
    expect(SOURCE).toContain(
      "--step-rail-completed-border:var(--accent-solid)",
    );
    expect(SOURCE).toContain("--step-rail-completed-fill:var(--accent-solid)");
    expect(SOURCE).toContain(
      "--step-rail-completed-fg:var(--accent-foreground)",
    );
    expect(SOURCE).toContain("--step-rail-completed-line:var(--accent-solid)");
    expect(SOURCE).toContain("stepRailCompletedChromeAccentClassName");
    expect(SOURCE).toContain("stepRailCompletedChromeOutcomeClassName");
    expect(SOURCE).toContain(
      "--step-rail-completed-border:var(--status-indicator-success)",
    );
    expect(SOURCE).toContain(
      "--step-rail-completed-fill:var(--status-indicator-success)",
    );
    expect(SOURCE).toContain("[--step-rail-completed-fg:white]");
    expect(SOURCE).toContain("--step-rail-completed-line:var(--primary)");
    expect(SOURCE).not.toContain("var(--color-green-500)");
    expect(SOURCE).toContain("bg-[var(--step-rail-completed-fill)]");
    expect(SOURCE).toContain("bg-[var(--step-rail-completed-line)]");
    expect(SOURCE).not.toContain(
      'from "@/components/ui/outcome-status-indicator"',
    );
    expect(SOURCE).not.toContain('from "lucide-react"');
    expect(SOURCE).not.toMatch(
      /completed:\s*"border-accent-solid bg-accent-solid text-accent-foreground"/,
    );
    const rootMatch = SOURCE.match(
      /export const stepRailRootClassName = \[([\s\S]*?)\]\.join/,
    );
    expect(rootMatch?.[1]).toBeTruthy();
    expect(rootMatch?.[1]).not.toContain("completed");
    expect(SOURCE).not.toContain("readStepRailChromeDelayMs");
    expect(TIMELINE_SOURCE).toContain("stepRailCompletedChromeAccentClassName");
    expect(TIMELINE_SOURCE).toContain("stepRailCompletedChromeOutcomeClassName");
    expect(TIMELINE_SOURCE).toContain('completedChrome = "accent"');
  });

  test("vertical fill grows downward and stepper stubs meet the next circle", () => {
    expect(SOURCE).toContain(
      'vertical: "[clip-path:inset(0_0_100%_0)] data-[state=completed]:[clip-path:inset(0_0_0_0)]"',
    );
    expect(SOURCE).not.toContain("[clip-path:inset(100%_0_0_0)]");
    expect(SOURCE).toContain("-bottom-[var(--step-rail-trigger-pad,0px)]");
    expect(SOURCE).toContain("--step-rail-line-stagger");
    expect(SOURCE).toContain("stepRailLineStaggerSteps");
  });

  test("TimelineSteps root owns timeline connector tokens", () => {
    expect(TIMELINE_SOURCE).toContain("stepRailTimelineRootClassName");
  });

  test("dot rail geometry scales with size independently of glyph circles", () => {
    expect(SOURCE).toContain("stepRailDotLaneVar");
    expect(SOURCE).toContain("stepRailIndicatorLaneVar");
    expect(SOURCE).toContain('glyph: "none"');
    expect(SOURCE).toContain('size: "default"');
    expect(SOURCE).toContain(
      'class: "size-4 [--step-rail-indicator-size:1rem]"',
    );
  });

  test("dot rail semantic paint uses role solid fills at every size", () => {
    expect(SOURCE).toContain('state: "success"');
    expect(SOURCE).toContain(
      "border-success-solid bg-success-solid text-success-solid-foreground",
    );
  });

  test("xs size coerces glyph auto to dot-only rail", () => {
    expect(SOURCE).toContain("stepRailResolvedGlyph");
    expect(SOURCE).toContain('return size === "xs" ? "none" : glyph');
  });

  test("xs indicators cap custom SVG on the live none/xs paint path", () => {
    expect(SOURCE).toMatch(
      /glyph:\s*"none",\s*size:\s*"xs",\s*class:\s*"[^"]*\[&>svg\]:size-2/,
    );
    expect(SOURCE).not.toMatch(
      /glyph:\s*"none",\s*size:\s*"xs",\s*class:.*"\[&>svg\]:size-3\.5"/s,
    );
  });

  test("rail stacks above a Neutral plate and under the indicator", () => {
    expect(SOURCE).toContain(
      '"relative z-20 flex shrink-0 items-center justify-center rounded-full border leading-none',
    );
    expect(SOURCE).toContain('"pointer-events-none absolute z-10 overflow-hidden"');
    expect(SOURCE).not.toContain("z-[1]");
    expect(SOURCE).not.toContain('"relative z-10 flex shrink-0 items-center justify-center rounded-full');
  });

  test("line fill and indicator chrome honor prefers-reduced-motion", () => {
    expect(SOURCE).toContain(
      "motion-reduce:transition-none motion-reduce:delay-0",
    );
    expect(SOURCE).toContain(
      'window.matchMedia("(prefers-reduced-motion: reduce)")',
    );
  });
});
