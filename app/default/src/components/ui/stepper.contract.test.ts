import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "stepper.tsx"),
  "utf8",
);

describe("Stepper", () => {
  test("centers horizontal step labels under the indicator", () => {
    expect(SOURCE).toContain(
      'horizontal: "flex w-full min-w-0 flex-col text-center"',
    );
    expect(SOURCE).toContain(
      'vertical: "row-start-1 w-max max-w-full min-w-0 justify-self-start flex-row text-left"',
    );
    expect(SOURCE).not.toMatch(
      /p-\[var\(--stepper-trigger-pad\)\] text-left outline-none/,
    );
  });

  test("horizontal hit area fills the step column instead of hugging the label", () => {
    expect(SOURCE).toContain('horizontal: "flex-1 flex-col items-stretch"');
    expect(SOURCE).toContain("w-full min-w-0");
  });

  test("step titles scale with stepper size", () => {
    expect(SOURCE).toContain("min-w-0 text-pretty font-medium leading-none tracking-tight");
    expect(SOURCE).toContain('size === "sm" ? "text-xs" : "text-sm"');
  });

  test("sm size tightens vertical step rhythm", () => {
    expect(SOURCE).toContain('vertical: "grid grid-cols-1"');
    expect(SOURCE).toContain("row-start-2");
    expect(SOURCE).toContain('class: "h-4"');
    expect(SOURCE).toContain('class: "h-8"');
    expect(SOURCE).not.toContain(
      "top-[calc(var(--stepper-trigger-pad,0.375rem)+var(--stepper-indicator-size,2rem))]",
    );
  });

  test("completedVariant success paints checks, not the connector edge", () => {
    expect(SOURCE).toContain("completedVariant?: StepperCompletedVariant");
    expect(SOURCE).toContain(
      "border-success-solid bg-success-solid text-success-solid-foreground",
    );
    expect(SOURCE).toContain('accent: "bg-accent-solid"');
    expect(SOURCE).not.toContain('success: "bg-success-solid"');
  });

  test("connectorVariant primary paints completed rails in ink", () => {
    expect(SOURCE).toContain("connectorVariant?: StepperConnectorVariant");
    expect(SOURCE).toContain('primary: "bg-primary"');
    expect(SOURCE).toContain("data-connector-variant={connectorVariant}");
  });
});
