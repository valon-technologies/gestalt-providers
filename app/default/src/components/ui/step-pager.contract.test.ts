import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(HERE, "step-pager.tsx"), "utf8");

describe("StepPager", () => {
  test("imports cn from @/lib/cn (console vendor convention)", () => {
    expect(SOURCE).toContain('from "@/lib/cn"');
    expect(SOURCE).not.toContain('from "@/lib/utils"');
  });

  test("exports compound StepPager API", () => {
    expect(SOURCE).toContain("function StepPager");
    expect(SOURCE).toContain("function StepPagerPrevious");
    expect(SOURCE).toContain("function StepPagerNext");
    expect(SOURCE).toContain("function StepPagerStartSpacer");
    expect(SOURCE).toContain("function StepPagerLink");
    expect(SOURCE).toContain('data-slot="step-pager"');
  });

  test("composes Card solid/outline surfaces and ghost wash", () => {
    expect(SOURCE).toContain('cardVariants({ variant: "solid" })');
    expect(SOURCE).toContain('cardVariants({ variant: "outline" })');
    expect(SOURCE).toContain("ghost:");
  });

  test("pager hairline uses semantic border-border (not legacy border-alpha)", () => {
    expect(SOURCE).toContain("border-t border-border pt-6");
    expect(SOURCE).not.toContain("border-alpha");
  });

  test("destination cards pack to the title instead of stretching to max-w-xs", () => {
    expect(SOURCE).toContain("group flex h-fit w-fit max-w-full");
    expect(SOURCE).toContain("self-start");
    expect(SOURCE).toContain("flex flex-wrap items-start justify-between");
    expect(SOURCE).toContain("grid w-fit max-w-full");
    expect(SOURCE).toContain("grid-cols-[minmax(0,max-content)_auto]");
    expect(SOURCE).toContain("grid-cols-[auto_minmax(0,max-content)]");
    expect(SOURCE).not.toContain("max-w-xs");
    expect(SOURCE).not.toContain("items-stretch");
    expect(SOURCE).not.toContain("grid w-full items-start");
    expect(SOURCE).not.toContain("minmax(0,1fr)");
  });
});
