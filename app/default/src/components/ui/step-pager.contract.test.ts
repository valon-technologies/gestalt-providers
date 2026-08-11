import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(HERE, "step-pager.tsx"), "utf8");

describe("StepPager (toolshed#4190)", () => {
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
});
