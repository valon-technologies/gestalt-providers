import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, test } from "vitest";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "AppPromptExamplePromo.tsx"),
  "utf8",
);

const OVERVIEW = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "../pages/app-workspace/overview.tsx"),
  "utf8",
);

describe("AppPromptExamplePromo", () => {
  test("uses semantic promo stage, stacked pills, and accessible feedback", () => {
    expect(SOURCE).toContain("bg-promo-stage");
    expect(SOURCE).toContain("createPromptCopyController");
    expect(SOURCE).toContain("data-testid=\"app-prompt-example\"");
    expect(SOURCE).toContain("data-testid=\"app-prompt-card\"");
    expect(SOURCE).toContain("text-pretty");
    expect(SOURCE).toContain("role=\"status\"");
    expect(SOURCE).toContain("aria-live=\"polite\"");
    expect(SOURCE).toContain("prompts.map");
    expect(SOURCE).toContain("flex-col");
    expect(SOURCE).toContain("aria-label={copyButtonLabel}");
    expect(SOURCE).not.toContain("ChevronRightIcon");
  });
});

describe("AppWorkspaceOverviewPage prompt wiring", () => {
  test("renders one promo with all prompts and places connect in PageHeaderActions", () => {
    expect(OVERVIEW).toContain("getAppPromptExamples(integration, surfaces.hasMcp)");
    expect(OVERVIEW).toContain("prompts={promptExamples}");
    expect(OVERVIEW).toContain("<AppPromptExamplePromo");
    expect(OVERVIEW).not.toContain("promptExamples.map");
    expect(OVERVIEW).toContain("<PageHeaderActions>");
    expect(OVERVIEW).toContain("{connectLabel}");
  });
});
