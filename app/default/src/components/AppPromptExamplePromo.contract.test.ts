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
  test("uses semantic promo stage, copy controller, and accessible feedback", () => {
    expect(SOURCE).toContain("bg-promo-stage");
    expect(SOURCE).toContain("createPromptCopyController");
    expect(SOURCE).toContain("data-testid=\"app-prompt-example\"");
    expect(SOURCE).toContain("data-testid=\"app-prompt-card\"");
    expect(SOURCE).toContain("text-pretty");
    expect(SOURCE).toContain("role=\"status\"");
    expect(SOURCE).toContain("aria-live=\"polite\"");
    expect(SOURCE).toContain("aria-label={copyButtonLabel}");
    expect(SOURCE).not.toContain("ChevronRightIcon");
  });
});

describe("AppWorkspaceOverviewPage prompt wiring", () => {
  test("renders promo from getAppPromptExample and places connect in PageHeaderActions", () => {
    expect(OVERVIEW).toContain("getAppPromptExample(integration, surfaces.hasMcp)");
    expect(OVERVIEW).toContain("<AppPromptExamplePromo");
    expect(OVERVIEW).toContain("<PageHeaderActions>");
    expect(OVERVIEW).toContain("{connectLabel}");
  });
});
