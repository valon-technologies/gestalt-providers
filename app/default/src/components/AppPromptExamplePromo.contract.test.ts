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

const INTEGRATIONS_QUERY = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "../lib/queries/integrations.ts"),
  "utf8",
);

describe("AppPromptExamplePromo", () => {
  test("uses semantic promo stage, stacked pills, and accessible feedback", () => {
    expect(SOURCE).toContain("bg-promo-stage");
    expect(SOURCE).toContain("createPromptCopyController");
    expect(SOURCE).toContain("data-testid=\"app-prompt-example\"");
    expect(SOURCE).toContain("data-testid={`app-prompt-card-${prompt.id}`}");
    expect(SOURCE).toContain("text-pretty");
    expect(SOURCE).toContain("role=\"status\"");
    expect(SOURCE).toContain("aria-live=\"polite\"");
    expect(SOURCE).toContain("prompts.map");
    expect(SOURCE).toContain("flex-col");
    expect(SOURCE).toContain("Copy example prompt:");
    expect(SOURCE).toContain("current.promptId !== promptId");
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

describe("integrations query prompt overrides", () => {
  test("applies DEV overrides in the query adapter, not the API transport", () => {
    expect(INTEGRATIONS_QUERY).toContain("import.meta.env.DEV");
    expect(INTEGRATIONS_QUERY).toContain("applyDevPromptOverrides");
    expect(INTEGRATIONS_QUERY).toContain("fetchIntegrationsForUi");
  });
});
