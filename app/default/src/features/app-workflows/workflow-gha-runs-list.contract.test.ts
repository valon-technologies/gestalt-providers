import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(HERE, "workflow-gha-runs-list.tsx"), "utf8");

describe("WorkflowGroupedDefinitionRunsList stacking", () => {
  test("sticky group header sits above isolated run rows", () => {
    expect(SOURCE).toContain(
      "sticky top-[calc(var(--page-layout-mobile-nav-top)+var(--page-layout-mobile-nav-height))]",
    );
    expect(SOURCE).toContain("lg:top-[var(--app-sticky-chrome-height)]");
    expect(SOURCE).toContain(
      "border-b border-transparent bg-background pt-6 pb-4",
    );
    expect(SOURCE).toContain('headerStuck && "border-border"');
    expect(SOURCE).not.toContain('className="space-y-8"');
    expect(SOURCE).not.toContain("pb-8");
    expect(SOURCE).toContain(
      'className="space-y-2 border-b border-border pb-6 pl-[calc(var(--size-control-sm)+0.25rem)]"',
    );
    expect(SOURCE).toContain("relative isolate z-0 rounded-none");
    expect(SOURCE).toContain('className="relative z-[2] shrink-0"');
  });
});
