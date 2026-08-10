import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));

const HELPER = readFileSync(
  join(HERE, "page-layout-anchor-offset.ts"),
  "utf8",
);
const DOCS_SHELL = readFileSync(
  join(HERE, "../docs/DocsShell.tsx"),
  "utf8",
);
const CATALOG = readFileSync(
  join(HERE, "../components/AppsCatalogPageClient.tsx"),
  "utf8",
);

describe("page-layout anchor offset ownership", () => {
  test("exports a single probe + live hook for scroll-spy consumers", () => {
    expect(HELPER).toContain("export function readPageLayoutAnchorOffsetPx");
    expect(HELPER).toContain("export function usePageLayoutAnchorOffsetPx");
    expect(HELPER).toContain("height:var(--page-layout-anchor-offset)");
    expect(HELPER).toContain("[data-slot='app-sticky-chrome']");
  });

  test("docs and catalog scroll-spies share the live probe hook", () => {
    expect(DOCS_SHELL).toContain("usePageLayoutAnchorOffsetPx");
    expect(CATALOG).toContain("usePageLayoutAnchorOffsetPx");
    expect(CATALOG).not.toContain("CATALOG_TOC_ACTIVATION_OFFSET_MOBILE");
    expect(DOCS_SHELL).not.toContain("function readPageLayoutAnchorOffsetPx");
  });

  test("docs probes anchor offset from the chrome-var scope, not only :root", () => {
    expect(HELPER).toContain("scopeRef");
    expect(HELPER).toContain("scope ?? document.documentElement");
    expect(DOCS_SHELL).toContain("pageLayoutRef");
    expect(DOCS_SHELL).toContain("usePageLayoutAnchorOffsetPx(");
    expect(DOCS_SHELL).toContain("pageLayoutRef");
  });
});
