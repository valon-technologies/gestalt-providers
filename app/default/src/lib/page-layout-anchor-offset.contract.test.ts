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
const DOCS_CONTENT = readFileSync(
  join(HERE, "../docs/DocsContent.tsx"),
  "utf8",
);
const DOCS_OPTION_SWITCHER = readFileSync(
  join(HERE, "../docs/docs-option-switcher.tsx"),
  "utf8",
);
const CATALOG = readFileSync(
  join(HERE, "../components/AppsCatalogPageClient.tsx"),
  "utf8",
);
const SETUP = readFileSync(
  join(HERE, "../pages/build.tsx"),
  "utf8",
);
const CONTENT_TOP = readFileSync(
  join(HERE, "page-layout-content-top.ts"),
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
    expect(CATALOG).toContain("pageLayoutRef");
    expect(CATALOG).toContain("usePageLayoutAnchorOffsetPx(");
  });

  test("docs hash scroll targets the switcher that owns the hash", () => {
    expect(DOCS_OPTION_SWITCHER).toContain("data-docs-hash-ids");
    expect(DOCS_OPTION_SWITCHER).toContain(
      'className="scroll-mt-[var(--page-layout-anchor-offset)]"',
    );
    expect(DOCS_SHELL).toContain("data-docs-hash-ids");
    expect(DOCS_SHELL).toContain("CSS.escape(id)");
    expect(DOCS_OPTION_SWITCHER).toContain("navigate({ to: pathname, hash: id, replace: true })");
    expect(DOCS_OPTION_SWITCHER).not.toContain("history.replaceState");
    expect(DOCS_CONTENT).toContain("from \"./docs-option-switcher\"");
    expect(DOCS_CONTENT).not.toContain("history.replaceState");
    expect(DOCS_SHELL).not.toContain("history.replaceState");
  });

  test("docs, apps, and setup share the same content top seam", () => {
    expect(CONTENT_TOP).toContain('PAGE_LAYOUT_CONTENT_TOP_GAP = "4rem"');
    expect(DOCS_SHELL).toContain("pageLayoutContentTopStyle");
    expect(DOCS_SHELL).toContain("<Container>");
    expect(CATALOG).toContain("pageLayoutContentTopStyle");
    expect(CATALOG).toContain("<Container>");
    expect(SETUP).toContain('<Container as="main">');
  });

  test("docs and setup share the same reading column measure", () => {
    expect(CONTENT_TOP).toContain("max-w-[65ch]");
    expect(DOCS_SHELL).toContain("PAGE_LAYOUT_READING_COLUMN_CLASS");
    expect(SETUP).toContain("PAGE_LAYOUT_READING_COLUMN_CLASS");
    expect(SETUP).not.toContain("max-w-5xl");
    expect(DOCS_SHELL).not.toContain('max-w-[65ch]"');
  });
});
