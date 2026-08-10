import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const RAW = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "page-layout.tsx"),
  "utf8",
);

// Comments name the very utilities these assertions forbid (`h-svh`, `fixed`, …).
const SOURCE = RAW.replace(/^\s*\/\/.*$/gm, "").replace(/\/\*[\s\S]*?\*\//g, "");

/**
 * PageLayout's whole reason to exist is that it is NOT an app shell. Adopting a
 * viewport-height shell here would re-home the scroll container and invalidate
 * every `sticky` offset and scroll-spy observer in a consuming app. `sidebar`
 * and `inspector-rail` are the components for that; this one must stay in flow.
 */
describe("PageLayout document-flow contract", () => {
  test("never claims the viewport height", () => {
    expect(SOURCE).not.toMatch(/\bh-svh\b/);
    expect(SOURCE).not.toMatch(/\bh-screen\b/);
    expect(SOURCE).not.toMatch(/\bmin-h-svh\b/);
  });

  test("never positions itself out of flow", () => {
    expect(SOURCE).not.toMatch(/\bfixed\b/);
    expect(SOURCE).not.toMatch(/\babsolute\b/);
  });

  test("does not own the page column", () => {
    // container.md: the registry ships no Container primitive and no width token —
    // the consuming app wraps this in its own `mx-auto w-full max-w-*`.
    expect(SOURCE).not.toMatch(/\bmax-w-(?!xl\b)[a-z0-9]/);
    expect(SOURCE).not.toMatch(/\bmx-auto\b/);
  });
});

/**
 * Two columns at `lg`, three at `xl`. Three tracks at `lg` leave
 * 1024 − 48 − 220 − 240 − 80 = 436px of prose, below a readable measure.
 */
describe("PageLayout breakpoint contract", () => {
  test("unlocks the pane at lg and the aside at xl via track width tokens", () => {
    expect(SOURCE).toContain("var(--page-layout-pane-width)");
    expect(SOURCE).toContain("var(--page-layout-aside-width)");
    expect(SOURCE).toContain(
      "lg:grid-cols-[var(--page-layout-pane-width)_minmax(0,1fr)]",
    );
    expect(SOURCE).toContain(
      "xl:grid-cols-[var(--page-layout-pane-width)_minmax(0,1fr)_var(--page-layout-aside-width)]",
    );
  });

  test("never puts a three-track template on lg", () => {
    expect(SOURCE).not.toMatch(
      /lg:grid-cols-\[var\(--page-layout-pane-width\)_minmax\(0,1fr\)_var\(--page-layout-aside-width\)\]/,
    );
  });
});

describe("PageLayout track width contract", () => {
  test("exposes semantic track tiers backed by CSS custom properties", () => {
    expect(SOURCE).toContain("pageLayoutTrackVariants");
    expect(SOURCE).toContain('tracks: {');
    expect(SOURCE).toContain('default: ""');
    expect(SOURCE).toContain(
      'compact: "[--page-layout-pane-width:11rem] [--page-layout-aside-width:11rem]"',
    );
    expect(SOURCE).toContain('data-tracks={tracks ?? "default"}');
  });
});

const GLOBALS_CSS = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "../../globals.css"),
  "utf8",
);

function extractCssTokenValue(css: string, token: string): string {
  const match = css.match(
    new RegExp(`${token.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}:\\s*([^;]+);`),
  );
  expect(match).not.toBeNull();
  return match![1].trim();
}

describe("PageLayout theme contract", () => {
  test("defines track width tokens in globals.css", () => {
    expect(extractCssTokenValue(GLOBALS_CSS, "--page-layout-pane-width")).toBe("13.75rem");
    expect(extractCssTokenValue(GLOBALS_CSS, "--page-layout-aside-width")).toBe("13.75rem");
  });

  test("derives Pane sticky top from measured chrome height plus a gap", () => {
    expect(GLOBALS_CSS).toContain("--app-sticky-chrome-height:");
    expect(GLOBALS_CSS).toContain("--page-layout-pane-gap:");
    expect(extractCssTokenValue(GLOBALS_CSS, "--page-layout-pane-gap")).toBe(
      "1.5rem",
    );
    expect(extractCssTokenValue(GLOBALS_CSS, "--page-layout-pane-top")).toContain(
      "var(--app-sticky-chrome-height)",
    );
    expect(extractCssTokenValue(GLOBALS_CSS, "--page-layout-pane-top")).toContain(
      "var(--page-layout-pane-gap)",
    );
  });

  test("docks the mobile Menu flush under chrome (no pane-gap)", () => {
    expect(GLOBALS_CSS).toContain("--page-layout-mobile-nav-top:");
    expect(extractCssTokenValue(GLOBALS_CSS, "--page-layout-mobile-nav-top")).toBe(
      "var(--app-sticky-chrome-height)",
    );
    expect(extractCssTokenValue(GLOBALS_CSS, "--page-layout-mobile-nav-top")).not.toContain(
      "page-layout-pane-gap",
    );
  });

  test("defines mobile Menu height and a shared anchor clearance token", () => {
    expect(extractCssTokenValue(GLOBALS_CSS, "--page-layout-mobile-nav-height")).toBe(
      "3rem",
    );
    expect(extractCssTokenValue(GLOBALS_CSS, "--page-layout-anchor-gap")).toBe(
      "0.75rem",
    );
    expect(GLOBALS_CSS).toContain("--page-layout-anchor-offset:");
    // Mobile clearance uses the flush Menu stack, not pane-top (which includes rail gap).
    expect(GLOBALS_CSS).toContain("var(--page-layout-mobile-nav-top)");
    expect(GLOBALS_CSS).toContain("var(--page-layout-mobile-nav-height)");
    // Desktop drops Menu height from the clearance stack.
    expect(GLOBALS_CSS).toMatch(
      /@media\s*\(min-width:\s*1024px\)[\s\S]*--page-layout-anchor-offset:\s*calc\(\s*var\(--page-layout-pane-top\)\s*\+\s*var\(--page-layout-anchor-gap\)/,
    );
  });
});

/**
 * The rails are plain divs. Wrapping them in `<aside>` mints an unlabelled
 * `complementary` landmark around a `<nav>` that already has a name — and these
 * pages carry two rails.
 */
describe("PageLayout landmark contract", () => {
  test("emits exactly one main and no aside elements", () => {
    expect(SOURCE.match(/<main\b/g)?.length).toBe(1);
    expect(SOURCE).not.toMatch(/<aside\b/);
  });

  test("mints no headings of its own", () => {
    expect(SOURCE).not.toMatch(/<h[1-6]\b/);
  });
});

/**
 * Sticky offsets read a token instead of a hardcoded step, so an app sets the
 * value once next to its nav-height token rather than per page.
 */
describe("PageLayout sticky contract", () => {
  test("reads the offset from a custom property", () => {
    expect(SOURCE).toContain("--page-layout-pane-top");
    expect(SOURCE).toContain("--page-layout-pane-bottom");
    expect(SOURCE).toContain("var(--page-layout-pane-top,0px)");
    expect(SOURCE).toContain("var(--page-layout-pane-bottom,0px)");
    expect(SOURCE).not.toMatch(/\btop-\d/);
  });

  test("sticks paneMobile inside a tall main that includes header + columns", () => {
    // Sticky fails if the parent is only as tall as the Menu bar. paneMobile +
    // header + columns share page-layout-main so sticky outlives the short bar,
    // Menu sits above PageHeader on small viewports, and rubber-bands with
    // sticky AppTopBar. Opaque bg + shared gap so SegmentedControl hosts stay
    // readable/spaced. Menu docks on mobile-nav-top (flush chrome), not pane-top.
    expect(SOURCE).toContain('data-slot="page-layout-main"');
    expect(SOURCE).toContain('data-slot="page-layout-pane-mobile"');
    expect(SOURCE).toContain(
      'className={cn("grid grid-cols-1", pageLayoutGapVariants[gap ?? "default"])}',
    );
    expect(SOURCE).toContain(
      "sticky top-[var(--page-layout-mobile-nav-top,var(--page-layout-pane-top,0px))] z-40 w-full min-w-0 bg-background lg:hidden",
    );
    // DOM order: paneMobile → header → columns (not header above paneMobile).
    const paneMobileIdx = SOURCE.indexOf('data-slot="page-layout-pane-mobile"');
    const headerIdx = SOURCE.indexOf(
      "{header ? <PageLayoutHeader>{header}</PageLayoutHeader> : null}",
      paneMobileIdx,
    );
    const columnsIdx = SOURCE.indexOf('data-slot="page-layout-columns"', headerIdx);
    expect(paneMobileIdx).toBeGreaterThan(-1);
    expect(headerIdx).toBeGreaterThan(paneMobileIdx);
    expect(columnsIdx).toBeGreaterThan(headerIdx);
    expect(SOURCE).not.toContain("overflow-x-auto p-1 lg:hidden");
  });

  test("leaves a spacing band for outward focus rings in scrollable rails", () => {
    expect(SOURCE).toContain("lg:overflow-y-auto");
    // Horizontal + bottom pad for focus rings; no top pad so rails share a top
    // edge with the content column (docs / catalog chrome alignment).
    expect(SOURCE).toContain("lg:px-1");
    expect(SOURCE).toContain("lg:pb-1");
    expect(SOURCE).toContain("lg:pt-0");
    expect(SOURCE).toContain("xl:overflow-y-auto");
    expect(SOURCE).toContain("xl:px-1");
    expect(SOURCE).toContain("xl:pb-1");
    expect(SOURCE).toContain("xl:pt-0");
  });

  test("shares the gap variant between the outer layout, main, and track grid", () => {
    expect(SOURCE).toContain("const pageLayoutGapVariants");
    expect(SOURCE).toContain("pageLayoutColumnsVariants({ gap })");
    expect(SOURCE).toContain("pageLayoutVariants({ gap })");
    expect(SOURCE).toContain('pageLayoutGapVariants[gap ?? "default"]');
  });
});

/**
 * Heading level is derived from whether a header was passed. A second source for
 * the same fact (a `headerPlacement` prop) is how the two drift apart.
 */
describe("PageLayout heading-level contract", () => {
  test("infers placement rather than taking it as a prop", () => {
    expect(SOURCE).toContain("usePageHeadingLevel");
    expect(SOURCE).not.toMatch(/headerPlacement\?:/);
  });
});

