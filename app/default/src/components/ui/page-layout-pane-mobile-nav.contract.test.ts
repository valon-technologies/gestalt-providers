import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const RAW = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "page-layout-pane-mobile-nav.tsx"),
  "utf8",
);

const SOURCE = RAW.replace(/^\s*\/\/.*$/gm, "").replace(/\/\*[\s\S]*?\*\//g, "");

describe("PageLayoutPaneMobileNav disclosure contract", () => {
  test("uses a Collapsible trigger bar, not a side Sheet", () => {
    expect(SOURCE).toContain("Collapsible");
    expect(SOURCE).toContain("CollapsibleTrigger");
    expect(SOURCE).not.toContain("CollapsibleContent");
    expect(SOURCE).not.toContain("SheetContent");
    expect(SOURCE).not.toContain('side="left"');
  });

  test("defaults the bar label to Menu and places the caret before it", () => {
    expect(SOURCE).toContain('label = "Menu"');
    const triggerIdx = SOURCE.indexOf("CollapsibleTrigger");
    const caretIdx = SOURCE.indexOf("ChevronDown", triggerIdx);
    const labelIdx = SOURCE.indexOf("{label}", caretIdx);
    expect(caretIdx).toBeGreaterThan(-1);
    expect(labelIdx).toBeGreaterThan(caretIdx);
    expect(SOURCE).toContain("disclosureCaretClassName");
  });

  test("keeps the Menu bar in flow (sticky owned by PageLayout)", () => {
    // Bar itself must not be position:fixed — that skips iOS overscroll bounce
    // while sticky AppTopBar still bounces. Sticky lives on PageLayout's tall
    // pane-mobile wrapper.
    expect(SOURCE).toContain("ml-[calc(50%-50vw)]");
    expect(SOURCE).toContain(
      '"group flex h-[length:var(--page-layout-mobile-nav-height,3rem)] w-full items-center border-b border-border bg-background text-sm font-medium"',
    );
    expect(SOURCE).not.toContain('"group fixed');
    expect(SOURCE).not.toContain('className="h-12" aria-hidden');
  });

  test("aligns the caret with the AppTopBar content column", () => {
    expect(SOURCE).toContain("mx-auto w-full max-w-7xl px-6");
    expect(SOURCE).toContain("pageColumnClassName");
  });

  test("open panel fills the remaining viewport and owns scroll", () => {
    expect(SOURCE).toContain(
      "fixed inset-x-0 bottom-0 top-[calc(var(--page-layout-pane-top,0px)+var(--page-layout-mobile-nav-height,3rem))]",
    );
    expect(SOURCE).toContain("useDocumentScrollLock");
    expect(SOURCE).toContain('body.style.overflow = "hidden"');
    expect(SOURCE).toContain("h-full overflow-y-auto");
  });

  test("closes on Escape, inerts page columns, and restores trigger focus", () => {
    expect(SOURCE).toContain("useOpenOverlayChrome");
    expect(SOURCE).toContain('event.key !== "Escape"');
    expect(SOURCE).toContain('[data-slot="page-layout-columns"]');
    expect(SOURCE).toContain('setAttribute("inert", "")');
    expect(SOURCE).toContain("triggerRef.current?.focus()");
  });

  test("keeps the panel accessible name distinct from the Menu trigger", () => {
    expect(SOURCE).toContain('panelLabel = "Sections"');
    expect(SOURCE).toContain("aria-controls={open ? panelId : undefined}");
    expect(SOURCE).toContain('aria-modal="true"');
  });

  test("supports controlled and uncontrolled open state", () => {
    expect(SOURCE).toContain("open?: boolean");
    expect(SOURCE).toContain("onOpenChange?: (open: boolean) => void");
    expect(SOURCE).toContain("isControlled");
  });

  test("does not depend on a router", () => {
    expect(SOURCE).not.toMatch(/from ["']@tanstack\/react-router["']/);
    expect(SOURCE).not.toMatch(/from ["']next\/link["']/);
    expect(SOURCE).not.toMatch(/from ["']react-router/);
  });
});
