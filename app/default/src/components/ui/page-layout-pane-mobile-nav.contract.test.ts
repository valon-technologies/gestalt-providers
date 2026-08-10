import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const dir = dirname(fileURLToPath(import.meta.url));
const RAW = readFileSync(join(dir, "page-layout-pane-mobile-nav.tsx"), "utf8");
const APP_TOP_BAR = readFileSync(join(dir, "app-top-bar.tsx"), "utf8");

const SOURCE = RAW.replace(/^\s*\/\/.*$/gm, "").replace(/\/\*[\s\S]*?\*\//g, "");

describe("PageLayoutPaneMobileNav disclosure contract", () => {
  test("uses a plain Menu button + dialog overlay, not Sheet or Collapsible", () => {
    expect(SOURCE).toContain('type="button"');
    expect(SOURCE).toContain("aria-expanded={open}");
    expect(SOURCE).toContain('aria-haspopup="dialog"');
    expect(SOURCE).toContain('role="dialog"');
    expect(SOURCE).toContain('aria-modal="true"');
    expect(SOURCE).not.toContain("Collapsible");
    expect(SOURCE).not.toContain("CollapsibleTrigger");
    expect(SOURCE).not.toContain("CollapsibleContent");
    expect(SOURCE).not.toContain("SheetContent");
    expect(SOURCE).not.toContain('side="left"');
    expect(SOURCE).not.toContain('role="region"');
  });

  test("defaults the bar label to Menu and places the caret before it", () => {
    expect(SOURCE).toContain('label = "Menu"');
    const caretIdx = SOURCE.indexOf("ChevronDown");
    const labelIdx = SOURCE.indexOf("{label}", caretIdx);
    expect(caretIdx).toBeGreaterThan(-1);
    expect(labelIdx).toBeGreaterThan(caretIdx);
    expect(SOURCE).toContain("disclosureCaretClassName");
  });

  test("keeps the Menu bar in flow (sticky owned by PageLayout)", () => {
    expect(SOURCE).toContain("ml-[calc(50%-50vw)]");
    expect(SOURCE).toContain(
      '"group focus-ring flex h-[length:var(--page-layout-mobile-nav-height,3rem)] w-full items-center border-b border-border bg-background text-left text-sm font-medium text-foreground"',
    );
    expect(SOURCE).not.toContain('"group fixed');
  });

  test("aligns the caret with AppTopBar via shared column variants", () => {
    expect(SOURCE).toContain("appTopBarColumnVariants");
    expect(SOURCE).toContain('from "@/components/ui/app-top-bar"');
    expect(APP_TOP_BAR).toContain("appTopBarColumnVariants");
    // Console column SoT — keep in sync with AppTopBarInner.
    expect(APP_TOP_BAR).toContain('cva("mx-auto w-full max-w-7xl px-6")');
  });

  test("open panel fills from the live Menu bar bottom and owns scroll", () => {
    expect(SOURCE).toContain("useOverlayTopPx");
    expect(SOURCE).toContain("getBoundingClientRect().bottom");
    expect(SOURCE).toContain("top: overlayTopPx");
    expect(SOURCE).not.toContain(
      "top-[calc(var(--page-layout-pane-top,0px)+var(--page-layout-mobile-nav-height,3rem))]",
    );
    // Same scroll-lock library Radix Dialog uses (html + iOS overscroll).
    expect(SOURCE).toContain('from "react-remove-scroll"');
    expect(SOURCE).toContain("RemoveScroll");
    expect(SOURCE).not.toContain("useDocumentScrollLock");
    expect(SOURCE).not.toContain('body.style.overflow = "hidden"');
    expect(SOURCE).toContain("h-full overflow-y-auto");
  });

  test("traps focus in the dialog and inerts PageLayout bands under the overlay", () => {
    expect(SOURCE).toContain("FocusScope");
    expect(SOURCE).toContain("trapped");
    expect(SOURCE).toContain("loop");
    expect(SOURCE).toContain("useOpenOverlayChrome");
    expect(SOURCE).toContain("onCloseRef");
    expect(SOURCE).toContain('event.key !== "Escape"');
    expect(SOURCE).toContain("PAGE_LAYOUT_INERT_SLOTS");
    expect(SOURCE).toContain('"page-layout-header"');
    expect(SOURCE).toContain('"page-layout-columns"');
    expect(SOURCE).toContain('"page-layout-footer"');
    expect(SOURCE).toContain('setAttribute("inert", "")');
    // Restore focus only when the Menu trigger still has a layout box — lg:hidden
    // auto-close must not focus a display:none button.
    expect(SOURCE).toContain(
      "if (trigger && trigger.getClientRects().length > 0)",
    );
    expect(SOURCE).toContain("trigger.focus()");
    expect(SOURCE).not.toContain("triggerRef.current?.focus()");
    expect(SOURCE).toContain('panelLabel = "Sections"');
    expect(SOURCE).toContain("aria-controls={open ? panelId : undefined}");
    expect(SOURCE).toContain("aria-labelledby={titleId}");
    // Effect deps must not include onClose — identity churn would steal focus.
    expect(SOURCE).toContain("}, [open, rootRef, triggerRef]);");
    // CSS lg:hidden cannot clear open side effects — close when chrome has no box.
    expect(SOURCE).toContain("useCloseOpenWhenHostHidden");
    expect(SOURCE).toContain("getClientRects().length === 0");
    expect(SOURCE).not.toContain("useCloseOpenAtLgBreakpoint");
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
