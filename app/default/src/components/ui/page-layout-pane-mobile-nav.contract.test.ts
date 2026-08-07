import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const RAW = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "page-layout-pane-mobile-nav.tsx"),
  "utf8",
);

const SOURCE = RAW.replace(/^\s*\/\/.*$/gm, "").replace(/\/\*[\s\S]*?\*\//g, "");

describe("PageLayoutPaneMobileNav sheet contract", () => {
  test("opens navigation in a left Sheet", () => {
    expect(SOURCE).toContain('side="left"');
    expect(SOURCE).toContain("SheetContent");
    expect(SOURCE).toContain("SheetTrigger");
  });

  test("owns dismiss inside the titled header bar", () => {
    // Absolute Sheet close (top-4/right-4) is Dialog geometry; the titled bar
    // keeps one inset owner so top/bottom/right stay equal.
    expect(SOURCE).toContain("showCloseButton={false}");
    expect(SOURCE).toContain("SheetClose");
    expect(SOURCE).toContain("flex-row items-center justify-between");
    expect(SOURCE).toContain('aria-label="Close"');
    expect(SOURCE).toContain('size="icon-sm"');
  });

  test("exposes the current destination as visible label text", () => {
    expect(SOURCE).toContain("{label}");
    expect(SOURCE).toContain("truncate");
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
