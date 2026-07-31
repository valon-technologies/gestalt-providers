import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const RAW = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "nav-list.tsx"),
  "utf8",
);

// Explanatory `//` comments quote the very patterns these assertions look for
// (`data-selected="false"`, `role="menu"`, …) — strip them so the contract tests
// the code, not the prose explaining it.
const SOURCE = RAW.replace(/^\s*\/\/.*$/gm, "");

/**
 * The single most fragile detail in this component. React serializes booleans
 * onto `data-*`, so `data-selected={active}` renders `data-selected="false"` on
 * every idle item — which still matches the `[data-selected]` presence selector
 * the shared role ladder keys off (selectable-rows.md), painting the whole rail
 * as selected. The component must emit the attribute only when active.
 */
describe("NavList selection contract", () => {
  test("marks selection by presence, never by a boolean value", () => {
    expect(SOURCE).toContain(`data-selected={active ? "" : undefined}`);
    expect(SOURCE).not.toMatch(/data-selected=\{active\}/);
    expect(SOURCE).not.toMatch(/data-selected=\{[^}]*\?\s*"true"/);
  });

  test("derives aria-current from the same `active` prop", () => {
    expect(SOURCE).toContain(`aria-current={active ? current : undefined}`);
    expect(SOURCE).toContain('current?: "page" | "location"');
    expect(SOURCE).toContain('current = "page"');
  });

  test("owns aria-current and forbids callers from overriding it", () => {
    expect(SOURCE).toContain('Omit<React.ComponentProps<"a">, "children" | "aria-current">');
    expect(SOURCE).toMatch(/current = "page",\s*\n\s*pointer,/);
    expect(SOURCE).toMatch(/\{\.\.\.props\}\s*\n\s*aria-current=\{active \? current : undefined\}/);
  });

  test("consumes the shared role ladder rather than authoring fills", () => {
    expect(SOURCE).toContain("listItemInteraction");
    // No bespoke selected/hover fills — the ladder owns every regime.
    expect(SOURCE).not.toMatch(/data-\[selected\]:bg-/);
    expect(SOURCE).not.toMatch(/hover:bg-(?!transparent)/);
  });

  test("uses the standard outward focus ring", () => {
    expect(SOURCE).toContain('outward: "focus-ring"');
    expect(SOURCE).toContain('inset: "focus-ring-inset"');
    expect(SOURCE).toContain('"inset-on-accent": "focus-ring-inset-on-accent"');
    expect(SOURCE).toContain("focusRing");
    expect(SOURCE).not.toMatch(/outline-none[\s\S]{0,80}focus-ring/);
  });
});

/**
 * These are links inside `role="navigation"`. Menu roles would drag in the wrong
 * keyboard model (roving arrow focus, typeahead, escape-to-close) and the wrong
 * assistive-tech announcement.
 */
describe("NavList semantics contract", () => {
  test("takes no menu roles", () => {
    expect(SOURCE).not.toMatch(/role="menu(item)?"/);
    expect(SOURCE).not.toMatch(/aria-selected/);
  });

  test("renders a nav landmark wrapping a list", () => {
    expect(SOURCE).toContain("<nav");
    expect(SOURCE).toContain("<ul");
  });

  test("requires an accessible name on the landmark", () => {
    // Required (not optional) in the props interface — an unlabelled <nav> is not
    // reachable by landmark navigation and these pages carry more than one.
    expect(SOURCE).toMatch(/"aria-label":\s*string;/);
    expect(SOURCE).not.toMatch(/"aria-label"\?:/);
  });

  test("group labels are never heading elements", () => {
    expect(SOURCE).toContain("eyebrowVariants");
    expect(SOURCE).toContain('tone: "secondary"');
    expect(SOURCE).not.toMatch(/<h[1-6]/);
  });
});

describe("NavList action boundary contract", () => {
  test("renders actions as a sibling of the destination link", () => {
    expect(SOURCE).toContain("actions?: React.ReactNode;");
    expect(SOURCE).toContain("<NavListItemActions>{actions}</NavListItemActions>");
    expect(SOURCE).toContain("grid-cols-[minmax(0,1fr)_auto]");
    expect(SOURCE).toMatch(
      /actions\s*&&\s*"grid min-w-0 grid-cols-\[minmax\(0,1fr\)_auto\] rounded-md text-muted-foreground"/,
    );
    expect(SOURCE).toContain("text-muted-foreground");
    expect(SOURCE).toContain('true: "rounded-none text-inherit"');
    expect(SOURCE).toContain('actions && "col-start-1 row-start-1"');
    expect(SOURCE).not.toContain('actions && "col-span-full row-start-1"');
    expect(SOURCE).not.toContain("A real control here is nested-interactive inside a link");
  });

  test("suppresses row wash while sibling controls own the pointer", () => {
    expect(SOURCE).toContain('from "@/lib/nested-interactive"');
    expect(SOURCE).toContain("nestedInteractiveSuppress.selectableRowSiblingControl");
    expect(SOURCE).toContain("data-no-row-click");
  });
});

/**
 * Router-agnostic by construction: the registry must never depend on a router,
 * and the consumer must be able to project the treatment onto its own link.
 */
describe("NavList router seam contract", () => {
  test("exposes asChild and defaults to an anchor", () => {
    expect(SOURCE).toContain("asChild");
    expect(SOURCE).toContain(`const Comp = asChild ? Slot : "a";`);
  });

  test("imports no router", () => {
    expect(SOURCE).not.toMatch(/from "(react-router|@tanstack\/react-router|next\/link)/);
  });
});

