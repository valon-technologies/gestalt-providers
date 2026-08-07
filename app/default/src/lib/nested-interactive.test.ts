import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  NESTED_INTERACTIVE_OPT_OUT_ATTR,
  NESTED_INTERACTIVE_SELECTOR,
  nestedInteractiveHasArg,
  nestedInteractiveSuppress,
  SURFACE_LINK_ANCHOR_ATTR,
} from "./nested-interactive";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "nested-interactive.ts"),
  "utf8",
);

describe("nested-interactive", () => {
  test("NESTED_INTERACTIVE_SELECTOR lists tags, roles, and the opt-out attr", () => {
    expect(NESTED_INTERACTIVE_SELECTOR).toContain("button");
    expect(NESTED_INTERACTIVE_SELECTOR).toContain("[role=button]");
    expect(NESTED_INTERACTIVE_SELECTOR).toContain(
      `[${NESTED_INTERACTIVE_OPT_OUT_ATTR}]`,
    );
  });

  test("nestedInteractiveHasArg excludes the stretch anchor from a matches by default", () => {
    expect(nestedInteractiveHasArg("hover")).toBe(
      `a:not([${SURFACE_LINK_ANCHOR_ATTR}]):hover,button:hover,input:hover,select:hover,textarea:hover,[role=button]:hover,[role=checkbox]:hover,[role=combobox]:hover,[${NESTED_INTERACTIVE_OPT_OUT_ATTR}]:hover`,
    );
    expect(nestedInteractiveHasArg("active")).toContain(
      `a:not([${SURFACE_LINK_ANCHOR_ATTR}]):active`,
    );
  });

  test("nestedInteractiveHasArg can include every anchor when excludeAnchorAttr is null", () => {
    expect(nestedInteractiveHasArg("hover", { excludeAnchorAttr: null })).toMatch(
      /^a:hover,/,
    );
  });

  test("nestedInteractiveHasArg can omit link matchers for sibling-link rows", () => {
    expect(nestedInteractiveHasArg("hover", { omitLinkMatchers: true })).toBe(
      `button:hover,input:hover,select:hover,textarea:hover,[role=button]:hover,[role=checkbox]:hover,[role=combobox]:hover,[${NESTED_INTERACTIVE_OPT_OUT_ATTR}]:hover`,
    );
  });

  test("suppress presets embed default :has() membership (lockstep with hasArg)", () => {
    const hover = nestedInteractiveHasArg("hover");
    const active = nestedInteractiveHasArg("active");

    expect(nestedInteractiveSuppress.tableRow).toBe(
      `[&_tr:hover:has(${hover})]:bg-transparent [&_tr:active:has(${active})]:bg-transparent`,
    );
    expect(nestedInteractiveSuppress.tableStickyCell).toBe(
      `[tbody_tr:hover:has(${hover})_&]:bg-card [tbody_tr:active:has(${active})_&]:bg-card`,
    );
    expect(nestedInteractiveSuppress.solidSecondary).toBe(
      `[&:hover:has(${hover})]:bg-secondary [&:active:has(${active})]:bg-secondary`,
    );
    expect(nestedInteractiveSuppress.outlineCard).toBe(
      `[&:hover:has(${hover})]:bg-card [&:active:has(${active})]:bg-card`,
    );
    expect(nestedInteractiveSuppress.treeItemLabel).toBe(
      `[.group:hover:has(${hover})_&]:bg-background [.group:active:has(${active})_&]:bg-background`,
    );

    const hoverNoLink = nestedInteractiveHasArg("hover", { omitLinkMatchers: true });
    const activeNoLink = nestedInteractiveHasArg("active", { omitLinkMatchers: true });

    expect(nestedInteractiveSuppress.solidNeutralHoverStretchLink).toBe(
      `[&:hover:has(${hoverNoLink})]:bg-neutral-hover [&:active:has(${activeNoLink})]:bg-neutral-hover`,
    );
    expect(nestedInteractiveSuppress.selectableRowSiblingControl).toBe(
      `[&:hover:has(${hoverNoLink}):not([data-selected]):not([data-soft])]:bg-transparent [&:active:has(${activeNoLink}):not([data-selected]):not([data-soft])]:bg-transparent [&:hover:has(${hoverNoLink}):not([data-selected]):not([data-soft])]:text-muted-foreground [&:active:has(${activeNoLink}):not([data-selected]):not([data-soft])]:text-muted-foreground [&:hover:has(${hoverNoLink})[data-selected]]:bg-accent-vivid [&:active:has(${activeNoLink})[data-selected]]:bg-accent-vivid [&:hover:has(${hoverNoLink})[data-selected]]:text-accent-vivid-foreground [&:active:has(${activeNoLink})[data-selected]]:text-accent-vivid-foreground`,
    );
  });

  test("suppress presets are complete string literals (Tailwind @source requires it)", () => {
    expect(SOURCE).toContain(nestedInteractiveSuppress.tableRow);
    expect(SOURCE).toContain(nestedInteractiveSuppress.tableStickyCell);
    expect(SOURCE).toContain(nestedInteractiveSuppress.solidSecondary);
    expect(SOURCE).toContain(nestedInteractiveSuppress.solidNeutralHoverStretchLink);
    expect(SOURCE).toContain(nestedInteractiveSuppress.outlineCard);
    expect(SOURCE).toContain(nestedInteractiveSuppress.selectableRowSiblingControl);
    expect(SOURCE).toContain(nestedInteractiveSuppress.treeItemLabel);
    expect(SOURCE).not.toContain("nestedInteractiveSuppressDescendant");
    expect(SOURCE).not.toContain("`[&:hover:has(${");
    expect(SOURCE).not.toContain("`[&_${descendant}");
  });
});
