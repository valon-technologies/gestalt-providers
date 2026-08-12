/**
 * Tenant-neutral motion identifiers — public bundle must not publish
 * org-branded class/keyframe IDs.
 *
 * Ownership: `globals.css` owns keyframe IDs; components bind via class /
 * `[data-slot=*-content]`. Names describe the motion role
 * (`spinner-trail`, `accordion-drawer-*`, `collapsible-maxwidth-*`).
 *
 * Failure mode: reintroducing `valon-spinner*` / `valon-accordion*` /
 * `valon-collapsible-maxwidth*` alongside (or instead of) Registry names.
 */

import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, test } from "vitest";

const GLOBALS = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "../../globals.css"),
  "utf8",
);

const BRANDED_MOTION_ID = /valon-(spinner|accordion|collapsible)/;

describe("tenant-neutral motion IDs", () => {
  test("globals.css uses Registry spinner / drawer keyframe and class names", () => {
    expect(GLOBALS).toContain("@keyframes spinner-trail-spin");
    expect(GLOBALS).toContain(".spinner-trail");
    expect(GLOBALS).toContain("@keyframes brand-spinner-trail");
    expect(GLOBALS).toContain(".brand-spinner__trail");
    expect(GLOBALS).toContain("@keyframes accordion-drawer-open");
    expect(GLOBALS).toContain("@keyframes accordion-drawer-close");
    expect(GLOBALS).toContain("animation-name: accordion-drawer-open");
    expect(GLOBALS).toContain("animation-name: accordion-drawer-close");
    expect(GLOBALS).toContain("@keyframes collapsible-maxwidth-open");
    expect(GLOBALS).toContain("@keyframes collapsible-maxwidth-close");
    expect(GLOBALS).not.toMatch(BRANDED_MOTION_ID);
  });

  test("drawer slots bind the height keyframes", () => {
    expect(GLOBALS).toMatch(
      /\[data-slot="accordion-content"\]\[data-state="open"\][\s\S]*?animation-name:\s*accordion-drawer-open/,
    );
    expect(GLOBALS).toMatch(
      /\[data-slot="collapsible-content"\]\[data-state="open"\][\s\S]*?animation-name:\s*accordion-drawer-open/,
    );
  });

  test("Alert animateSize composes height + max-width drawers", () => {
    expect(GLOBALS).toContain(
      "animation-name: accordion-drawer-open, collapsible-maxwidth-open",
    );
    expect(GLOBALS).toContain(
      "animation-name: accordion-drawer-close, collapsible-maxwidth-close",
    );
  });
});
