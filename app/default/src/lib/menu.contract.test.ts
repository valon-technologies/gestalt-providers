import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const MENU = readFileSync(join(HERE, "menu.tsx"), "utf8");
const SELECT = readFileSync(
  join(HERE, "../components/ui/select.tsx"),
  "utf8",
);
const DROPDOWN = readFileSync(
  join(HERE, "../components/ui/dropdown-menu.tsx"),
  "utf8",
);

describe("Menu and Select Registry contracts", () => {
  test("menu rows use neutral chrome and size only direct-child icons", () => {
    expect(MENU).toContain("bg-neutral-hover");
    expect(MENU).toContain("bg-neutral-pressed");
    expect(MENU).toContain("outline-none");
    expect(MENU).toContain("menuItemDirectIconClassName");
    expect(MENU).toContain("[&>svg]:pointer-events-none");
    expect(MENU).toContain("[&>svg:not([class*='size-'])]:size-4");
    expect(MENU).not.toContain("[&_svg]:pointer-events-none");
    expect(MENU).not.toContain("[&_svg:not([class*='size-'])]:size-4");
  });

  test("Select option icons target the ItemText slot", () => {
    expect(MENU).toContain("selectItemTextSlotClassName");
    expect(MENU).toContain(
      "[&_[data-slot=select-item-text]>svg:not([class*='size-'])]:size-4",
    );
    expect(SELECT).toContain("selectItemTextSlotClassName");
    expect(SELECT).toContain('data-slot="select-item-text"');
    expect(SELECT).not.toMatch(/ItemText[^>]*className=/);
    expect(SELECT).toContain("*:data-[slot=select-value]:truncate");
  });

  test("dropdown checkbox rows share the SelectionCheck contract", () => {
    expect(DROPDOWN).toContain('menuItemVariants({ indicator: "leading" })');
    expect(DROPDOWN).toContain('tone="solid"');
    expect(DROPDOWN).toContain("SelectionCheck");
  });
});
