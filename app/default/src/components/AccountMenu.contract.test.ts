import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  ACCOUNT_MENU_THEME_LABEL_ID,
  ACCOUNT_MENU_THEME_SECTION_LABEL,
  ACCOUNT_MENU_UTILITY_LINKS,
} from "./AccountMenu";

const DIR = dirname(fileURLToPath(import.meta.url));

const THEME_TOGGLE = readFileSync(join(DIR, "ui/theme-toggle.tsx"), "utf8").replace(
  /^\s*\/\/.*$/gm,
  "",
);
const ACCOUNT_MENU = readFileSync(join(DIR, "AccountMenu.tsx"), "utf8").replace(
  /^\s*\/\/.*$/gm,
  "",
);
const NAV = readFileSync(join(DIR, "Nav.tsx"), "utf8").replace(/^\s*\/\/.*$/gm, "");

describe("ThemeToggle placement contract", () => {
  test("exposes header and menu placements", () => {
    expect(THEME_TOGGLE).toContain('export type ThemeTogglePlacement = "header" | "menu"');
    expect(THEME_TOGGLE).toContain("placement?: ThemeTogglePlacement");
    expect(THEME_TOGGLE).toContain('placement = "header"');
  });

  test("placements share one icon+tooltip presentation until they diverge", () => {
    expect(THEME_TOGGLE).toContain("ICON_TOOLTIP_PRESENTATION");
    expect(THEME_TOGGLE).toMatch(
      /header:\s*ICON_TOOLTIP_PRESENTATION/,
    );
    expect(THEME_TOGGLE).toMatch(/menu:\s*ICON_TOOLTIP_PRESENTATION/);
    expect(THEME_TOGGLE).toContain("showLabels: false");
    expect(THEME_TOGGLE).toContain("tooltips: true");
  });

  test("placement defaults apply unless callers override", () => {
    expect(THEME_TOGGLE).toContain("showLabels={showLabels ?? defaults.showLabels}");
    expect(THEME_TOGGLE).toContain("tooltips={tooltips ?? defaults.tooltips}");
  });

  test("supports labelledBy for a visible section label", () => {
    expect(THEME_TOGGLE).toContain("export type ThemeToggleNameProps");
    expect(THEME_TOGGLE).toContain(
      'Extract<SegmentedControlNameProps, { labelledBy: string }>',
    );
    expect(THEME_TOGGLE).toContain('label: label?.trim() || "Theme"');
    expect(THEME_TOGGLE).toContain("{...nameProps}");
    expect(THEME_TOGGLE).not.toContain("labelledBy={labelledBy}");
    expect(THEME_TOGGLE).not.toContain("label={label}");
  });
});

describe("AccountMenu beat-order contract", () => {
  test("exports utility links with Docs before Settings", () => {
    expect(ACCOUNT_MENU_UTILITY_LINKS.map((link) => link.label)).toEqual([
      "Docs",
      "Settings",
    ]);
  });

  test("models identity then utilities then theme then session", () => {
    const utilities = ACCOUNT_MENU.indexOf("{ACCOUNT_MENU_UTILITY_LINKS.map");
    const themeLabel = ACCOUNT_MENU.indexOf("{ACCOUNT_MENU_THEME_SECTION_LABEL}");
    const themeToggle = ACCOUNT_MENU.indexOf('placement="menu"');
    const labelledBy = ACCOUNT_MENU.indexOf("labelledBy={ACCOUNT_MENU_THEME_LABEL_ID}");
    const logout = ACCOUNT_MENU.indexOf("Log out");

    expect(utilities).toBeGreaterThan(-1);
    expect(themeLabel).toBeGreaterThan(utilities);
    expect(themeToggle).toBeGreaterThan(themeLabel);
    expect(labelledBy).toBeGreaterThan(themeLabel);
    expect(logout).toBeGreaterThan(themeToggle);
  });

  test("frames theme as a labeled group wired to the radiogroup", () => {
    expect(ACCOUNT_MENU_THEME_SECTION_LABEL).toBe("Theme");
    expect(ACCOUNT_MENU_THEME_LABEL_ID).toBe("account-menu-theme-label");
    expect(ACCOUNT_MENU).toContain("DropdownMenuGroup");
    expect(ACCOUNT_MENU).toContain("DropdownMenuSeparator");
    expect(ACCOUNT_MENU).toContain("labelledBy={ACCOUNT_MENU_THEME_LABEL_ID}");
    expect(ACCOUNT_MENU).toContain("id={ACCOUNT_MENU_THEME_LABEL_ID}");
  });

  test("keeps the menu open while changing theme", () => {
    expect(ACCOUNT_MENU).toContain("onPointerDown={(event) => event.preventDefault()}");
  });

  test("documents Docs as signed-in account chrome", () => {
    expect(ACCOUNT_MENU).toMatch(/Docs is account-gated/);
  });

  test("account trigger uses ghost Button for hover and press", () => {
    expect(ACCOUNT_MENU).toContain('import { Button } from "./ui/button"');
    expect(ACCOUNT_MENU).toContain('variant="ghost"');
    expect(ACCOUNT_MENU).toContain('size="icon-lg"');
    expect(ACCOUNT_MENU).toContain('aria-label="Open user menu"');
    expect(ACCOUNT_MENU).toContain('className="group rounded-full p-0');
    // Hover = Neutral-dark fill only; press adds fill + press scrim (no stacked hover scrim).
    expect(ACCOUNT_MENU).toContain("hover:after:opacity-0");
    expect(ACCOUNT_MENU).toContain(
      "active:after:opacity-[var(--state-overlay-press,0.14)]",
    );
    expect(ACCOUNT_MENU).toContain(
      "data-[state=open]:after:opacity-[var(--state-overlay-press,0.14)]",
    );
    expect(ACCOUNT_MENU).toContain("bg-neutral-hover");
    expect(ACCOUNT_MENU).toContain(
      "group-hover:group-not-active:group-not-data-[state=open]:bg-neutral-dark-hover",
    );
    expect(ACCOUNT_MENU).toContain("group-active:bg-neutral-dark-pressed");
    expect(ACCOUNT_MENU).toContain("group-data-[state=open]:bg-neutral-dark-pressed");
  });
});

describe("Nav account chrome ownership", () => {
  test("delegates signed-in flyout to AccountMenu", () => {
    expect(NAV).toContain("import { AccountMenu } from \"./AccountMenu\"");
    expect(NAV).toContain("<AccountMenu");
    expect(NAV).not.toContain("Open user menu");
  });

  test("keeps header theme access for guests only", () => {
    expect(NAV).toContain('<ThemeToggle placement="header" size="sm" />');
    expect(NAV).toContain("{!displayLabel && <ThemeToggle placement=\"header\" size=\"sm\" />}");
  });

  test("primary nav stays product destinations", () => {
    expect(NAV).toContain('{ href: "/apps", label: "Apps" }');
    expect(NAV).toContain('{ href: BUILD_PATH, label: "Build" }');
    expect(NAV).not.toContain('label: "Docs"');
  });
});
