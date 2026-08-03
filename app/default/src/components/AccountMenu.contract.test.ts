import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

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

  test("header placement stays icon-only with tooltips", () => {
    expect(THEME_TOGGLE).toMatch(
      /header:\s*\{\s*showLabels:\s*false,\s*tooltips:\s*true/,
    );
  });

  test("menu placement stays icon-only with tooltips", () => {
    expect(THEME_TOGGLE).toMatch(
      /menu:\s*\{\s*showLabels:\s*false,\s*tooltips:\s*true/,
    );
  });

  test("placement defaults apply unless callers override", () => {
    expect(THEME_TOGGLE).toContain("showLabels={showLabels ?? defaults.showLabels}");
    expect(THEME_TOGGLE).toContain("tooltips={tooltips ?? defaults.tooltips}");
  });
});

describe("AccountMenu beat-order contract", () => {
  test("models identity then utilities then theme then session", () => {
    const docs = ACCOUNT_MENU.indexOf(">Docs</Link>");
    const settings = ACCOUNT_MENU.indexOf(">Settings</Link>");
    const themeLabel = ACCOUNT_MENU.search(
      /DropdownMenuLabel[^>]*>\s*Theme\s*</,
    );
    const themeToggle = ACCOUNT_MENU.indexOf('placement="menu"');
    const logout = ACCOUNT_MENU.indexOf("Log out");

    expect(docs).toBeGreaterThan(-1);
    expect(settings).toBeGreaterThan(docs);
    expect(themeLabel).toBeGreaterThan(settings);
    expect(themeToggle).toBeGreaterThan(themeLabel);
    expect(logout).toBeGreaterThan(themeToggle);
  });

  test("frames theme as a labeled group with separators", () => {
    expect(ACCOUNT_MENU).toContain("DropdownMenuGroup");
    expect(ACCOUNT_MENU).toContain("DropdownMenuSeparator");
    expect(ACCOUNT_MENU).toMatch(/DropdownMenuLabel[^>]*>\s*Theme\s*</);
    expect(ACCOUNT_MENU).toContain('<ThemeToggle placement="menu" size="sm" />');
  });

  test("keeps the menu open while changing theme", () => {
    expect(ACCOUNT_MENU).toContain("onPointerDown={(event) => event.preventDefault()}");
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
