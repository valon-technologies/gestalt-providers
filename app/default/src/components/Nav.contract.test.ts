import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const NAV = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "Nav.tsx"),
  "utf8",
).replace(/^\s*\/\/.*$/gm, "");

describe("Nav AppTopBar composition", () => {
  test("composes Registry AppTopBar with display-face brand", () => {
    expect(NAV).toContain("AppTopBar");
    expect(NAV).toContain("AppTopBarBrand");
    expect(NAV).toContain('<AppTopBarBrand size="lg" asChild>');
    expect(NAV).toContain('<Link to="/apps">');
    expect(NAV).toContain("AppLogoName");
    expect(NAV).toContain("usePlatformBrand");
    expect(NAV).toContain("brand.name");
    expect(NAV).not.toContain(">Gestalt<");
    expect(NAV).toContain("AppTopBarStart");
    expect(NAV).toContain("AppTopBarCenter");
    expect(NAV).toContain("AppTopBarEnd");
    expect(NAV).not.toContain("onNavigate");
    expect(NAV).not.toContain("font-heading");
    expect(NAV).not.toContain("font-bold");
  });

  test("delegates signed-in flyout to AccountMenu", () => {
    expect(NAV).toContain('import { AccountMenu } from "./AccountMenu"');
    expect(NAV).toContain("<AccountMenu");
    expect(NAV).not.toContain("Open user menu");
  });

  test("keeps header theme access for guests only", () => {
    expect(NAV).toContain('<ThemeToggle placement="header" size="sm" />');
    expect(NAV).toContain('{!displayLabel && <ThemeToggle placement="header" size="sm" />}');
  });

  test("primary nav stays product destinations", () => {
    expect(NAV).toContain('{ href: "/apps", label: "Apps" }');
    expect(NAV).toContain('{ href: BUILD_PATH, label: "Build" }');
    expect(NAV).not.toContain('label: "Docs"');
  });
});
