import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "app-top-bar.tsx"),
  "utf8",
);

describe("AppTopBar column contract", () => {
  test("Inner and Page share one column clamp + gutter source", () => {
    expect(SOURCE).toContain("const appTopBarColumnVariants = cva(");
    expect(SOURCE).toContain(
      "cn(appTopBarColumnVariants(), appTopBarInnerVariants(), className)",
    );
    expect(SOURCE).toContain(
      "cn(appTopBarColumnVariants(), appTopBarPageVariants(), className)",
    );
    expect(SOURCE).not.toMatch(/max-w-7xl[\s\S]*max-w-7xl/);
  });

  test("column clamp matches Container so chrome and pages stay aligned", () => {
    expect(SOURCE).toContain('cva("mx-auto w-full max-w-7xl px-6")');
  });

  test("shell owns sticky stacking — bar itself is not sticky", () => {
    expect(SOURCE).not.toContain("sticky top-0");
    expect(SOURCE).toContain('cva("w-full border-b bg-background")');
  });

  test("brand wordmark always uses the display face", () => {
    expect(SOURCE).toContain("AppTopBarBrand");
    expect(SOURCE).toContain("AppLogoName");
    expect(SOURCE).toMatch(/PageHeaderTitle here/);
  });

  test("brand props attach to the interactive AppLogo root", () => {
    const brandBlock = SOURCE.slice(
      SOURCE.indexOf("function AppTopBarBrand"),
      SOURCE.indexOf("export {"),
    );
    expect(brandBlock).toContain("{...props}");
    expect(brandBlock).toContain("<AppLogo");
    expect(brandBlock).not.toMatch(/<AppLogoName[^>]*\{\.\.\.props\}/);
  });
});
