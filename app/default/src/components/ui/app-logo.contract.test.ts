import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "app-logo.tsx"),
  "utf8",
);

describe("AppLogo wordmark contract", () => {
  test("AppLogoName always uses the display face", () => {
    expect(SOURCE).toContain("font-display");
    expect(SOURCE).toContain("AppLogoName");
    expect(SOURCE).toMatch(/appLogoNameVariants/);
    expect(SOURCE).not.toContain("Season");
  });

  test("sidebar collapse hides name stack via AppLogoText", () => {
    expect(SOURCE).toContain("group-data-[collapsible=icon]:hidden");
    expect(SOURCE).toContain("AppLogoText");
  });

  test("asChild composes lockup layout onto the consumer element", () => {
    const asChildBlock = SOURCE.slice(
      SOURCE.indexOf("if (asChild)"),
      SOURCE.indexOf("if (href)"),
    );
    expect(asChildBlock).toContain("AppLogoSizeContext.Provider");
    expect(asChildBlock).toContain("<Slot");
    expect(asChildBlock).toContain("{children}");
    expect(asChildBlock).not.toContain("{body}");
  });

  test("onNavigate is not overridable by a caller onClick prop", () => {
    expect(SOURCE).toContain(
      'Omit<React.HTMLAttributes<HTMLElement>, "onClick">',
    );
  });

  test("AppLogoName size scale stays on heading tokens (chrome, not PageHeader display)", () => {
    const nameVariants = SOURCE.slice(
      SOURCE.indexOf("const appLogoNameVariants"),
      SOURCE.indexOf("type AppLogoNameSize"),
    );
    expect(nameVariants).toContain('default: "text-heading-sm tracking-display"');
    expect(nameVariants).toContain('md: "text-heading-lg tracking-heading"');
    expect(nameVariants).toContain('lg: "text-heading-xl tracking-display"');
    expect(nameVariants).not.toMatch(/text-display-/);
    expect(nameVariants).not.toMatch(/\bxl\s*:/);
  });
});
