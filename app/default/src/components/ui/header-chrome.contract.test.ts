import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "header-chrome.tsx"),
  "utf8",
);

describe("header-chrome", () => {
  test("exports createHeaderChrome factory and icon role helpers", () => {
    expect(SOURCE).toContain("export function createHeaderChrome");
    expect(SOURCE).toContain("export const HEADER_CHROME_ICON_ROLE");
    expect(SOURCE).toContain("export function isHeaderChromeIconChild");
    expect(SOURCE).toContain('Object.assign(Icon, { headerChromeRole: HEADER_CHROME_ICON_ROLE })');
    expect(SOURCE).toContain("data-header-chrome-icon");
    expect(SOURCE).toContain("stackedRowGapY");
    expect(SOURCE).toContain("iconStackPadding");
    expect(SOURCE).toContain("AlignContext");
    expect(SOURCE).toContain('align === "center" ? "left-1/2 -translate-x-1/2" : "left-0"');
    expect(SOURCE).toContain("isHeaderChromeIconChild(child)");
    expect(SOURCE).not.toContain('child.props["data-slot"]');
    expect(SOURCE).not.toContain("rowGapYHasSelectors");
  });
});
