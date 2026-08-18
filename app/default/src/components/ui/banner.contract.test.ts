import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const RAW = readFileSync(join(HERE, "banner.tsx"), "utf8");
const SOURCE = RAW.replace(/\/\*[\s\S]*?\*\//g, "").replace(/^\s*\/\/.*$/gm, "");

describe("Banner vendor convention", () => {
  test("imports cn from @/lib/cn (console vendor convention)", () => {
    expect(SOURCE).toContain('from "@/lib/cn"');
    expect(SOURCE).not.toContain('from "@/lib/utils"');
  });

  test("does not own sticky placement", () => {
    expect(SOURCE).not.toContain("sticky");
    expect(SOURCE).not.toContain("fixed");
  });
});

describe("Banner geometry contract", () => {
  test("has no rounded corners", () => {
    expect(SOURCE).not.toMatch(/rounded-(?!\[inherit\])/);
  });

  test("has no border chrome", () => {
    expect(SOURCE).toContain("w-full");
    expect(SOURCE).not.toMatch(/\bborder(?:-[a-z0-9_[\]-]+)?\b/);
  });
});

describe("Banner intent + a11y contract", () => {
  test("ships default/info/warning/destructive, never success", () => {
    expect(SOURCE).toContain("default:");
    expect(SOURCE).toContain("info:");
    expect(SOURCE).toContain("warning:");
    expect(SOURCE).toContain("destructive:");
    expect(SOURCE).not.toMatch(/\bsuccess:/);
  });

  test("sets no default ARIA live role", () => {
    expect(SOURCE).not.toContain('role="alert"');
    expect(SOURCE).not.toContain('role="status"');
  });

  test("BannerIcon is decorative", () => {
    expect(SOURCE).toMatch(/BannerIcon[\s\S]*?aria-hidden="true"/);
  });
});

describe("Banner title + description pairing", () => {
  test("ships BannerTitle as a sibling slot", () => {
    expect(SOURCE).toContain("function BannerTitle");
    expect(SOURCE).toContain('data-slot="banner-title"');
    expect(SOURCE).toContain("min-w-0 shrink-0 font-medium tracking-tight");
    expect(SOURCE).not.toContain("line-clamp");
    expect(SOURCE).not.toContain("truncate");
    expect(SOURCE).toContain(
      "export { Banner, BannerIcon, BannerTitle, BannerDescription, BannerActions, BannerClose }",
    );
  });

  test("mutes Description when Title is present", () => {
    expect(SOURCE).toContain(
      "group-has-[[data-slot=banner-title]]/banner:text-muted-foreground",
    );
  });
});
