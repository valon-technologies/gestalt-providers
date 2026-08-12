import { describe, expect, test } from "vitest";

import {
  DEFAULT_PLATFORM_BRAND_NAME,
  applyPlatformBrandIcons,
  defaultPlatformBrand,
  normalizeBrand,
} from "@/lib/platform-brand";

describe("platform-brand", () => {
  test("defaults to Gestalt", () => {
    expect(defaultPlatformBrand()).toEqual({ name: DEFAULT_PLATFORM_BRAND_NAME });
    expect(DEFAULT_PLATFORM_BRAND_NAME).toBe("Gestalt");
  });

  test("normalizeBrand requires a non-empty payload", () => {
    expect(normalizeBrand({})).toBeNull();
    expect(normalizeBrand({ name: "  " })).toBeNull();
    expect(normalizeBrand({ name: "Valon Tools" })).toEqual({
      name: "Valon Tools",
    });
    expect(
      normalizeBrand({ name: "Valon Tools", markSrc: "theme/mark.svg" }),
    ).toEqual({
      name: "Valon Tools",
      markSrc: "/theme/mark.svg",
    });
    expect(normalizeBrand({ markSrc: "/theme/mark.svg" })).toEqual({
      name: DEFAULT_PLATFORM_BRAND_NAME,
      markSrc: "/theme/mark.svg",
    });
  });

  test("applyPlatformBrandIcons retargets tab icons at the mark", () => {
    const attrs = new Map<string, string>([
      ["href", "/favicon-32x32.png"],
      ["type", "image/png"],
      ["sizes", "32x32"],
    ]);
    const icon = {
      rel: "icon",
      setAttribute(name: string, value: string) {
        attrs.set(name, value);
      },
      removeAttribute(name: string) {
        attrs.delete(name);
      },
    };
    const appleAttrs = new Map<string, string>([
      ["href", "/apple-touch-icon.png"],
      ["sizes", "180x180"],
    ]);
    const apple = {
      rel: "apple-touch-icon",
      setAttribute(name: string, value: string) {
        appleAttrs.set(name, value);
      },
      removeAttribute(name: string) {
        appleAttrs.delete(name);
      },
    };
    applyPlatformBrandIcons("/theme/mark.svg", {
      querySelectorAll: () => [icon, apple],
    });
    expect(attrs.get("href")).toBe("/theme/mark.svg");
    expect(attrs.get("type")).toBe("image/svg+xml");
    expect(attrs.has("sizes")).toBe(false);
    expect(appleAttrs.get("href")).toBe("/theme/mark.svg");
    expect(appleAttrs.get("sizes")).toBe("180x180");
  });
});
