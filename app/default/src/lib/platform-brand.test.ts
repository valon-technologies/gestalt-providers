import { describe, expect, test } from "vitest";

import {
  DEFAULT_PLATFORM_BRAND_NAME,
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
});
