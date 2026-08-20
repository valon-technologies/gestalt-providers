import { describe, expect, test } from "vitest";
import { ADMIN_PATH, SETUP_PATH, DOCS_PATH } from "@/lib/constants";
import { CHROME_NAV, chromeProductNav, chromeUtilityNav } from "./chrome-nav";

describe("chrome nav destinations", () => {
  test("Setup is a product destination, not an account utility", () => {
    expect(chromeProductNav(false).map((item) => item.label)).toEqual([
      "Apps",
      "Setup",
    ]);
    expect(chromeUtilityNav().map((item) => item.label)).toEqual([
      "Docs",
      "Settings",
    ]);
    expect(chromeUtilityNav().map((item) => item.to)).not.toContain(SETUP_PATH);
  });

  test("product and utility destinations do not share a path", () => {
    const productTos = CHROME_NAV.filter((item) => item.kind === "product").map(
      (item) => item.to,
    );
    const utilityTos = CHROME_NAV.filter((item) => item.kind === "utility").map(
      (item) => item.to,
    );
    expect(productTos.filter((to) => utilityTos.includes(to))).toEqual([]);
  });

  test("Admin stays a gated product destination", () => {
    expect(chromeProductNav(false).map((item) => item.to)).not.toContain(
      ADMIN_PATH,
    );
    expect(chromeProductNav(true).map((item) => item.to)).toEqual([
      "/apps",
      SETUP_PATH,
      ADMIN_PATH,
    ]);
  });

  test("Docs and Settings stay utilities", () => {
    expect(chromeUtilityNav().map((item) => item.to)).toEqual([
      DOCS_PATH,
      "/settings",
    ]);
    expect(chromeProductNav(true).map((item) => item.label)).not.toContain(
      "Docs",
    );
  });
});
