import { describe, expect, test } from "vitest";
import type { Integration } from "@/lib/api";
import { canShowAdminNav } from "./admin-access-gate";

function app(partial: Partial<Integration> & Pick<Integration, "name">): Integration {
  return { displayName: partial.name, ...partial };
}

describe("canShowAdminNav", () => {
  test("local-dev chrome always shows Admin", () => {
    expect(
      canShowAdminNav({ localDevChrome: true, integrations: [] }),
    ).toBe(true);
  });

  test("production shows Admin when the caller can manage an app", () => {
    expect(
      canShowAdminNav({
        localDevChrome: false,
        integrations: [app({ name: "slack", managementPath: "/apps/slack" })],
      }),
    ).toBe(true);
  });

  test("production hides Admin when no app is manageable", () => {
    expect(
      canShowAdminNav({
        localDevChrome: false,
        integrations: [app({ name: "slack" })],
      }),
    ).toBe(false);
  });
});
