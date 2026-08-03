import { describe, expect, test } from "vitest";

import {
  catalogCardActivateRoute,
  catalogCardActivateTarget,
} from "./catalogFilters";
import type { Integration } from "@/lib/api";

function stubIntegration(name: string): Integration {
  return { name, displayName: name, description: "" };
}

describe("catalogCardActivateRoute", () => {
  test("whole-card activate is always app overview", () => {
    const integration = stubIntegration("g-issues");
    expect(catalogCardActivateTarget(integration)).toBe("detail");
    expect(catalogCardActivateRoute(integration)).toEqual({
      to: "/apps/$app",
      params: { app: "g-issues" },
    });
  });
});
