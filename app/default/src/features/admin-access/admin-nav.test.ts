import { describe, expect, test } from "vitest";
import { adminNavIdForPathname } from "./admin-nav";

describe("adminNavIdForPathname", () => {
  test("maps Admin destinations", () => {
    expect(adminNavIdForPathname("/admin")).toBe("who-can-use");
    expect(adminNavIdForPathname("/admin/apps/slack")).toBe("who-can-use");
    expect(adminNavIdForPathname("/admin/platform-admins")).toBe("platform-admins");
    expect(adminNavIdForPathname("/admin/versions")).toBe("versions");
    expect(adminNavIdForPathname("/admin/versions/g-issues")).toBe("versions");
    expect(adminNavIdForPathname("/admin/metrics")).toBe("metrics");
  });
});
