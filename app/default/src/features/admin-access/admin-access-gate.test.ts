// @vitest-environment happy-dom
import { afterEach, describe, expect, test, vi } from "vitest";
import {
  canShowAdminNav,
  hasGrantedGestaltAdminAccess,
  probeGestaltAdminAccess,
  resetGestaltAdminAccessCache,
} from "./admin-access-gate";

describe("canShowAdminNav", () => {
  test("shows Admin only for Gestalt admins", () => {
    expect(canShowAdminNav(true)).toBe(true);
    expect(canShowAdminNav(false)).toBe(false);
    expect(canShowAdminNav(undefined)).toBe(false);
  });
});

describe("Gestalt admin admission cache", () => {
  afterEach(() => {
    resetGestaltAdminAccessCache();
    vi.unstubAllGlobals();
  });

  test("starts empty", () => {
    resetGestaltAdminAccessCache();
    expect(hasGrantedGestaltAdminAccess()).toBe(false);
  });

  test("remembers a successful probe so sibling Admin routes skip beforeLoad", async () => {
    resetGestaltAdminAccessCache();
    const fetchMock = vi.fn(async () => new Response("[]", { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    expect(await probeGestaltAdminAccess()).toBe(true);
    expect(hasGrantedGestaltAdminAccess()).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(await probeGestaltAdminAccess()).toBe(true);
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
