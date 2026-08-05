import { describe, expect, test } from "vitest";
import { createExclusiveHoverStore } from "@/features/registry/replica-hover-exclusive";

describe("createExclusiveHoverStore", () => {
  test("peek opens hover without pinning", () => {
    const store = createExclusiveHoverStore();
    store.openHover("a");
    expect(store.getSession()).toEqual({ key: "a", mode: "hover" });
  });

  test("pin upgrades hover and survives close attempts for that key", () => {
    const store = createExclusiveHoverStore();
    store.openHover("a");
    store.pin("a");
    expect(store.getSession()).toEqual({ key: "a", mode: "pinned" });
    // openHover must not downgrade pinned.
    store.openHover("a");
    expect(store.getSession()?.mode).toBe("pinned");
  });

  test("opening another key replaces the session (exclusive)", () => {
    const store = createExclusiveHoverStore();
    store.pin("a");
    store.openHover("b");
    expect(store.getSession()).toEqual({ key: "b", mode: "hover" });
  });

  test("close only clears the owning key", () => {
    const store = createExclusiveHoverStore();
    store.pin("a");
    store.close("b");
    expect(store.getSession()?.key).toBe("a");
    store.close("a");
    expect(store.getSession()).toBeNull();
  });
});
