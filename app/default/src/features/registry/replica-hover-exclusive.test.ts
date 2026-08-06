import { describe, expect, test } from "vitest";
import { createExclusiveHoverStore } from "@/features/registry/replica-hover-exclusive";

describe("createExclusiveHoverStore", () => {
  test("peek opens hover without pinning", () => {
    const store = createExclusiveHoverStore();
    store.openHover("a");
    expect(store.getSession()).toEqual({ key: "a", mode: "hover" });
    expect(store.getKeyState("a")).toBe("hover");
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

  test("hover on another key does not steal a pin", () => {
    const store = createExclusiveHoverStore();
    store.pin("a");
    store.openHover("b");
    expect(store.getSession()).toEqual({ key: "a", mode: "pinned" });
    expect(store.getKeyState("b")).toBe("closed");
  });

  test("pinning another key replaces a pinned session (exclusive)", () => {
    const store = createExclusiveHoverStore();
    store.pin("a");
    store.pin("b");
    expect(store.getSession()).toEqual({ key: "b", mode: "pinned" });
    expect(store.getKeyState("a")).toBe("closed");
  });

  test("subscribeKey notifies only affected keys", () => {
    const store = createExclusiveHoverStore();
    let a = 0;
    let b = 0;
    store.subscribeKey("a", () => {
      a += 1;
    });
    store.subscribeKey("b", () => {
      b += 1;
    });
    store.openHover("a");
    expect(a).toBe(1);
    expect(b).toBe(0);
    store.openHover("b");
    expect(a).toBe(2);
    expect(b).toBe(1);
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
