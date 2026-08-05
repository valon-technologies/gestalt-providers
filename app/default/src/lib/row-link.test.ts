import { describe, expect, test } from "vitest";

import { rowLinkClickIntent, type RowLinkClickEvent } from "./row-link";

const plain: RowLinkClickEvent = {
  button: 0,
  metaKey: false,
  ctrlKey: false,
  shiftKey: false,
  altKey: false,
  targetIsInteractive: false,
};

describe("rowLinkClickIntent", () => {
  test("plain left-click navigates in the SPA", () => {
    expect(rowLinkClickIntent(plain)).toBe("navigate");
  });

  test("modifier clicks are left to the browser", () => {
    expect(rowLinkClickIntent({ ...plain, metaKey: true })).toBe("native");
    expect(rowLinkClickIntent({ ...plain, ctrlKey: true })).toBe("native");
    expect(rowLinkClickIntent({ ...plain, shiftKey: true })).toBe("native");
    expect(rowLinkClickIntent({ ...plain, altKey: true })).toBe("native");
  });

  test("non-primary buttons are left to the browser", () => {
    expect(rowLinkClickIntent({ ...plain, button: 1 })).toBe("native");
    expect(rowLinkClickIntent({ ...plain, button: 2 })).toBe("native");
  });

  test("clicks on interactive controls inside the surface suppress navigation", () => {
    expect(rowLinkClickIntent({ ...plain, targetIsInteractive: true })).toBe(
      "suppress",
    );
  });
});
