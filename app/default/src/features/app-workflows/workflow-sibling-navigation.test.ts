import { describe, expect, it } from "vitest";
import { siblingNavigation } from "./workflow-sibling-navigation";

describe("siblingNavigation", () => {
  it("returns neighbors for a middle item", () => {
    expect(siblingNavigation(["a", "b", "c"], "b")).toEqual({
      index: 1,
      total: 3,
      previousId: "a",
      nextId: "c",
    });
  });

  it("disables previous at the start", () => {
    expect(siblingNavigation(["a", "b"], "a")).toEqual({
      index: 0,
      total: 2,
      previousId: null,
      nextId: "b",
    });
  });

  it("disables next at the end", () => {
    expect(siblingNavigation(["a", "b"], "b")).toEqual({
      index: 1,
      total: 2,
      previousId: "a",
      nextId: null,
    });
  });

  it("returns no neighbors when the id is missing", () => {
    expect(siblingNavigation(["a", "b"], "missing")).toEqual({
      index: -1,
      total: 2,
      previousId: null,
      nextId: null,
    });
  });
});
