import { beforeEach, describe, expect, it, vi } from "vitest";
import {
  forgetWorkflowDefinitionGroupDisclosureMemory,
  isWorkflowDefinitionGroupOpen,
  readCollapsedWorkflowDefinitionIds,
  resetWorkflowDefinitionGroupDisclosure,
  setWorkflowDefinitionGroupOpen,
} from "./workflow-runs-group-disclosure";

const session = new Map<string, string>();

describe("workflow-runs-group-disclosure", () => {
  beforeEach(() => {
    session.clear();
    vi.stubGlobal("sessionStorage", {
      getItem: (key: string) => session.get(key) ?? null,
      setItem: (key: string, value: string) => {
        session.set(key, value);
      },
      removeItem: (key: string) => {
        session.delete(key);
      },
    });
    resetWorkflowDefinitionGroupDisclosure();
  });

  it("defaults every definition group to open", () => {
    expect(isWorkflowDefinitionGroupOpen("spend", "def_a")).toBe(true);
    expect(readCollapsedWorkflowDefinitionIds("spend").size).toBe(0);
  });

  it("remembers collapsed groups across remount-style reads", () => {
    setWorkflowDefinitionGroupOpen("spend", "def_a", false);
    setWorkflowDefinitionGroupOpen("spend", "def_b", false);
    setWorkflowDefinitionGroupOpen("spend", "def_b", true);

    expect(isWorkflowDefinitionGroupOpen("spend", "def_a")).toBe(false);
    expect(isWorkflowDefinitionGroupOpen("spend", "def_b")).toBe(true);
    expect([...readCollapsedWorkflowDefinitionIds("spend")]).toEqual(["def_a"]);
  });

  it("isolates collapsed ids by app", () => {
    setWorkflowDefinitionGroupOpen("spend", "def_a", false);
    expect(isWorkflowDefinitionGroupOpen("other", "def_a")).toBe(true);
  });

  it("restores collapsed ids from sessionStorage when memory is empty", () => {
    setWorkflowDefinitionGroupOpen("spend", "def_a", false);
    forgetWorkflowDefinitionGroupDisclosureMemory();

    expect(isWorkflowDefinitionGroupOpen("spend", "def_a")).toBe(false);
  });
});
