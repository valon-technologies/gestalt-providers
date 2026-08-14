import { describe, expect, it } from "vitest";
import type { WorkflowRun } from "@/lib/api";
import {
  groupWorkflowRunsByDefinition,
  mergeWorkflowDefinitionIds,
  parseWorkflowRunsGroupBy,
  rollupWorkflowRunGroupStatus,
  serializeWorkflowRunsGroupBy,
} from "./workflow-runs-group";

function run(partial: Partial<WorkflowRun> & Pick<WorkflowRun, "id">): WorkflowRun {
  return {
    provider: "local",
    target: { steps: [] },
    status: "succeeded",
    ...partial,
  };
}

describe("workflow-runs-group", () => {
  it("parses and serializes group=definition", () => {
    expect(parseWorkflowRunsGroupBy(undefined)).toBe("none");
    expect(parseWorkflowRunsGroupBy("")).toBe("none");
    expect(parseWorkflowRunsGroupBy("runs")).toBe("none");
    expect(parseWorkflowRunsGroupBy("definition")).toBe("definition");
    expect(serializeWorkflowRunsGroupBy("none")).toBeUndefined();
    expect(serializeWorkflowRunsGroupBy("definition")).toBe("definition");
  });

  it("groups by definition in first-seen order", () => {
    const runs = [
      run({ id: "1", definitionId: "app_b" }),
      run({ id: "2", definitionId: "app_a" }),
      run({ id: "3", definitionId: "app_b" }),
      run({ id: "4" }),
      run({ id: "5", definitionId: "  " }),
    ];
    const groups = groupWorkflowRunsByDefinition(runs);
    expect(groups.map((group) => group.definitionId)).toEqual([
      "app_b",
      "app_a",
      "",
    ]);
    expect(groups[0]?.runs.map((item) => item.id)).toEqual(["1", "3"]);
    expect(groups[1]?.runs.map((item) => item.id)).toEqual(["2"]);
    expect(groups[2]?.label).toBe("Unknown definition");
    expect(groups[2]?.runs.map((item) => item.id)).toEqual(["4", "5"]);
  });

  it("keeps the full definition id as the group label", () => {
    const definitionId =
      "app_ai-spend-tracker_ai_spend_tracker_sync_every_four_hours";
    const groups = groupWorkflowRunsByDefinition([
      run({ id: "1", definitionId }),
    ]);
    expect(groups[0]?.label).toBe(definitionId);
    expect(groups[0]?.label.includes("…")).toBe(false);
  });

  it("merges inventory ids before activity-only ids", () => {
    expect(
      mergeWorkflowDefinitionIds(
        ["def_b", "def_a", "def_b"],
        ["def_a", "def_c", ""],
      ),
    ).toEqual(["def_b", "def_a", "def_c"]);
  });

  it("rollups group status with worst-outcome priority", () => {
    expect(rollupWorkflowRunGroupStatus([])).toBe("unknown");
    expect(
      rollupWorkflowRunGroupStatus([run({ id: "1", status: "succeeded" })]),
    ).toBe("succeeded");
    expect(
      rollupWorkflowRunGroupStatus([
        run({ id: "1", status: "succeeded" }),
        run({ id: "2", status: "running" }),
      ]),
    ).toBe("running");
    expect(
      rollupWorkflowRunGroupStatus([
        run({ id: "1", status: "running" }),
        run({ id: "2", status: "failed" }),
      ]),
    ).toBe("failed");
    expect(
      rollupWorkflowRunGroupStatus([
        run({ id: "1", status: "succeeded" }),
        run({ id: "2", status: "skipped" }),
      ]),
    ).toBe("succeeded");
  });
});
