import { describe, expect, it } from "vitest";
import type { WorkflowRun } from "@/lib/api";
import {
  mergeWorkflowDefinitionIds,
  parseWorkflowRunsGroupBy,
  rollupWorkflowRunGroupHeaderStatus,
  rollupWorkflowRunGroupStatus,
  rollupWorkflowStatusCounts,
  serializeWorkflowRunsGroupBy,
  shouldExpandGroupForVisibleMatches,
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

  it("rollups a histogram even when the loaded page is all succeeded", () => {
    expect(
      rollupWorkflowStatusCounts({
        pending: 0,
        running: 0,
        succeeded: 24,
        failed: 3,
        canceled: 0,
      }),
    ).toBe("failed");
    expect(
      rollupWorkflowRunGroupHeaderStatus({
        clientOnlyFilters: false,
        hasMore: true,
        loadedRuns: [run({ id: "1", status: "succeeded" })],
        statusCounts: {
          pending: 0,
          running: 0,
          succeeded: 24,
          failed: 3,
          canceled: 0,
        },
      }),
    ).toBe("failed");
  });

  it("does not claim group health from a truncated client-filtered page", () => {
    expect(
      rollupWorkflowRunGroupHeaderStatus({
        clientOnlyFilters: true,
        hasMore: true,
        loadedRuns: [run({ id: "1", status: "succeeded" })],
        statusCounts: {
          pending: 0,
          running: 0,
          succeeded: 24,
          failed: 3,
          canceled: 0,
        },
      }),
    ).toBe("unknown");
  });

  it("does not paint succeeded from page 1 when the list is truncated without counts", () => {
    expect(
      rollupWorkflowRunGroupHeaderStatus({
        clientOnlyFilters: false,
        hasMore: true,
        loadedRuns: [run({ id: "1", status: "succeeded" })],
      }),
    ).toBe("unknown");
  });

  it("expands a group when filters match loaded rows", () => {
    expect(
      shouldExpandGroupForVisibleMatches({
        filtersActive: true,
        matchingRunCount: 2,
      }),
    ).toBe(true);
    expect(
      shouldExpandGroupForVisibleMatches({
        filtersActive: true,
        matchingRunCount: 0,
      }),
    ).toBe(false);
    expect(
      shouldExpandGroupForVisibleMatches({
        filtersActive: false,
        matchingRunCount: 2,
      }),
    ).toBe(false);
  });
});
