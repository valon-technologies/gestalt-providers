import { describe, expect, it } from "vitest";
import type { WorkflowRun } from "@/lib/api";
import { mergeWorkflowRunSummaries } from "./workflow-run-summaries";

function run(partial: Partial<WorkflowRun> & Pick<WorkflowRun, "id">): WorkflowRun {
  return {
    provider: "local",
    target: { steps: [] },
    status: "succeeded",
    ...partial,
  };
}

describe("mergeWorkflowRunSummaries", () => {
  it("indexes runs from later list pages over earlier ones", () => {
    const first = mergeWorkflowRunSummaries(
      [],
      [run({ id: "run_a", status: "running" })],
    );
    const next = mergeWorkflowRunSummaries(first, [
      run({ id: "run_a", status: "succeeded" }),
      run({ id: "run_b", definitionId: "app_example" }),
    ]);
    expect(next.map((item) => item.id)).toEqual(["run_a", "run_b"]);
    expect(next[0]?.status).toBe("succeeded");
  });

  it("keeps existing rows when a page is empty", () => {
    const existing = [run({ id: "run_a" })];
    expect(mergeWorkflowRunSummaries(existing, [])).toEqual(existing);
  });
});
