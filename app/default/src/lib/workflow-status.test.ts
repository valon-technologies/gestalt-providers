import { describe, expect, it } from "vitest";
import {
  normalizeWorkflowRun,
  normalizeWorkflowStatus,
} from "@/lib/api";
import { workflowRunCounts } from "@/features/app-workflows/workflow-format";
import { workflowRunBadgeVariant } from "@/lib/workflowActivity";

describe("normalizeWorkflowStatus", () => {
  it.each([
    ["succeeded", "succeeded"],
    ["FAILED", "failed"],
    ["WORKFLOW_RUN_STATUS_SUCCEEDED", "succeeded"],
    ["WORKFLOW_RUN_STATUS_FAILED", "failed"],
    ["WORKFLOW_STEP_STATUS_RUNNING", "running"],
    ["WORKFLOW_STEP_STATUS_PENDING", "pending"],
    ["WORKFLOW_RUN_STATUS_CANCELED", "canceled"],
    ["cancelled", "canceled"],
    ["", "unknown"],
    [undefined, "unknown"],
    ["not-a-status", "unknown"],
  ] as const)("maps %s to %s", (input, expected) => {
    expect(normalizeWorkflowStatus(input)).toBe(expected);
  });
});

describe("normalizeWorkflowRun status", () => {
  it("maps snake_case definition_id from list summaries", () => {
    const run = normalizeWorkflowRun({
      id: "run-1",
      provider: "temporal",
      status: "succeeded",
      target: { steps: [] },
      definition_id: "app_demo_nightly",
    } as Parameters<typeof normalizeWorkflowRun>[0] & {
      definition_id: string;
    });
    expect(run.definitionId).toBe("app_demo_nightly");
  });

  it("collapses proto run and step status enums", () => {
    const run = normalizeWorkflowRun({
      id: "run-1",
      provider: "local",
      status: "WORKFLOW_RUN_STATUS_SUCCEEDED",
      target: { steps: [] },
      steps: [
        {
          stepId: "sync",
          status: "WORKFLOW_STEP_STATUS_SUCCEEDED",
        },
      ],
    });
    expect(run.status).toBe("succeeded");
    expect(run.steps[0]?.status).toBe("succeeded");
  });
});

describe("workflow status presentation", () => {
  it("counts and badges proto enum statuses as success", () => {
    const counts = workflowRunCounts([
      {
        id: "a",
        provider: "local",
        status: "WORKFLOW_RUN_STATUS_SUCCEEDED",
        target: { steps: [] },
      },
      {
        id: "b",
        provider: "local",
        status: "WORKFLOW_RUN_STATUS_FAILED",
        target: { steps: [] },
      },
    ]);
    expect(counts).toEqual({ running: 0, succeeded: 1, failed: 1 });
    expect(workflowRunBadgeVariant("WORKFLOW_RUN_STATUS_SUCCEEDED")).toBe(
      "success",
    );
  });
});
