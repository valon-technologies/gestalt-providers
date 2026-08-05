import { describe, expect, it } from "vitest";
import type { WorkflowDefinition, WorkflowRun } from "@/lib/api";
import {
  workflowDefinitionMatchesApp,
  workflowRunBadgeVariant,
  workflowRunMatchesApp,
} from "@/lib/workflowActivity";

describe("workflowRunBadgeVariant", () => {
  it.each([
    ["succeeded", "success"],
    ["failed", "destructive"],
    ["running", "info"],
    ["pending", "warning"],
    ["canceled", "muted"],
    ["WORKFLOW_RUN_STATUS_SUCCEEDED", "success"],
    ["WORKFLOW_STEP_STATUS_FAILED", "destructive"],
    [undefined, "muted"],
    ["unknown", "muted"],
  ] as const)("maps %s to %s", (status, variant) => {
    expect(workflowRunBadgeVariant(status)).toBe(variant);
  });
});

function run(partial: Partial<WorkflowRun> & Pick<WorkflowRun, "id" | "provider">): WorkflowRun {
  return {
    target: { steps: [] },
    ...partial,
  };
}

describe("workflowRunMatchesApp", () => {
  it("prefers server-declared targetApp over step targets", () => {
    const owned = run({
      id: "run-1",
      provider: "local",
      targetApp: "slack",
      target: {
        steps: [{ id: "s1", app: { name: "datadog", operation: "monitors.get" } }],
      },
    });
    expect(workflowRunMatchesApp(owned, "slack")).toBe(true);
    expect(workflowRunMatchesApp(owned, "datadog")).toBe(false);
  });

  it("falls back to step targets when targetApp is absent", () => {
    const owned = run({
      id: "run-2",
      provider: "local",
      target: {
        steps: [{ id: "s1", app: { name: "slack", operation: "chat.postMessage" } }],
      },
    });
    expect(workflowRunMatchesApp(owned, "slack")).toBe(true);
  });

  it("falls back to app_<name>_ definition ids when target is empty", () => {
    const owned = run({
      id: "run-3",
      provider: "local",
      definitionId: "app_slack_notify_on_failure",
    });
    expect(workflowRunMatchesApp(owned, "slack")).toBe(true);
  });
});

describe("workflowDefinitionMatchesApp", () => {
  it("matches step target apps", () => {
    const definition: WorkflowDefinition = {
      id: "def_1",
      provider: "basic",
      target: {
        steps: [{ id: "s1", app: { name: "slack", operation: "chat.postMessage" } }],
      },
      activations: [],
    };
    expect(workflowDefinitionMatchesApp(definition, "slack")).toBe(true);
    expect(workflowDefinitionMatchesApp(definition, "gmail")).toBe(false);
  });

  it("matches app_<name>_ definition ids", () => {
    const definition: WorkflowDefinition = {
      id: "app_slack_notify",
      provider: "basic",
      target: { steps: [] },
      activations: [],
    };
    expect(workflowDefinitionMatchesApp(definition, "slack")).toBe(true);
  });
});
