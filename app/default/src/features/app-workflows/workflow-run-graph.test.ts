import { describe, expect, it } from "vitest";
import type { WorkflowRun } from "@/lib/api";
import {
  formatDuration,
  projectWorkflowRunGraph,
} from "./workflow-run-graph";

function baseRun(overrides: Partial<WorkflowRun> = {}): WorkflowRun {
  return {
    id: "run_1",
    provider: "basic",
    status: "succeeded",
    target: {
      steps: [
        { id: "a", app: { name: "slack", operation: "chat.postMessage" } },
        { id: "b", app: { name: "datadog", operation: "monitors.get" } },
      ],
    },
    startedAt: "2026-08-04T12:00:00Z",
    completedAt: "2026-08-04T12:01:30Z",
    steps: [
      {
        stepId: "a",
        status: "succeeded",
        startedAt: "2026-08-04T12:00:00Z",
        completedAt: "2026-08-04T12:00:40Z",
      },
      {
        stepId: "b",
        status: "skipped",
        skipReason: "when_false",
        startedAt: "2026-08-04T12:00:40Z",
        completedAt: "2026-08-04T12:00:40Z",
      },
    ],
    ...overrides,
  };
}

describe("projectWorkflowRunGraph", () => {
  it("projects sequential steps into one job when stages are absent", () => {
    const graph = projectWorkflowRunGraph(baseRun());
    expect(graph.stages).toHaveLength(1);
    expect(graph.stages[0]?.kind).toBe("sequential");
    expect(graph.stages[0]?.jobs).toHaveLength(1);
    expect(graph.stages[0]?.jobs[0]?.steps.map((step) => step.id)).toEqual([
      "a",
      "b",
    ]);
    expect(graph.durationMs).toBe(90_000);
    expect(formatDuration(graph.durationMs)).toBe("1m 30s");
  });

  it("preserves explicit parallel stages for future backend support", () => {
    const graph = projectWorkflowRunGraph(
      baseRun({
        stages: [
          {
            id: "gate",
            kind: "sequential",
            jobs: [
              {
                id: "changes",
                name: "changes",
                status: "succeeded",
                durationMs: 10_000,
                steps: [{ id: "detect", name: "Detect", status: "succeeded" }],
              },
            ],
          },
          {
            id: "checks",
            kind: "parallel",
            jobs: [
              {
                id: "ui",
                name: "UI tests",
                status: "skipped",
                durationMs: 0,
                steps: [{ id: "skip", name: "Skip", status: "skipped" }],
              },
              {
                id: "validate",
                name: "Validate",
                status: "succeeded",
                durationMs: 50_000,
                steps: [{ id: "run", name: "Run", status: "succeeded" }],
              },
            ],
          },
        ],
      }),
    );
    expect(graph.stages.map((stage) => stage.kind)).toEqual([
      "sequential",
      "parallel",
    ]);
    expect(graph.stages[1]?.jobs).toHaveLength(2);
  });
});
