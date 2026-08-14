import { describe, expect, it } from "vitest";
import {
  decodeTemporalRunHandle,
  resolveWorkflowRunPublicId,
  runSearchTerms,
  shortRunId,
  workflowRunListTitle,
  workflowRunPathId,
} from "./workflow-format";

function encodeHandle(payload: Record<string, string>): string {
  const json = JSON.stringify(payload);
  const b64 = btoa(json)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
  return b64;
}

describe("shortRunId", () => {
  it("does not collide on Temporal handles that share kind/owner envelope", () => {
    const a = encodeHandle({
      kind: "temporal-run",
      run_workflow_id: "wf-a",
      run_temporal_run_id: "019ff24b-c635-7620-8189-f78ab4a49972",
      owner_key: "ai-spend-tracker",
    });
    const b = encodeHandle({
      kind: "temporal-run",
      run_workflow_id: "wf-b",
      run_temporal_run_id: "019ff182-c5f0-731c-8a1f-4be3d7313c32",
      owner_key: "ai-spend-tracker",
    });

    expect(a.slice(0, 10)).toBe(b.slice(0, 10));
    expect(shortRunId(a)).toBe("f78ab4a49972");
    expect(shortRunId(b)).toBe("4be3d7313c32");
    expect(shortRunId(a)).not.toBe(shortRunId(b));
  });

  it("falls back for non-handle ids", () => {
    expect(shortRunId("run_1")).toBe("run_1");
    expect(shortRunId("abcdefghijklmnopqrstuvwxyz0123456789")).toBe(
      "abcdefghij…23456789",
    );
  });
});

describe("resolveWorkflowRunPublicId", () => {
  it("maps a short route id back to the public handle via known runs", () => {
    const id = encodeHandle({
      kind: "temporal-run",
      run_workflow_id: "wf-a",
      run_temporal_run_id: "019ff24b-c635-7620-8189-f78ab4a49972",
      owner_key: "ai-spend-tracker",
    });
    expect(
      resolveWorkflowRunPublicId("ai-spend-tracker", shortRunId(id), [
        { id },
      ]),
    ).toBe(id);
    expect(workflowRunPathId(id)).toBe("f78ab4a49972");
  });
});

describe("runSearchTerms", () => {
  it("includes the visible short run id so list search can match a copied chip", () => {
    const id = encodeHandle({
      kind: "temporal-run",
      run_workflow_id: "wf-a",
      run_temporal_run_id: "019ff24b-c635-7620-8189-f78ab4a49972",
      owner_key: "ai-spend-tracker",
    });
    const terms = runSearchTerms({
      id,
      provider: "temporal",
      target: { steps: [] },
    });
    expect(terms).toContain("f78ab4a49972");
    expect(terms).toContain("019ff24b-c635-7620-8189-f78ab4a49972");
  });
});

describe("workflowRunListTitle", () => {
  it("uses target or definition on the flat list", () => {
    expect(
      workflowRunListTitle({
        id: "run_1",
        definitionId: "app_daily_digest",
        target: { steps: [] },
      }),
    ).toBe("app_daily_digest");
    expect(
      workflowRunListTitle({
        id: "run_1",
        definitionId: "app_daily_digest",
        target: {
          steps: [{ app: { name: "slack", operation: "post" } }],
        },
      }),
    ).toBe("slack.post");
  });

  it("uses the short run id when grouped by definition", () => {
    const id = encodeHandle({
      kind: "temporal-run",
      run_workflow_id: "wf-a",
      run_temporal_run_id: "019ff24b-c635-7620-8189-f78ab4a49972",
      owner_key: "ai-spend-tracker",
    });
    expect(
      workflowRunListTitle(
        {
          id,
          definitionId: "app_ai-spend-tracker_ai_spend_tracker_daily_digest",
          target: { steps: [] },
        },
        { groupedByDefinition: true },
      ),
    ).toBe("f78ab4a49972");
  });
});

describe("decodeTemporalRunHandle", () => {
  it("round-trips kind and temporal run id", () => {
    const id = encodeHandle({
      kind: "temporal-run",
      run_workflow_id: "wf",
      run_temporal_run_id: "019ff24b-c635-7620-8189-f78ab4a49972",
      owner_key: "ai-spend-tracker",
    });
    expect(decodeTemporalRunHandle(id)).toMatchObject({
      kind: "temporal-run",
      run_temporal_run_id: "019ff24b-c635-7620-8189-f78ab4a49972",
    });
  });
});
