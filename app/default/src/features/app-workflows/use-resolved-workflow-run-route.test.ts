import { describe, expect, it } from "vitest";
import { rewriteShortWorkflowRunPath, publicWorkflowRunIdForGetRun } from "./use-resolved-workflow-run-route";
import { shortRunId } from "./workflow-format";

function encodeHandle(payload: Record<string, string>): string {
  const json = JSON.stringify(payload);
  const b64 = btoa(json)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
  return b64;
}

describe("rewriteShortWorkflowRunPath", () => {
  const publicRunId = encodeHandle({
    kind: "temporal-run",
    run_workflow_id: "wf-a",
    run_temporal_run_id: "019ff24b-c635-7620-8189-f78ab4a49972",
    owner_key: "example-app",
  });
  const short = shortRunId(publicRunId);

  it("rewrites a full-handle run URL to the short segment", () => {
    const next = rewriteShortWorkflowRunPath({
      pathname: `/apps/example-app/admin/workflows/runs/${encodeURIComponent(publicRunId)}`,
      app: "example-app",
      routeRunId: publicRunId,
      publicRunId,
    });
    expect(next).toBe(
      `/apps/example-app/admin/workflows/runs/${encodeURIComponent(short)}`,
    );
  });

  it("keeps a job/step suffix", () => {
    const next = rewriteShortWorkflowRunPath({
      pathname: `/apps/example-app/admin/workflows/runs/${encodeURIComponent(publicRunId)}/jobs/post/steps/run`,
      app: "example-app",
      routeRunId: publicRunId,
      publicRunId,
    });
    expect(next).toBe(
      `/apps/example-app/admin/workflows/runs/${encodeURIComponent(short)}/jobs/post/steps/run`,
    );
  });

  it("leaves an already-short path alone", () => {
    expect(
      rewriteShortWorkflowRunPath({
        pathname: `/apps/example-app/admin/workflows/runs/${short}`,
        app: "example-app",
        routeRunId: short,
        publicRunId,
      }),
    ).toBeNull();
  });
});

describe("publicWorkflowRunIdForGetRun", () => {
  const publicRunId = encodeHandle({
    kind: "temporal-run",
    run_workflow_id: "wf-a",
    run_temporal_run_id: "019ff24b-c635-7620-8189-f78ab4a49972",
    owner_key: "example-app",
  });
  const short = shortRunId(publicRunId);

  it("uses a Temporal handle from the route without waiting on discovery", () => {
    expect(
      publicWorkflowRunIdForGetRun({
        routeRunId: publicRunId,
        resolvedId: publicRunId,
        discoveryExhausted: false,
      }),
    ).toBe(publicRunId);
  });

  it("prefers a list match over the short route segment", () => {
    expect(
      publicWorkflowRunIdForGetRun({
        routeRunId: short,
        resolvedId: short,
        listRunId: publicRunId,
        discoveryExhausted: false,
      }),
    ).toBe(publicRunId);
  });

  it("does not GetRun a short id while discovery is still paging", () => {
    expect(
      publicWorkflowRunIdForGetRun({
        routeRunId: short,
        resolvedId: short,
        discoveryExhausted: false,
      }),
    ).toBeNull();
  });

  it("falls back to the route id after discovery is exhausted", () => {
    expect(
      publicWorkflowRunIdForGetRun({
        routeRunId: "run_failed_1",
        resolvedId: "run_failed_1",
        discoveryExhausted: true,
      }),
    ).toBe("run_failed_1");
  });
});
