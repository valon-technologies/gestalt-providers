import { describe, expect, it } from "vitest";
import { rewriteShortWorkflowRunPath } from "./use-resolved-workflow-run-route";
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
