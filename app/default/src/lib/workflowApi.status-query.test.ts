import { describe, expect, it } from "vitest";
import {
  listWorkflowRunsQuery,
  toWorkflowRunStatusQueryParam,
} from "@/lib/workflowApi";

describe("toWorkflowRunStatusQueryParam", () => {
  it("maps product status tokens to protobuf enum names", () => {
    expect(toWorkflowRunStatusQueryParam("failed")).toBe(
      "WORKFLOW_RUN_STATUS_FAILED",
    );
    expect(toWorkflowRunStatusQueryParam("running")).toBe(
      "WORKFLOW_RUN_STATUS_RUNNING",
    );
    expect(toWorkflowRunStatusQueryParam("SUCCEEDED")).toBe(
      "WORKFLOW_RUN_STATUS_SUCCEEDED",
    );
  });

  it("passes through already-encoded enum names", () => {
    expect(
      toWorkflowRunStatusQueryParam("WORKFLOW_RUN_STATUS_CANCELED"),
    ).toBe("WORKFLOW_RUN_STATUS_CANCELED");
  });
});

describe("listWorkflowRunsQuery", () => {
  it("includes definitionId when set", () => {
    const params = new URLSearchParams(
      listWorkflowRunsQuery("temporal", {
        targetApp: "demo",
        status: "failed",
        definitionId: "app_demo_nightly",
      }),
    );
    expect(params.get("provider")).toBe("temporal");
    expect(params.get("targetApp")).toBe("demo");
    expect(params.get("status")).toBe("WORKFLOW_RUN_STATUS_FAILED");
    expect(params.get("definitionId")).toBe("app_demo_nightly");
  });

  it("omits blank definitionId", () => {
    const params = new URLSearchParams(
      listWorkflowRunsQuery("temporal", {
        targetApp: "demo",
        definitionId: "  ",
      }),
    );
    expect(params.has("definitionId")).toBe(false);
  });
});
