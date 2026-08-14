import { describe, expect, it } from "vitest";
import type { WorkflowRun } from "@/lib/api";
import {
  applyWorkflowRunsListQuery,
  emptyWorkflowRunsListQuery,
  parseWorkflowRunsSearch,
  serverListStatus,
  workflowDefinitionRunCountLabel,
  workflowListHasMorePages,
  workflowRunsListQueryFromSearch,
  workflowRunsListQueryIsActive,
  workflowRunsListQueryUsesClientOnlyFilters,
  workflowRunsSearchFromQuery,
  workflowRunsStatusFilterScope,
  workflowVisibleRunTotalCount,
} from "./workflow-runs-list-query";

function run(partial: Partial<WorkflowRun> & Pick<WorkflowRun, "id">): WorkflowRun {
  return {
    provider: "local",
    target: { steps: [] },
    status: "succeeded",
    ...partial,
  };
}

describe("workflow-runs-list-query", () => {
  it("round-trips search params with stable status order", () => {
    const query = workflowRunsListQueryFromSearch({
      q: " digest ",
      status: "failed,running,failed",
      definition: " app_demo_nightly ",
      group: "definition",
    });
    expect(query).toEqual({
      q: "digest",
      statuses: ["running", "failed"],
      definitionId: "app_demo_nightly",
      groupBy: "definition",
    });
    expect(workflowRunsSearchFromQuery(query)).toEqual({
      q: "digest",
      status: "running,failed",
      definition: "app_demo_nightly",
      group: "definition",
    });
  });

  it("parses empty search to an inactive flat query", () => {
    expect(parseWorkflowRunsSearch({})).toEqual({});
    expect(workflowRunsListQueryFromSearch({})).toEqual(
      emptyWorkflowRunsListQuery(),
    );
    expect(
      workflowRunsListQueryIsActive(emptyWorkflowRunsListQuery()),
    ).toBe(false);
  });

  it("treats groupBy as layout, not an active filter", () => {
    expect(
      workflowRunsListQueryIsActive({
        q: "",
        statuses: [],
        groupBy: "definition",
      }),
    ).toBe(false);
    expect(
      workflowRunsListQueryUsesClientOnlyFilters({
        q: "",
        statuses: [],
        groupBy: "definition",
      }),
    ).toBe(false);
    expect(
      workflowRunsSearchFromQuery({
        q: "",
        statuses: [],
        groupBy: "definition",
      }),
    ).toEqual({ group: "definition" });
  });

  it("uses server status only for a single selected status", () => {
    expect(
      serverListStatus({ q: "", statuses: ["failed"], groupBy: "none" }),
    ).toBe("failed");
    expect(
      serverListStatus({
        q: "",
        statuses: ["failed", "running"],
        groupBy: "none",
      }),
    ).toBeUndefined();
    expect(
      workflowRunsStatusFilterScope({
        q: "",
        statuses: [],
        groupBy: "none",
      }),
    ).toBe("none");
    expect(
      workflowRunsStatusFilterScope({
        q: "",
        statuses: ["running"],
        groupBy: "none",
      }),
    ).toBe("server");
    expect(
      workflowRunsStatusFilterScope({
        q: "",
        statuses: ["running", "failed"],
        groupBy: "none",
      }),
    ).toBe("loaded-only");
  });

  it("marks search and multi-status as client-only filters", () => {
    expect(
      workflowRunsListQueryUsesClientOnlyFilters({
        q: "",
        statuses: [],
        groupBy: "none",
      }),
    ).toBe(false);
    expect(
      workflowRunsListQueryUsesClientOnlyFilters({
        q: "",
        statuses: ["failed"],
        groupBy: "none",
      }),
    ).toBe(false);
    expect(
      workflowRunsListQueryUsesClientOnlyFilters({
        q: "incident",
        statuses: [],
        groupBy: "none",
      }),
    ).toBe(true);
    expect(
      workflowRunsListQueryUsesClientOnlyFilters({
        q: "",
        statuses: [],
        definitionId: "def_1",
        groupBy: "none",
      }),
    ).toBe(false);
    expect(
      workflowRunsListQueryUsesClientOnlyFilters({
        q: "",
        statuses: ["running", "failed"],
        groupBy: "none",
      }),
    ).toBe(true);
  });

  it("applies definition and search client-side", () => {
    const runs = [
      run({
        id: "a",
        status: "failed",
        definitionId: "app_demo_nightly",
      }),
      run({
        id: "b",
        status: "succeeded",
        definitionId: "app_demo_sync",
      }),
      run({
        id: "c",
        status: "failed",
        definitionId: "app_demo_sync",
        statusMessage: "digest timeout",
      }),
    ];
    expect(
      applyWorkflowRunsListQuery(runs, {
        q: "digest",
        statuses: ["failed"],
        definitionId: "app_demo_sync",
        groupBy: "none",
      }).map((item) => item.id),
    ).toEqual(["c"]);
  });

  it("ORs multiple statuses on the loaded corpus", () => {
    const runs = [
      run({ id: "a", status: "failed" }),
      run({ id: "b", status: "running" }),
      run({ id: "c", status: "succeeded" }),
    ];
    expect(
      applyWorkflowRunsListQuery(runs, {
        q: "",
        statuses: ["failed", "running"],
        groupBy: "none",
      }).map((item) => item.id),
    ).toEqual(["a", "b"]);
  });

  it("hides Load more on an empty page when the next-page token is a leak", () => {
    expect(
      workflowListHasMorePages({
        hasNextPage: true,
        loadedCount: 0,
        totalCount: 0,
      }),
    ).toBe(false);
    expect(
      workflowListHasMorePages({
        hasNextPage: true,
        loadedCount: 0,
      }),
    ).toBe(false);
    expect(
      workflowListHasMorePages({
        hasNextPage: true,
        loadedCount: 0,
        totalCount: 4,
      }),
    ).toBe(true);
    expect(
      workflowListHasMorePages({
        hasNextPage: true,
        loadedCount: 20,
        totalCount: 20,
      }),
    ).toBe(false);
    expect(
      workflowListHasMorePages({
        hasNextPage: true,
        loadedCount: 20,
      }),
    ).toBe(true);
  });

  it("labels definition run counts from the server total, not the page size", () => {
    expect(
      workflowDefinitionRunCountLabel({
        loading: true,
        loadedCount: 0,
        hasMore: false,
      }),
    ).toBe("…");
    expect(
      workflowDefinitionRunCountLabel({
        loading: false,
        loadedCount: 20,
        totalCount: 128,
        hasMore: true,
      }),
    ).toBe("128 runs");
    expect(
      workflowDefinitionRunCountLabel({
        loading: false,
        loadedCount: 1,
        totalCount: 1,
        hasMore: false,
      }),
    ).toBe("1 run");
    expect(
      workflowDefinitionRunCountLabel({
        loading: false,
        loadedCount: 0,
        hasMore: true,
      }),
    ).toBe("…");
    expect(
      workflowDefinitionRunCountLabel({
        loading: false,
        loadedCount: 20,
        hasMore: true,
      }),
    ).toBe("20+ runs");
    expect(
      workflowDefinitionRunCountLabel({
        loading: false,
        loadedCount: 7,
        hasMore: false,
      }),
    ).toBe("7 runs");
    expect(
      workflowVisibleRunTotalCount(
        { q: "digest", statuses: [], groupBy: "definition" },
        128,
      ),
    ).toBeUndefined();
    expect(
      workflowVisibleRunTotalCount(
        { q: "", statuses: ["failed"], groupBy: "definition" },
        128,
      ),
    ).toBe(128);
    expect(
      workflowDefinitionRunCountLabel({
        loading: false,
        loadedCount: 3,
        totalCount: workflowVisibleRunTotalCount(
          { q: "digest", statuses: [], groupBy: "definition" },
          128,
        ),
        hasMore: true,
      }),
    ).toBe("3+ runs");
  });
});
