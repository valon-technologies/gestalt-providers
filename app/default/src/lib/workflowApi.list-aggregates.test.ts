import { describe, expect, it } from "vitest";
import {
  normalizeStatusCounts,
  normalizeWorkflowRunListResponse,
  parseOptionalCount,
  pickWorkflowRunListAggregates,
} from "@/lib/workflowApi";

describe("parseOptionalCount", () => {
  it("accepts finite numbers and protojson int64 strings", () => {
    expect(parseOptionalCount(0)).toBe(0);
    expect(parseOptionalCount(42)).toBe(42);
    expect(parseOptionalCount("0")).toBe(0);
    expect(parseOptionalCount("17")).toBe(17);
  });

  it("rejects empty, non-numeric, and non-finite values", () => {
    expect(parseOptionalCount(undefined)).toBeUndefined();
    expect(parseOptionalCount(null)).toBeUndefined();
    expect(parseOptionalCount("")).toBeUndefined();
    expect(parseOptionalCount("  ")).toBeUndefined();
    expect(parseOptionalCount("nope")).toBeUndefined();
    expect(parseOptionalCount(Number.NaN)).toBeUndefined();
    expect(parseOptionalCount(Number.POSITIVE_INFINITY)).toBeUndefined();
  });
});

describe("normalizeWorkflowRunListResponse aggregates", () => {
  it("coerces string totals and status histogram fields", () => {
    const page = normalizeWorkflowRunListResponse({
      runs: [],
      nextPageToken: " next ",
      totalCount: "128",
      statusCounts: {
        pending: "1",
        running: "2",
        succeeded: "100",
        failed: "20",
        canceled: "5",
      },
    });
    expect(page.nextPageToken).toBe("next");
    expect(page.totalCount).toBe(128);
    expect(page.statusCounts).toEqual({
      pending: 1,
      running: 2,
      succeeded: 100,
      failed: 20,
      canceled: 5,
    });
  });

  it("reads proto-name aggregate fields", () => {
    const page = normalizeWorkflowRunListResponse({
      runs: [],
      next_page_token: "cursor",
      total_count: "64",
      status_counts: {
        pending: "0",
        running: "1",
        succeeded: "60",
        failed: "3",
        canceled: "0",
      },
    });
    expect(page.nextPageToken).toBe("cursor");
    expect(page.totalCount).toBe(64);
    expect(page.statusCounts).toEqual({
      pending: 0,
      running: 1,
      succeeded: 60,
      failed: 3,
      canceled: 0,
    });
  });

  it("omits aggregates when the API leaves them out", () => {
    const page = normalizeWorkflowRunListResponse({ runs: [] });
    expect(page.totalCount).toBeUndefined();
    expect(page.statusCounts).toBeUndefined();
  });

  it("keeps an all-zero statusCounts as a known empty histogram", () => {
    expect(normalizeStatusCounts({})).toEqual({
      pending: 0,
      running: 0,
      succeeded: 0,
      failed: 0,
      canceled: 0,
    });
  });
});

describe("pickWorkflowRunListAggregates", () => {
  it("uses the first page that carries totals or status counts", () => {
    expect(
      pickWorkflowRunListAggregates([
        {},
        { totalCount: 9 },
        { totalCount: 1 },
      ]),
    ).toEqual({ totalCount: 9, statusCounts: undefined });

    expect(
      pickWorkflowRunListAggregates([
        {},
        {
          statusCounts: {
            pending: 0,
            running: 1,
            succeeded: 0,
            failed: 0,
            canceled: 0,
          },
        },
      ]),
    ).toEqual({
      totalCount: undefined,
      statusCounts: {
        pending: 0,
        running: 1,
        succeeded: 0,
        failed: 0,
        canceled: 0,
      },
    });
  });
});
