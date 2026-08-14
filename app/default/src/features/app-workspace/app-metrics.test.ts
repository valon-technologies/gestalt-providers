import { describe, expect, it } from "vitest";
import {
  appMetricsIsEmpty,
  averageDurationMs,
  formatAverageDuration,
  formatMetricCount,
} from "./app-metrics";
import { APP_METRICS_PAGE_DESCRIPTION } from "./app-metrics-copy";

describe("app metrics formatting", () => {
  it("says this is traffic since the server started", () => {
    expect(APP_METRICS_PAGE_DESCRIPTION).toMatch(/since it started/);
  });
  it("returns null average when there is no duration count", () => {
    expect(averageDurationMs(1.5, 0)).toBeNull();
    expect(formatAverageDuration(1.5, 0)).toBe("No data");
  });

  it("formats latency in milliseconds or seconds", () => {
    expect(formatAverageDuration(0.012, 1)).toBe("12.0 ms");
    expect(formatAverageDuration(2.5, 1)).toBe("2.50 s");
  });

  it("formats counts and empty traffic", () => {
    expect(formatMetricCount(12)).toBe("12");
    expect(appMetricsIsEmpty({ requests: 0, operations: [] })).toBe(true);
    expect(appMetricsIsEmpty({ requests: 1, operations: [] })).toBe(false);
  });
});
