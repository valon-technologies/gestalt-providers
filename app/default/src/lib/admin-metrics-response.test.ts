import { describe, expect, it } from "vitest";
import { isAdminMetricsScrapeText } from "./admin-metrics-response";

describe("isAdminMetricsScrapeText", () => {
  it("accepts Prometheus text", () => {
    expect(
      isAdminMetricsScrapeText(
        "text/plain; version=0.0.4; charset=utf-8",
        "# HELP gestaltd_up 1\ngestaltd_up 1\n",
      ),
    ).toBe(true);
  });

  it("rejects the product SPA HTML fallback", () => {
    expect(
      isAdminMetricsScrapeText(
        "text/html; charset=utf-8",
        "<!doctype html>\n<html lang=\"en\">\n",
      ),
    ).toBe(false);
    expect(
      isAdminMetricsScrapeText("text/plain", "<!doctype html>\n<html lang=\"en\">"),
    ).toBe(false);
  });
});
