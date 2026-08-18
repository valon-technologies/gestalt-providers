import { describe, expect, it } from "vitest";

import { APIError, APITimeoutError, type Integration } from "@/lib/api";
import {
  appsCatalogQueryStatus,
  connectionOverlayKnown,
  shouldRetryAppsCatalogQuery,
  workspaceConnectionView,
  workspaceIntegrationsPending,
} from "@/lib/queries/integrations";

const cached: Integration[] = [{ name: "example-app", displayName: "Example App" }];

describe("appsCatalogQueryStatus", () => {
  it("is loading when the query has not settled", () => {
    expect(
      appsCatalogQueryStatus({
        isPending: true,
        error: null,
        data: undefined,
      }),
    ).toEqual({ status: "loading", integrations: [] });
  });

  it("is ready when the query succeeded", () => {
    expect(
      appsCatalogQueryStatus({
        isPending: false,
        error: null,
        data: cached,
      }),
    ).toEqual({ status: "ready", integrations: cached });
  });

  it("is unavailable without hiding cached apps", () => {
    const error = new APIError(503, "Service Unavailable");
    expect(
      appsCatalogQueryStatus({
        isPending: false,
        error,
        data: cached,
      }),
    ).toEqual({ status: "unavailable", error, integrations: cached });
  });

  it("is unavailable with an empty catalog on first failure", () => {
    const error = new APITimeoutError();
    expect(
      appsCatalogQueryStatus({
        isPending: false,
        error,
        data: undefined,
      }),
    ).toEqual({ status: "unavailable", error, integrations: [] });
  });
});

describe("shouldRetryAppsCatalogQuery", () => {
  it("does not auto-retry timeouts or 503s", () => {
    expect(shouldRetryAppsCatalogQuery(0, new APITimeoutError())).toBe(false);
    expect(
      shouldRetryAppsCatalogQuery(0, new APIError(503, "Service Unavailable")),
    ).toBe(false);
  });

  it("retries other failures once", () => {
    const transient = new APIError(500, "Internal Server Error");
    expect(shouldRetryAppsCatalogQuery(0, transient)).toBe(true);
    expect(shouldRetryAppsCatalogQuery(1, transient)).toBe(false);
  });
});

describe("workspaceIntegrationsPending", () => {
  it("holds the app workspace until overlay status arrives", () => {
    expect(workspaceIntegrationsPending(true, false)).toBe(true);
    expect(workspaceIntegrationsPending(false, true)).toBe(true);
    expect(workspaceIntegrationsPending(false, false)).toBe(false);
  });
});

describe("connectionOverlayKnown", () => {
  it("is known for composed listings that already include status", () => {
    expect(connectionOverlayKnown(false, false, new Error("ignored"))).toBe(
      true,
    );
  });

  it("is unknown while the catalog overlay is pending or failed", () => {
    expect(connectionOverlayKnown(true, true, null)).toBe(false);
    expect(connectionOverlayKnown(true, false, new APIError(503, "down"))).toBe(
      false,
    );
    expect(connectionOverlayKnown(true, false, null)).toBe(true);
  });
});

describe("workspaceConnectionView", () => {
  it("loads until directory and overlay settle", () => {
    expect(
      workspaceConnectionView({
        directoryPending: false,
        overlayPending: true,
        overlayError: null,
      }),
    ).toEqual({ status: "loading" });
  });

  it("blocks status surfaces when overlay fails", () => {
    const error = new APIError(503, "Service Unavailable");
    expect(
      workspaceConnectionView({
        directoryPending: false,
        overlayPending: false,
        overlayError: error,
      }),
    ).toEqual({ status: "overlay_unavailable", error });
  });

  it("is ready when overlay succeeded or is not used", () => {
    expect(
      workspaceConnectionView({
        directoryPending: false,
        overlayPending: false,
        overlayError: null,
      }),
    ).toEqual({ status: "ready" });
  });
});
