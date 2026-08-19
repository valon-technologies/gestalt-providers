import { QueryClient } from "@tanstack/react-query";
import { describe, expect, it } from "vitest";

import {
  APIError,
  APITimeoutError,
  type AppConnectionStatus,
  type AppsDirectory,
  type Integration,
} from "@/lib/api";
import {
  appsCatalogQueryStatus,
  commitIntegrationDisconnect,
  connectionOverlayKnown,
  shouldRetryAppsCatalogQuery,
  workspaceConnectionView,
  workspaceIntegrationsPending,
} from "@/lib/queries/integrations";
import { queryKeys } from "@/lib/query-keys";

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

describe("commitIntegrationDisconnect", () => {
  const overlay: AppConnectionStatus[] = [
    {
      name: "gmail",
      connected: true,
      connections: [
        {
          name: "default",
          connected: true,
          instances: [{ name: "work", connection: "default", preferred: true }],
        },
      ],
    },
  ];

  it("writes the disconnect into overlay and leaves a catalog directory untouched", async () => {
    const queryClient = new QueryClient();
    const directory: AppsDirectory = {
      source: "catalog",
      entries: [{ name: "gmail", displayName: "Gmail" }],
    };
    queryClient.setQueryData(queryKeys.integrations.connections(), overlay);
    queryClient.setQueryData(queryKeys.integrations.directory(), directory);

    await commitIntegrationDisconnect(queryClient, "gmail", {
      instance: "work",
      connection: "default",
    });

    expect(
      queryClient.getQueryData<AppConnectionStatus[]>(
        queryKeys.integrations.connections(),
      )?.[0],
    ).toMatchObject({ connected: false, connections: [{ instances: [] }] });
    expect(
      queryClient.getQueryData<AppsDirectory>(queryKeys.integrations.directory()),
    ).toBe(directory);
  });

  it("rewrites composed listings that still carry connection status", async () => {
    const queryClient = new QueryClient();
    const gmail: Integration = {
      name: "gmail",
      connections: [
        {
          name: "default",
          connected: true,
          instances: [{ name: "work", connection: "default" }],
        },
      ],
    };
    queryClient.setQueryData<AppsDirectory>(queryKeys.integrations.directory(), {
      source: "composed",
      entries: [{ name: "gmail" }],
      integrations: [gmail],
    });

    await commitIntegrationDisconnect(queryClient, "gmail", {
      instance: "work",
      connection: "default",
    });

    const next = queryClient.getQueryData<AppsDirectory>(
      queryKeys.integrations.directory(),
    );
    expect(next?.source).toBe("composed");
    if (next?.source !== "composed") {
      throw new Error("expected composed directory");
    }
    expect(next.integrations[0]?.connections?.[0]?.instances).toEqual([]);
  });
});
