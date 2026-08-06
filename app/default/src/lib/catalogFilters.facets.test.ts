import { describe, expect, test } from "vitest";

import { filterCatalogIntegrations } from "./catalogFilters";
import type { Integration } from "@/lib/api";

function stub(
  partial: Partial<Integration> & Pick<Integration, "name">,
): Integration {
  return {
    displayName: partial.name,
    description: "",
    ...partial,
  };
}

const catalog: Integration[] = [
  stub({
    name: "admin-ui",
    displayName: "Admin UI",
    mountedPath: "/admin-ui",
    managementPath: "/apps/admin-ui/admin",
    status: "ready",
    credentialState: "not_required",
    connections: [],
  }),
  stub({
    name: "plain-ui",
    displayName: "Plain UI",
    mountedPath: "/plain-ui",
    status: "ready",
    credentialState: "not_required",
    connections: [],
  }),
  stub({
    name: "broken-oauth",
    displayName: "Broken OAuth",
    status: "degraded",
    credentialState: "invalid",
    connections: [
      {
        name: "default",
        status: "degraded",
        credentialState: "invalid",
        authTypes: ["oauth"],
        actions: ["reconnect"],
      },
    ],
  }),
  stub({
    name: "admin-broken",
    displayName: "Admin Broken",
    managementPath: "/apps/admin-broken/admin",
    status: "degraded",
    credentialState: "invalid",
    connections: [
      {
        name: "default",
        status: "degraded",
        credentialState: "invalid",
        authTypes: ["oauth"],
        actions: ["reconnect"],
      },
    ],
  }),
];

describe("filterCatalogIntegrations facets", () => {
  test("admin facet keeps only manageable apps", () => {
    const result = filterCatalogIntegrations(catalog, {
      query: "",
      connection: "all",
      surface: "all",
      admin: true,
    });
    expect(result.map((app) => app.name)).toEqual([
      "admin-broken",
      "admin-ui",
    ]);
  });

  test("admin false/omitted does not restrict by managementPath", () => {
    const omitted = filterCatalogIntegrations(catalog, {
      query: "",
      connection: "all",
      surface: "all",
    });
    const explicit = filterCatalogIntegrations(catalog, {
      query: "",
      connection: "all",
      surface: "all",
      admin: false,
    });
    expect(omitted.map((app) => app.name)).toEqual(
      explicit.map((app) => app.name),
    );
    expect(omitted).toHaveLength(4);
  });

  test("ANDs admin with surface and connection facets", () => {
    const adminUi = filterCatalogIntegrations(catalog, {
      query: "",
      connection: "all",
      surface: "has_ui",
      admin: true,
    });
    expect(adminUi.map((app) => app.name)).toEqual(["admin-ui"]);

    const adminAttention = filterCatalogIntegrations(catalog, {
      query: "",
      connection: "needs_attention",
      surface: "all",
      admin: true,
    });
    expect(adminAttention.map((app) => app.name)).toEqual(["admin-broken"]);
  });

  test("ANDs facets with search query", () => {
    const result = filterCatalogIntegrations(catalog, {
      query: "admin",
      connection: "all",
      surface: "all",
      admin: true,
    });
    expect(result.map((app) => app.name)).toEqual([
      "admin-broken",
      "admin-ui",
    ]);

    const noMatch = filterCatalogIntegrations(catalog, {
      query: "plain",
      connection: "all",
      surface: "all",
      admin: true,
    });
    expect(noMatch).toEqual([]);
  });
});
