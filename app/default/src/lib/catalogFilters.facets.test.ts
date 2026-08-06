import { describe, expect, test } from "vitest";

import {
  catalogFacetsToFilterOptions,
  countCatalogFacets,
  pruneActiveCatalogFacets,
} from "./catalogFacets";
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

  test("web_app facet keeps only mounted UI apps", () => {
    const result = filterCatalogIntegrations(catalog, {
      query: "",
      ...catalogFacetsToFilterOptions(["web_app"]),
    });
    expect(result.map((app) => app.name)).toEqual(["admin-ui", "plain-ui"]);
  });

  test("needs_attention facet keeps only attention apps", () => {
    const result = filterCatalogIntegrations(catalog, {
      query: "",
      ...catalogFacetsToFilterOptions(["needs_attention"]),
    });
    expect(result.map((app) => app.name)).toEqual([
      "admin-broken",
      "broken-oauth",
    ]);
  });

  test("ANDs admin with surface and connection facets", () => {
    const adminUi = filterCatalogIntegrations(catalog, {
      query: "",
      ...catalogFacetsToFilterOptions(["web_app", "admin"]),
    });
    expect(adminUi.map((app) => app.name)).toEqual(["admin-ui"]);

    const adminAttention = filterCatalogIntegrations(catalog, {
      query: "",
      ...catalogFacetsToFilterOptions(["needs_attention", "admin"]),
    });
    expect(adminAttention.map((app) => app.name)).toEqual(["admin-broken"]);
  });

  test("ANDs facets with search query", () => {
    const result = filterCatalogIntegrations(catalog, {
      query: "admin",
      ...catalogFacetsToFilterOptions(["admin"]),
    });
    expect(result.map((app) => app.name)).toEqual([
      "admin-broken",
      "admin-ui",
    ]);

    const noMatch = filterCatalogIntegrations(catalog, {
      query: "plain",
      ...catalogFacetsToFilterOptions(["admin"]),
    });
    expect(noMatch).toEqual([]);
  });
});

describe("catalogFacets model", () => {
  test("counts each facet axis independently", () => {
    expect(countCatalogFacets(catalog)).toEqual({
      web_app: 2,
      admin: 2,
      needs_attention: 2,
    });
  });

  test("prunes active facets whose counts drop to zero", () => {
    expect(
      pruneActiveCatalogFacets(
        ["web_app", "admin", "needs_attention"],
        { web_app: 2, admin: 0, needs_attention: 1 },
      ),
    ).toEqual(["web_app", "needs_attention"]);
  });
});
