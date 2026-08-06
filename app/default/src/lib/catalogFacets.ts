import type { Integration } from "@/lib/api";
import {
  canManageApp,
  getAppSurfaces,
  matchesConnectionFilter,
  type ConnectionFilter,
  type SurfaceFilter,
} from "@/lib/catalogFilters";

/**
 * Apps catalog AND-facet model — one id per filter axis, owned here so the
 * page only binds ChipGroup UI to counts / selection.
 */
export type CatalogFacetId = "web_app" | "admin" | "needs_attention";

export type CatalogFacetCounts = Record<CatalogFacetId, number>;

export const CATALOG_FACETS: ReadonlyArray<{
  id: CatalogFacetId;
  label: string;
}> = [
  { id: "web_app", label: "Web App" },
  { id: "admin", label: "Admin" },
  { id: "needs_attention", label: "Needs attention" },
];

const CATALOG_FACET_IDS = new Set<string>(
  CATALOG_FACETS.map((facet) => facet.id),
);

export function isCatalogFacetId(value: string): value is CatalogFacetId {
  return CATALOG_FACET_IDS.has(value);
}

export function countCatalogFacets(
  integrations: Integration[],
): CatalogFacetCounts {
  let admin = 0;
  let webApp = 0;
  let needsAttention = 0;
  for (const integration of integrations) {
    if (canManageApp(integration)) admin += 1;
    if (getAppSurfaces(integration).hasUi) webApp += 1;
    if (matchesConnectionFilter(integration, "needs_attention")) {
      needsAttention += 1;
    }
  }
  return { admin, web_app: webApp, needs_attention: needsAttention };
}

/** Drop facets whose catalog count is zero so hidden chips cannot keep filtering. */
export function pruneActiveCatalogFacets(
  active: readonly CatalogFacetId[],
  counts: CatalogFacetCounts,
): CatalogFacetId[] {
  return active.filter((id) => counts[id] > 0);
}

export function catalogFacetsToFilterOptions(
  active: readonly CatalogFacetId[],
): {
  connection: ConnectionFilter;
  surface: SurfaceFilter;
  admin: boolean;
} {
  return {
    connection: active.includes("needs_attention") ? "needs_attention" : "all",
    surface: active.includes("web_app") ? "has_ui" : "all",
    admin: active.includes("admin"),
  };
}
