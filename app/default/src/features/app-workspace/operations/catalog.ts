import type { IntegrationOperation } from "@/lib/api";
import { textContainsAllSearchTokens } from "@/lib/search-highlight";

/**
 * Presentation projection for the App Operation Catalog.
 * Searchable fields must be displayable (or explicitly labeled secondary).
 */
export type OperationCatalogEntry = {
  id: string;
  /** Human title when it differs from `id`; otherwise null. */
  title: string | null;
  description: string | null;
  method: string | null;
  rolesLabel: string | null;
  readOnly: boolean;
  /** Shown as secondary reference under the description when present. */
  path: string | null;
  source: IntegrationOperation;
};

export function isCatalogVisibleOperation(
  operation: IntegrationOperation,
): boolean {
  return operation.visible !== false && typeof operation.id === "string" && Boolean(operation.id);
}

export function toCatalogEntry(
  operation: IntegrationOperation,
): OperationCatalogEntry {
  const id = operation.id;
  const rawTitle = operation.title?.trim() || null;
  const title = rawTitle && rawTitle !== id ? rawTitle : null;
  const description = operation.description?.trim() || null;
  const method = operation.method?.trim() || null;
  const rolesLabel =
    operation.allowedRoles && operation.allowedRoles.length > 0
      ? operation.allowedRoles.join(", ")
      : null;
  const path = operation.path?.trim() || null;

  return {
    id,
    title,
    description,
    method,
    rolesLabel,
    readOnly: Boolean(operation.readOnly),
    path,
    source: operation,
  };
}

/** Haystack derived only from fields the catalog surfaces. */
export function getCatalogSearchHaystack(entry: OperationCatalogEntry): string {
  return [
    entry.id,
    entry.title,
    entry.description,
    entry.method,
    entry.rolesLabel,
    entry.path,
    entry.readOnly ? "read-only" : null,
  ]
    .filter(Boolean)
    .join(" ");
}

export function matchesCatalogSearchQuery(
  entry: OperationCatalogEntry,
  rawQuery: string,
): boolean {
  return textContainsAllSearchTokens(getCatalogSearchHaystack(entry), rawQuery);
}

export function filterCatalogEntries(
  entries: OperationCatalogEntry[],
  query: string,
): OperationCatalogEntry[] {
  const trimmed = query.trim();
  if (!trimmed) return entries;
  return entries.filter((entry) => matchesCatalogSearchQuery(entry, trimmed));
}

export function catalogEntriesFromOperations(
  operations: IntegrationOperation[],
): OperationCatalogEntry[] {
  return operations.filter(isCatalogVisibleOperation).map(toCatalogEntry);
}
