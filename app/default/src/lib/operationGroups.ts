import type { IntegrationOperation } from "@/lib/api";
import {
  getCatalogSearchHaystack,
  matchesCatalogSearchQuery,
  toCatalogEntry,
} from "@/features/app-workspace/operations/catalog";

export type OperationResourceGroup = {
  prefix: string;
  label: string;
  sectionId: string;
  operations: IntegrationOperation[];
};

/** First segment of an operation id (`issues.list` → `issues`). */
export function operationResourcePrefix(operationId: string): string {
  const dot = operationId.indexOf(".");
  return dot === -1 ? operationId : operationId.slice(0, dot);
}

/** DOM id for a resource group heading on the operations page. */
export function operationResourceSectionId(prefix: string): string {
  const safe = prefix.replace(/[^a-zA-Z0-9_-]/g, "-");
  return `ops-resource-${safe}`;
}

/** Human label for a resource prefix in section headings and TOC. */
export function formatOperationResourceLabel(prefix: string): string {
  const spaced = prefix
    .replace(/_/g, " ")
    .replace(/([a-z])([A-Z])/g, "$1 $2")
    .trim();
  if (!spaced) return prefix;
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

export function groupOperationsByResource(
  operations: IntegrationOperation[],
): OperationResourceGroup[] {
  const byPrefix = new Map<string, IntegrationOperation[]>();

  for (const operation of operations) {
    const prefix = operationResourcePrefix(operation.id);
    const bucket = byPrefix.get(prefix);
    if (bucket) {
      bucket.push(operation);
    } else {
      byPrefix.set(prefix, [operation]);
    }
  }

  return [...byPrefix.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([prefix, ops]) => ({
      prefix,
      label: formatOperationResourceLabel(prefix),
      sectionId: operationResourceSectionId(prefix),
      operations: [...ops].sort((a, b) => a.id.localeCompare(b.id)),
    }));
}

/** Delegates to the catalog presentation haystack (displayable fields only). */
export function getOperationSearchHaystack(operation: IntegrationOperation): string {
  return getCatalogSearchHaystack(toCatalogEntry(operation));
}

export function matchesOperationSearchQuery(
  operation: IntegrationOperation,
  rawQuery: string,
): boolean {
  return matchesCatalogSearchQuery(toCatalogEntry(operation), rawQuery);
}

export function filterOperations(
  operations: IntegrationOperation[],
  query: string,
): IntegrationOperation[] {
  const trimmed = query.trim();
  if (!trimmed) return operations;

  return operations.filter((operation) =>
    matchesOperationSearchQuery(operation, trimmed),
  );
}
