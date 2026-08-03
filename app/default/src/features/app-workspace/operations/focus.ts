export type OperationFocus =
  | { status: "idle" }
  | { status: "pending"; operationId: string }
  | { status: "matched"; operationId: string; sectionId: string }
  | { status: "unknown"; operationId: string }
  | {
      status: "hidden";
      operationId: string;
      reason: "filtered";
    };

export type ResolveOperationFocusInput = {
  hash: string | null;
  loading: boolean;
  /** Visible catalog operation ids (already filtered by `visible !== false`). */
  visibleIds: ReadonlySet<string>;
  /** Ids remaining after the active search filter. */
  filteredIds: ReadonlySet<string>;
  sectionIdForOperation: (operationId: string) => string;
};

/**
 * Pure deep-link resolver for the operations catalog.
 * Callers must not silently ignore unknown/hidden outcomes.
 */
export function resolveOperationFocus({
  hash,
  loading,
  visibleIds,
  filteredIds,
  sectionIdForOperation,
}: ResolveOperationFocusInput): OperationFocus {
  const operationId = hash?.trim() || null;
  if (!operationId) return { status: "idle" };

  if (loading) {
    return { status: "pending", operationId };
  }

  if (!visibleIds.has(operationId)) {
    return { status: "unknown", operationId };
  }

  if (!filteredIds.has(operationId)) {
    return { status: "hidden", operationId, reason: "filtered" };
  }

  return {
    status: "matched",
    operationId,
    sectionId: sectionIdForOperation(operationId),
  };
}

export function readOperationHash(
  rawHash: string = typeof window !== "undefined" ? window.location.hash : "",
): string | null {
  const withoutHash = rawHash.replace(/^#/, "");
  if (!withoutHash) return null;
  try {
    return decodeURIComponent(withoutHash);
  } catch {
    return withoutHash;
  }
}
