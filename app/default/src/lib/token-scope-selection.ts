import type { CheckboxTreeNode } from "@/components/ui/checkbox-tree";
import type { IntegrationOperation } from "@/lib/api";

/**
 * App selection while operations may still be lazy-loading.
 *
 * CheckboxTree shows empty `children: []` as a folder (Gestalt fork) so +/-
 * appears before ops arrive; leaf helpers treat `[]` as a selectable bare app
 * id until real children load. This module owns that bridge.
 */
export type SelectedAppState = {
  /** When true (default), encode bare appId. When false, encode checked ops only. */
  allOperations: boolean;
  /** Checked operation ids when allOperations is false. */
  operationIds: Set<string>;
};

export type OpsByApp = Record<
  string,
  IntegrationOperation[] | "loading" | "error"
>;

/** Scope grammar leaf: `app:operation` — matches encodeTokenScopes. */
export function operationLeafId(appName: string, operationId: string): string {
  return `${appName}:${operationId}`;
}

export function parseOperationLeafId(
  leafId: string,
): { appName: string; operationId: string } | null {
  const sep = leafId.indexOf(":");
  if (sep <= 0) return null;
  return {
    appName: leafId.slice(0, sep),
    operationId: leafId.slice(sep + 1),
  };
}

export function listedOperations(
  opsState: IntegrationOperation[] | "loading" | "error" | undefined,
): IntegrationOperation[] | null {
  return Array.isArray(opsState) && opsState.length > 0 ? opsState : null;
}

/** Build CheckboxTree nodes for the searchable app catalog. */
export function buildCatalogAccessTree(
  filteredApps: readonly { name: string; displayName?: string }[],
  opsByApp: OpsByApp,
): CheckboxTreeNode[] {
  return filteredApps.map((app) => {
    const label = app.displayName?.trim() || app.name;
    const ops = listedOperations(opsByApp[app.name]);
    if (!ops) {
      // Empty `children` keeps folder +/- chrome before operations load.
      return { id: app.name, label, children: [] };
    }
    return {
      id: app.name,
      label,
      children: ops.map((op) => ({
        id: operationLeafId(app.name, op.id),
        label: op.title?.trim() || op.id,
      })),
    };
  });
}

/** Derive CheckboxTree leaf value from SelectedAppState + loaded ops. */
export function leafValueFromSelectedApps(
  selectedApps: Record<string, SelectedAppState>,
  opsByApp: OpsByApp,
): string[] {
  const leaves: string[] = [];
  for (const [appName, state] of Object.entries(selectedApps)) {
    const ops = listedOperations(opsByApp[appName]);
    if (!ops) {
      leaves.push(appName);
      continue;
    }
    if (state.allOperations) {
      for (const op of ops) {
        leaves.push(operationLeafId(appName, op.id));
      }
      continue;
    }
    for (const opId of state.operationIds) {
      leaves.push(operationLeafId(appName, opId));
    }
  }
  return leaves;
}

/**
 * Map CheckboxTree leaf ids back to SelectedAppState.
 * Unchecking every leaf under an app removes it from the selection set.
 */
export function selectedAppsFromLeafValue(
  leafIds: readonly string[],
  previous: Record<string, SelectedAppState>,
  opsByApp: OpsByApp,
): Record<string, SelectedAppState> {
  const byApp = new Map<string, Set<string>>();
  const bareApps = new Set<string>();

  for (const leafId of leafIds) {
    const parsed = parseOperationLeafId(leafId);
    if (parsed) {
      const set = byApp.get(parsed.appName) ?? new Set<string>();
      set.add(parsed.operationId);
      byApp.set(parsed.appName, set);
      continue;
    }
    bareApps.add(leafId);
  }

  const next: Record<string, SelectedAppState> = {};

  for (const appName of bareApps) {
    next[appName] = previous[appName] ?? {
      allOperations: true,
      operationIds: new Set(),
    };
    if (!listedOperations(opsByApp[appName])) {
      next[appName] = { allOperations: true, operationIds: new Set() };
    }
  }

  for (const [appName, operationIds] of byApp) {
    const ops = listedOperations(opsByApp[appName]);
    if (!ops) {
      next[appName] = { allOperations: true, operationIds: new Set() };
      continue;
    }
    const allSelected =
      ops.length > 0 && ops.every((op) => operationIds.has(op.id));
    next[appName] = allSelected
      ? { allOperations: true, operationIds: new Set() }
      : { allOperations: false, operationIds };
  }

  return next;
}
