import { useCallback, useEffect, useState } from "react";

/**
 * Session disclosure for grouped Runs: which definition sections the user
 * closed. Default is open. Collapsed ids are the exception, keyed by app, so
 * toggling Group by definition (which remounts the list) does not reset them.
 *
 * URL owns layout (`group=definition`) and filters. This store owns working-set
 * chrome — not shareable, not a durable preference.
 */
const memory = new Map<string, Set<string>>();

function storageKey(app: string): string {
  return `gestalt.workflowDefinitionGroupsCollapsed:${app}`;
}

function parseCollapsedIds(raw: string | null): Set<string> {
  if (!raw?.trim()) return new Set();
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return new Set();
    const ids = new Set<string>();
    for (const value of parsed) {
      if (typeof value !== "string") continue;
      const id = value.trim();
      if (id) ids.add(id);
    }
    return ids;
  } catch {
    return new Set();
  }
}

function persistCollapsedIds(app: string, ids: Set<string>): void {
  memory.set(app, ids);
  try {
    sessionStorage.setItem(storageKey(app), JSON.stringify([...ids]));
  } catch {
    // private mode / quota — in-memory set still survives remount in this tab
  }
}

export function readCollapsedWorkflowDefinitionIds(app: string): Set<string> {
  const key = app.trim();
  if (!key) return new Set();
  const cached = memory.get(key);
  if (cached) return new Set(cached);
  try {
    const ids = parseCollapsedIds(sessionStorage.getItem(storageKey(key)));
    memory.set(key, ids);
    return new Set(ids);
  } catch {
    const empty = new Set<string>();
    memory.set(key, empty);
    return new Set();
  }
}

export function isWorkflowDefinitionGroupOpen(
  app: string,
  definitionId: string,
): boolean {
  const id = definitionId.trim();
  if (!id) return true;
  return !readCollapsedWorkflowDefinitionIds(app).has(id);
}

export function setWorkflowDefinitionGroupOpen(
  app: string,
  definitionId: string,
  open: boolean,
): void {
  const key = app.trim();
  const id = definitionId.trim();
  if (!key || !id) return;
  const next = readCollapsedWorkflowDefinitionIds(key);
  if (open) next.delete(id);
  else next.add(id);
  persistCollapsedIds(key, next);
}

/** Test helper — clears memory and session keys for this module. */
export function resetWorkflowDefinitionGroupDisclosure(): void {
  for (const app of memory.keys()) {
    try {
      sessionStorage.removeItem(storageKey(app));
    } catch {
      // ignore
    }
  }
  memory.clear();
}

/** Test helper — drop the in-process cache so the next read hits sessionStorage. */
export function forgetWorkflowDefinitionGroupDisclosureMemory(): void {
  memory.clear();
}

export function useWorkflowDefinitionGroupOpen(
  app: string,
  definitionId: string,
): [boolean, (open: boolean) => void] {
  const [open, setOpenState] = useState(() =>
    isWorkflowDefinitionGroupOpen(app, definitionId),
  );

  useEffect(() => {
    setOpenState(isWorkflowDefinitionGroupOpen(app, definitionId));
  }, [app, definitionId]);

  const setOpen = useCallback(
    (next: boolean) => {
      setWorkflowDefinitionGroupOpen(app, definitionId, next);
      setOpenState(next);
    },
    [app, definitionId],
  );

  return [open, setOpen];
}
