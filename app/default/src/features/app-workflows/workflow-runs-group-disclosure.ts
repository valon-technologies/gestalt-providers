import { useCallback, useEffect, useState } from "react";

/**
 * Session disclosure for grouped Runs. URL owns layout (`group=definition`)
 * and filters. This store owns working-set chrome — not shareable.
 *
 * Default: definitions with activity start open; dormant inventory starts
 * closed. Explicit toggles are stored as expanded/collapsed overrides.
 */
const memory = new Map<string, DisclosureState>();

type DisclosureState = {
  collapsed: Set<string>;
  expanded: Set<string>;
};

function emptyDisclosure(): DisclosureState {
  return { collapsed: new Set(), expanded: new Set() };
}

function storageKey(app: string): string {
  return `gestalt.workflowDefinitionGroupsCollapsed:${app}`;
}

function parseIdList(value: unknown): Set<string> {
  if (!Array.isArray(value)) return new Set();
  const ids = new Set<string>();
  for (const item of value) {
    if (typeof item !== "string") continue;
    const id = item.trim();
    if (id) ids.add(id);
  }
  return ids;
}

function parseDisclosure(raw: string | null): DisclosureState {
  if (!raw?.trim()) return emptyDisclosure();
  try {
    const parsed: unknown = JSON.parse(raw);
    if (Array.isArray(parsed)) {
      return { collapsed: parseIdList(parsed), expanded: new Set() };
    }
    if (parsed && typeof parsed === "object") {
      const record = parsed as { collapsed?: unknown; expanded?: unknown };
      return {
        collapsed: parseIdList(record.collapsed),
        expanded: parseIdList(record.expanded),
      };
    }
  } catch {
    // ignore corrupt session payload
  }
  return emptyDisclosure();
}

function persistDisclosure(app: string, state: DisclosureState): void {
  memory.set(app, state);
  try {
    sessionStorage.setItem(
      storageKey(app),
      JSON.stringify({
        collapsed: [...state.collapsed],
        expanded: [...state.expanded],
      }),
    );
  } catch {
    // private mode / quota — in-memory set still survives remount in this tab
  }
}

function readDisclosure(app: string): DisclosureState {
  const key = app.trim();
  if (!key) return emptyDisclosure();
  const cached = memory.get(key);
  if (cached) {
    return {
      collapsed: new Set(cached.collapsed),
      expanded: new Set(cached.expanded),
    };
  }
  try {
    const state = parseDisclosure(sessionStorage.getItem(storageKey(key)));
    memory.set(key, state);
    return {
      collapsed: new Set(state.collapsed),
      expanded: new Set(state.expanded),
    };
  } catch {
    const empty = emptyDisclosure();
    memory.set(key, empty);
    return emptyDisclosure();
  }
}

export function readCollapsedWorkflowDefinitionIds(app: string): Set<string> {
  return readDisclosure(app).collapsed;
}

export function isWorkflowDefinitionGroupOpen(
  app: string,
  definitionId: string,
  defaultOpen = true,
): boolean {
  const id = definitionId.trim();
  if (!id) return true;
  const state = readDisclosure(app);
  if (state.expanded.has(id)) return true;
  if (state.collapsed.has(id)) return false;
  return defaultOpen;
}

export function setWorkflowDefinitionGroupOpen(
  app: string,
  definitionId: string,
  open: boolean,
): void {
  const key = app.trim();
  const id = definitionId.trim();
  if (!key || !id) return;
  const next = readDisclosure(key);
  if (open) {
    next.expanded.add(id);
    next.collapsed.delete(id);
  } else {
    next.collapsed.add(id);
    next.expanded.delete(id);
  }
  persistDisclosure(key, next);
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
  defaultOpen = true,
): [boolean, (open: boolean) => void] {
  const [open, setOpenState] = useState(() =>
    isWorkflowDefinitionGroupOpen(app, definitionId, defaultOpen),
  );

  useEffect(() => {
    setOpenState(
      isWorkflowDefinitionGroupOpen(app, definitionId, defaultOpen),
    );
  }, [app, defaultOpen, definitionId]);

  const setOpen = useCallback(
    (next: boolean) => {
      setWorkflowDefinitionGroupOpen(app, definitionId, next);
      setOpenState(next);
    },
    [app, definitionId],
  );

  return [open, setOpen];
}
