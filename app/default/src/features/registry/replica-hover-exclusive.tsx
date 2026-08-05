import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";

type ExclusiveHoverStore = {
  subscribe: (listener: () => void) => () => void;
  getOpenKey: () => string | null;
  setOpenKey: (key: string | null) => void;
};

function createExclusiveHoverStore(): ExclusiveHoverStore {
  let openKey: string | null = null;
  const listeners = new Set<() => void>();
  return {
    subscribe(listener) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    getOpenKey: () => openKey,
    setOpenKey(next) {
      if (openKey === next) return;
      openKey = next;
      for (const listener of listeners) listener();
    },
  };
}

const ReplicaHoverExclusiveContext =
  createContext<ExclusiveHoverStore | null>(null);

/**
 * Ensures at most one replica HoverCard is open across the tree.
 *
 * Uses an external store so closed chips do not re-render when another chip
 * opens (context-value churn was forcing every HoverCard root to update).
 */
export function ReplicaHoverExclusiveProvider({
  children,
}: {
  children: ReactNode;
}) {
  const store = useMemo(() => createExclusiveHoverStore(), []);
  return (
    <ReplicaHoverExclusiveContext.Provider value={store}>
      {children}
    </ReplicaHoverExclusiveContext.Provider>
  );
}

export function useExclusiveReplicaHover(key: string): {
  open: boolean;
  onOpenChange: (open: boolean) => void;
} {
  const store = useContext(ReplicaHoverExclusiveContext);
  const [localOpen, setLocalOpen] = useState(false);

  const subscribe = useCallback(
    (onStoreChange: () => void) =>
      store ? store.subscribe(onStoreChange) : () => {},
    [store],
  );
  const getSnapshot = useCallback(
    () => (store ? store.getOpenKey() === key : false),
    [store, key],
  );

  const storeOpen = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);

  const onOpenChange = useCallback(
    (next: boolean) => {
      if (!store) {
        setLocalOpen(next);
        return;
      }
      if (next) store.setOpenKey(key);
      else if (store.getOpenKey() === key) store.setOpenKey(null);
    },
    [store, key],
  );

  if (!store) {
    return { open: localOpen, onOpenChange };
  }
  return { open: storeOpen, onOpenChange };
}
