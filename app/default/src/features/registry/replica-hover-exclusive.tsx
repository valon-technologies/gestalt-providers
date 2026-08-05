import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";

/**
 * One exclusive replica hover session for the tree.
 * Mode owns both open intent and pin so remounts cannot desync local pin state.
 */
export type ReplicaHoverSession = {
  key: string;
  mode: "hover" | "pinned";
};

type ExclusiveHoverStore = {
  subscribe: (listener: () => void) => () => void;
  getSession: () => ReplicaHoverSession | null;
  /** Peek — open or keep hover for `key` (does not downgrade pinned). */
  openHover: (key: string) => void;
  /** Durable open — click path / keyboard. */
  pin: (key: string) => void;
  /** Close only if `key` owns the session. */
  close: (key: string) => void;
};

export function createExclusiveHoverStore(): ExclusiveHoverStore {
  let session: ReplicaHoverSession | null = null;
  const listeners = new Set<() => void>();

  const emit = () => {
    for (const listener of listeners) listener();
  };

  const setSession = (next: ReplicaHoverSession | null) => {
    if (
      session?.key === next?.key &&
      session?.mode === next?.mode
    ) {
      return;
    }
    session = next;
    emit();
  };

  return {
    subscribe(listener) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    getSession: () => session,
    openHover(key) {
      if (session?.key === key && session.mode === "pinned") return;
      setSession({ key, mode: "hover" });
    },
    pin(key) {
      setSession({ key, mode: "pinned" });
    },
    close(key) {
      if (session?.key !== key) return;
      setSession(null);
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
  pinned: boolean;
  /** Radix open/close — pinned sessions ignore synthetic close. */
  onOpenChange: (open: boolean) => void;
  /** Click: closed→pin, hover→pin, pinned→close. */
  onTriggerClick: () => void;
  dismiss: () => void;
} {
  const store = useContext(ReplicaHoverExclusiveContext);
  const [localSession, setLocalSession] = useState<ReplicaHoverSession | null>(
    null,
  );

  const subscribe = useCallback(
    (onStoreChange: () => void) =>
      store ? store.subscribe(onStoreChange) : () => {},
    [store],
  );
  const getSnapshot = useCallback(
    () => (store ? store.getSession() : null),
    [store],
  );

  const storeSession = useSyncExternalStore(
    subscribe,
    getSnapshot,
    getSnapshot,
  );
  const session = store ? storeSession : localSession;
  const open = session?.key === key;
  const pinned = open && session?.mode === "pinned";

  const onOpenChange = useCallback(
    (next: boolean) => {
      if (!store) {
        if (next) {
          setLocalSession((prev) =>
            prev?.key === key && prev.mode === "pinned"
              ? prev
              : { key, mode: "hover" },
          );
          return;
        }
        setLocalSession((prev) =>
          prev?.key === key && prev.mode === "pinned" ? prev : null,
        );
        return;
      }
      if (next) {
        store.openHover(key);
        return;
      }
      // Pinned survives Radix synthetic closes (trigger remount / pointer leave).
      const current = store.getSession();
      if (current?.key === key && current.mode === "pinned") {
        return;
      }
      store.close(key);
    },
    [store, key],
  );

  const onTriggerClick = useCallback(() => {
    if (!store) {
      setLocalSession((prev) => {
        if (prev?.key === key && prev.mode === "pinned") return null;
        return { key, mode: "pinned" };
      });
      return;
    }
    const current = store.getSession();
    if (current?.key === key && current.mode === "pinned") {
      store.close(key);
      return;
    }
    store.pin(key);
  }, [store, key]);

  const dismiss = useCallback(() => {
    if (!store) {
      setLocalSession((prev) => (prev?.key === key ? null : prev));
      return;
    }
    store.close(key);
  }, [store, key]);

  return { open, pinned, onOpenChange, onTriggerClick, dismiss };
}
