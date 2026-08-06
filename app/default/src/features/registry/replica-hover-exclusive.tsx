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

export type ReplicaHoverKeyState = "closed" | "hover" | "pinned";

type ExclusiveHoverStore = {
  /** Subscribe to changes that affect `key` only (not every session swap). */
  subscribeKey: (key: string, listener: () => void) => () => void;
  getKeyState: (key: string) => ReplicaHoverKeyState;
  getSession: () => ReplicaHoverSession | null;
  /**
   * Peek — open or keep hover for `key`.
   * Does not steal a pin owned by another key; does not downgrade own pin.
   */
  openHover: (key: string) => void;
  /** Durable open — click path / keyboard. */
  pin: (key: string) => void;
  /** Close only if `key` owns the session. */
  close: (key: string) => void;
};

function keyStateFor(
  session: ReplicaHoverSession | null,
  key: string,
): ReplicaHoverKeyState {
  if (session?.key !== key) return "closed";
  return session.mode;
}

export function createExclusiveHoverStore(): ExclusiveHoverStore {
  let session: ReplicaHoverSession | null = null;
  const listenersByKey = new Map<string, Set<() => void>>();

  const emitKey = (key: string) => {
    const listeners = listenersByKey.get(key);
    if (!listeners) return;
    for (const listener of listeners) listener();
  };

  const setSession = (next: ReplicaHoverSession | null) => {
    if (
      session?.key === next?.key &&
      session?.mode === next?.mode
    ) {
      return;
    }
    const prev = session;
    session = next;
    if (prev?.key) emitKey(prev.key);
    if (next?.key && next.key !== prev?.key) emitKey(next.key);
  };

  return {
    subscribeKey(key, listener) {
      let listeners = listenersByKey.get(key);
      if (!listeners) {
        listeners = new Set();
        listenersByKey.set(key, listeners);
      }
      listeners.add(listener);
      return () => {
        listeners!.delete(listener);
        if (listeners!.size === 0) listenersByKey.delete(key);
      };
    },
    getKeyState: (key) => keyStateFor(session, key),
    getSession: () => session,
    openHover(key) {
      // Pinned sessions are durable until explicit unpin / dismiss / pin-other.
      if (session?.mode === "pinned" && session.key !== key) return;
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
 * Uses an external store with per-key subscriptions so closed chips do not
 * re-render when another chip opens.
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
  const [localState, setLocalState] = useState<ReplicaHoverKeyState>("closed");

  const subscribe = useCallback(
    (onStoreChange: () => void) =>
      store ? store.subscribeKey(key, onStoreChange) : () => {},
    [store, key],
  );
  const getSnapshot = useCallback(
    () => (store ? store.getKeyState(key) : localState),
    [store, key, localState],
  );

  const state = useSyncExternalStore(subscribe, getSnapshot, getSnapshot);
  const open = state !== "closed";
  const pinned = state === "pinned";

  const onOpenChange = useCallback(
    (next: boolean) => {
      if (!store) {
        if (next) {
          setLocalState((prev) => (prev === "pinned" ? prev : "hover"));
          return;
        }
        setLocalState((prev) => (prev === "pinned" ? prev : "closed"));
        return;
      }
      if (next) {
        store.openHover(key);
        return;
      }
      // Pinned survives Radix synthetic closes (trigger remount / pointer leave).
      if (store.getKeyState(key) === "pinned") return;
      store.close(key);
    },
    [store, key],
  );

  const onTriggerClick = useCallback(() => {
    if (!store) {
      setLocalState((prev) => (prev === "pinned" ? "closed" : "pinned"));
      return;
    }
    if (store.getKeyState(key) === "pinned") {
      store.close(key);
      return;
    }
    store.pin(key);
  }, [store, key]);

  const dismiss = useCallback(() => {
    if (!store) {
      setLocalState("closed");
      return;
    }
    store.close(key);
  }, [store, key]);

  return { open, pinned, onOpenChange, onTriggerClick, dismiss };
}
