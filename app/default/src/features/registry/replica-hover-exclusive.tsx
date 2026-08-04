import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from "react";

type ReplicaHoverExclusiveContextValue = {
  openKey: string | null;
  setOpenKey: (key: string | null) => void;
};

const ReplicaHoverExclusiveContext =
  createContext<ReplicaHoverExclusiveContextValue | null>(null);

/** Ensures at most one replica HoverCard is open across the tree. */
export function ReplicaHoverExclusiveProvider({
  children,
}: {
  children: ReactNode;
}) {
  const [openKey, setOpenKey] = useState<string | null>(null);
  const value = useMemo(() => ({ openKey, setOpenKey }), [openKey]);
  return (
    <ReplicaHoverExclusiveContext.Provider value={value}>
      {children}
    </ReplicaHoverExclusiveContext.Provider>
  );
}

export function useExclusiveReplicaHover(key: string): {
  open: boolean;
  onOpenChange: (open: boolean) => void;
} {
  const ctx = useContext(ReplicaHoverExclusiveContext);
  const [localOpen, setLocalOpen] = useState(false);

  const onOpenChange = useCallback(
    (next: boolean) => {
      if (!ctx) {
        setLocalOpen(next);
        return;
      }
      if (next) ctx.setOpenKey(key);
      else if (ctx.openKey === key) ctx.setOpenKey(null);
    },
    [ctx, key],
  );

  if (!ctx) {
    return { open: localOpen, onOpenChange };
  }
  return { open: ctx.openKey === key, onOpenChange };
}
