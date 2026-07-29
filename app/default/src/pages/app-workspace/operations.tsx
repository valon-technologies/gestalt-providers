import { useParams } from "@tanstack/react-router";
import { useEffect, useMemo, useState } from "react";
import { getIntegrationOperations, type IntegrationOperation } from "@/lib/api";
import { appOperationElementId } from "@/lib/appAdminPaths";
import { cn } from "@/lib/cn";
import { Badge } from "@/components/ui/badge";
import { SpinnerIcon } from "@/components/icons";

export default function AppWorkspaceOperationsPage() {
  const { app } = useParams({ from: "/apps/$app/operations" });
  const [operations, setOperations] = useState<IntegrationOperation[]>([]);
  const [operationsLoading, setOperationsLoading] = useState(true);
  const [operationsError, setOperationsError] = useState<string | null>(null);
  const [highlightedOperationId, setHighlightedOperationId] = useState<
    string | null
  >(null);

  useEffect(() => {
    let active = true;
    setOperationsLoading(true);
    setOperationsError(null);
    getIntegrationOperations(app)
      .then((ops) => {
        if (!active) return;
        setOperations(ops);
      })
      .catch((err) => {
        if (!active) return;
        setOperations([]);
        setOperationsError(
          err instanceof Error ? err.message : "Failed to load operations",
        );
      })
      .finally(() => {
        if (active) setOperationsLoading(false);
      });
    return () => {
      active = false;
    };
  }, [app]);

  const visibleOperations = useMemo(
    () =>
      operations.filter(
        (op) => op.visible !== false && typeof op.id === "string" && op.id,
      ),
    [operations],
  );

  useEffect(() => {
    const hash = window.location.hash.replace(/^#/, "");
    if (!hash || operationsLoading) return;
    if (!visibleOperations.some((op) => op.id === hash)) return;

    const el = document.querySelector(
      `[data-operation-id="${CSS.escape(hash)}"]`,
    );
    if (!el) return;

    el.scrollIntoView({ block: "nearest", behavior: "smooth" });
    setHighlightedOperationId(hash);
    const timer = window.setTimeout(() => setHighlightedOperationId(null), 2500);
    return () => window.clearTimeout(timer);
  }, [operationsLoading, visibleOperations]);

  return (
    <section aria-label="Operations">
      <h1 className="text-2xl font-heading text-foreground">Operations</h1>
      <p className="mt-1 text-sm text-muted-foreground">
        What this app can do — the callable operation catalog.
      </p>

      {operationsLoading ? (
        <p className="mt-5 flex items-center gap-1.5 text-sm text-faint">
          <SpinnerIcon className="size-4 animate-spin" aria-hidden />
          Loading operations…
        </p>
      ) : null}

      {operationsError ? (
        <p className="mt-5 text-sm text-ember-500">{operationsError}</p>
      ) : null}

      {!operationsLoading &&
      !operationsError &&
      visibleOperations.length === 0 ? (
        <p className="mt-5 text-sm text-faint">No visible operations for this app.</p>
      ) : null}

      {!operationsLoading && visibleOperations.length > 0 ? (
        <ul
          className="mt-5 divide-y divide-border rounded-lg border border-border"
          data-testid="app-operations-list"
        >
          {visibleOperations.map((operation) => (
            <li
              key={operation.id}
              id={appOperationElementId(operation.id)}
              data-operation-id={operation.id}
              className={cn(
                "px-4 py-3 transition-[background-color,box-shadow] duration-reveal",
                highlightedOperationId === operation.id &&
                  "bg-accent-subtle ring-2 ring-inset ring-accent-solid",
              )}
            >
              <div className="flex flex-wrap items-center gap-2">
                <code className="font-mono text-sm text-foreground">
                  {operation.id}
                </code>
                {operation.readOnly ? (
                  <Badge variant="muted" size="sm">
                    read-only
                  </Badge>
                ) : null}
              </div>
              {operation.title && operation.title !== operation.id ? (
                <p className="mt-1 text-sm text-foreground">{operation.title}</p>
              ) : null}
              {operation.description ? (
                <p className="mt-1 text-sm text-muted-foreground">
                  {operation.description}
                </p>
              ) : null}
              {operation.tags && operation.tags.length > 0 ? (
                <p className="mt-2 text-xs text-faint">
                  {operation.tags.join(" · ")}
                </p>
              ) : null}
            </li>
          ))}
        </ul>
      ) : null}
    </section>
  );
}
