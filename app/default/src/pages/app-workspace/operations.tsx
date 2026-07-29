import { useParams } from "@tanstack/react-router";
import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { getIntegrationOperations, type IntegrationOperation } from "@/lib/api";
import { appOperationElementId } from "@/lib/appAdminPaths";
import {
  filterOperations,
  groupOperationsByResource,
  type OperationResourceGroup,
} from "@/lib/operationGroups";
import { cn } from "@/lib/cn";
import { useScrollSpy } from "@/hooks/use-scroll-spy";
import { Badge } from "@/components/ui/badge";
import { Eyebrow } from "@/components/ui/eyebrow";
import { Input } from "@/components/ui/input";
import { SpinnerIcon } from "@/components/icons";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  TableOfContents,
  type TableOfContentsItem,
} from "@/components/ui/table-of-contents";

const TOC_ACTIVATION_OFFSET = 112;

function operationMethodVariant(method: string | undefined): "muted" | "info" | "secondary" {
  const upper = method?.trim().toUpperCase();
  if (upper === "GET" || upper === "HEAD") return "muted";
  if (upper === "POST" || upper === "PUT" || upper === "PATCH") return "info";
  return "secondary";
}

function operationDisplayTitle(operation: IntegrationOperation): string {
  if (operation.title?.trim() && operation.title !== operation.id) {
    return operation.title.trim();
  }
  const dot = operation.id.indexOf(".");
  return dot === -1 ? operation.id : operation.id.slice(dot + 1);
}

function OperationDetail({
  operation,
  highlighted,
}: {
  operation: IntegrationOperation;
  highlighted: boolean;
}) {
  const title = operationDisplayTitle(operation);

  return (
    <article
      id={appOperationElementId(operation.id)}
      data-operation-id={operation.id}
      className={cn(
        "scroll-mt-28 rounded-lg border border-border bg-card px-4 py-3 transition-[background-color,box-shadow] duration-reveal",
        highlighted && "bg-accent-subtle ring-2 ring-inset ring-accent-solid",
      )}
    >
      <div className="flex flex-wrap items-center gap-2">
        <h3 className="font-mono text-sm font-medium text-foreground">
          {operation.id}
        </h3>
        {operation.method ? (
          <Badge variant={operationMethodVariant(operation.method)} size="sm">
            {operation.method.toUpperCase()}
          </Badge>
        ) : null}
        {operation.readOnly ? (
          <Badge variant="muted" size="sm">read-only</Badge>
        ) : null}
      </div>
      {title !== operation.id ? (
        <p className="mt-1 text-sm font-medium text-foreground">{title}</p>
      ) : null}
      {operation.description ? (
        <p className="mt-1 text-sm text-muted-foreground text-pretty">
          {operation.description}
        </p>
      ) : null}
      {operation.allowedRoles && operation.allowedRoles.length > 0 ? (
        <p className="mt-2 text-xs text-faint">
          Roles: {operation.allowedRoles.join(", ")}
        </p>
      ) : null}
      {operation.tags && operation.tags.length > 0 ? (
        <p className="mt-2 text-xs text-faint">
          {operation.tags.join(" · ")}
        </p>
      ) : null}
    </article>
  );
}

function OperationResourceSection({
  group,
  highlightedOperationId,
}: {
  group: OperationResourceGroup;
  highlightedOperationId: string | null;
}) {
  return (
    <section
      id={group.sectionId}
      className="scroll-mt-28"
      aria-labelledby={`${group.sectionId}-heading`}
    >
      <h2
        id={`${group.sectionId}-heading`}
        className="text-xl font-heading text-foreground"
      >
        {group.label}
      </h2>

      <div className="mt-4">
        <Eyebrow tone="secondary" className="mb-2">Operations</Eyebrow>
        <Table variant="surface" className="rounded-lg border border-border">
          <TableHeader>
            <TableRow>
              <TableHead>Operation</TableHead>
              <TableHead className="w-24">Method</TableHead>
              <TableHead>Description</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {group.operations.map((operation) => (
              <TableRow key={operation.id}>
                <TableCell className="font-mono text-foreground">
                  {operation.id}
                </TableCell>
                <TableCell>
                  {operation.method ? (
                    <Badge
                      variant={operationMethodVariant(operation.method)}
                      size="sm"
                    >
                      {operation.method.toUpperCase()}
                    </Badge>
                  ) : (
                    <span className="text-faint">—</span>
                  )}
                </TableCell>
                <TableCell className="text-muted-foreground">
                  {operation.description?.trim() || "—"}
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>

      <div className="mt-6 space-y-3">
        <Eyebrow tone="secondary">Details</Eyebrow>
        {group.operations.map((operation) => (
          <OperationDetail
            key={operation.id}
            operation={operation}
            highlighted={highlightedOperationId === operation.id}
          />
        ))}
      </div>
    </section>
  );
}

export default function AppWorkspaceOperationsPage() {
  const { app } = useParams({ from: "/apps/$app/operations" });
  const [operations, setOperations] = useState<IntegrationOperation[]>([]);
  const [operationsLoading, setOperationsLoading] = useState(true);
  const [operationsError, setOperationsError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
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

  const filteredOperations = useMemo(
    () => filterOperations(visibleOperations, searchQuery),
    [visibleOperations, searchQuery],
  );

  const resourceGroups = useMemo(
    () => groupOperationsByResource(filteredOperations),
    [filteredOperations],
  );

  const tocItems = useMemo((): TableOfContentsItem[] => {
    const items: TableOfContentsItem[] = [];
    for (const group of resourceGroups) {
      items.push({
        id: group.sectionId,
        title: group.label,
        depth: 1,
      });
      for (const operation of group.operations) {
        items.push({
          id: appOperationElementId(operation.id),
          title: operationDisplayTitle(operation),
          depth: 2,
        });
      }
    }
    return items;
  }, [resourceGroups]);

  const scrollRootRef = useRef<HTMLElement | null>(null);
  useLayoutEffect(() => {
    scrollRootRef.current = document.documentElement;
  }, []);

  const sectionsKey = tocItems.map((item) => item.id).join(",");
  const getEntries = useCallback(() => {
    return tocItems.flatMap((item) => {
      if (item.kind === "separator") return [];
      const el = document.getElementById(item.id);
      return el
        ? [{ id: item.id, top: el.getBoundingClientRect().top }]
        : [];
    });
  }, [tocItems]);

  const { activeId, activate } = useScrollSpy({
    scrollRootRef,
    getEntries,
    sectionsKey,
    activationOffset: TOC_ACTIVATION_OFFSET,
    forceLastAtBottom: true,
    enabled: resourceGroups.length > 0,
    observeWindow: true,
  });

  const onTocSelect = useCallback(
    (id: string) => {
      const el = document.getElementById(id);
      if (!el) return;
      activate(id);
      el.scrollIntoView({ behavior: "smooth", block: "start" });
    },
    [activate],
  );

  useEffect(() => {
    const hash = window.location.hash.replace(/^#/, "");
    if (!hash || operationsLoading) return;
    if (!visibleOperations.some((op) => op.id === hash)) return;

    const el = document.querySelector(
      `[data-operation-id="${CSS.escape(hash)}"]`,
    );
    if (!el) return;

    const domId = appOperationElementId(hash);
    activate(domId);
    el.scrollIntoView({ block: "start", behavior: "smooth" });
    setHighlightedOperationId(hash);
    const timer = window.setTimeout(() => setHighlightedOperationId(null), 2500);
    return () => window.clearTimeout(timer);
  }, [activate, operationsLoading, visibleOperations]);

  const hasSearchQuery = searchQuery.trim().length > 0;

  return (
    <section aria-label="Operations">
      <h1 className="text-2xl font-heading text-foreground">Operations</h1>
      <p className="mt-1 text-sm text-muted-foreground text-pretty">
        Callable operation catalog for this app — grouped by resource, with
        method summaries and deep links for agents and the CLI.
      </p>

      {!operationsLoading && visibleOperations.length > 0 ? (
        <div className="mt-5 max-w-md">
          <Input
            type="search"
            value={searchQuery}
            onChange={(event) => setSearchQuery(event.target.value)}
            placeholder="Search operations…"
            aria-label="Search operations"
            data-testid="app-operations-search"
          />
        </div>
      ) : null}

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

      {!operationsLoading &&
      !operationsError &&
      visibleOperations.length > 0 &&
      filteredOperations.length === 0 ? (
        <p className="mt-5 text-sm text-muted-foreground">
          No operations match{" "}
          <span className="font-medium text-foreground">{searchQuery.trim()}</span>.
        </p>
      ) : null}

      {!operationsLoading && filteredOperations.length > 0 ? (
        <div
          className="mt-8 flex gap-8"
          data-testid="app-operations-reference"
        >
          <div
            className="min-w-0 flex-1 space-y-10"
            data-testid="app-operations-list"
          >
            {resourceGroups.map((group) => (
              <OperationResourceSection
                key={group.prefix}
                group={group}
                highlightedOperationId={highlightedOperationId}
              />
            ))}
          </div>

          {tocItems.length > 0 ? (
            <aside
              className="hidden w-52 shrink-0 xl:block"
              data-testid="app-operations-toc"
            >
              <div className="sticky top-28">
                <p className="mb-2 text-sm font-medium text-foreground">
                  On this page
                </p>
                <TableOfContents
                  items={tocItems}
                  activeId={activeId}
                  onItemSelect={onTocSelect}
                  label="Operations on this page"
                  maxHeight="calc(100vh - 9rem)"
                />
              </div>
            </aside>
          ) : null}
        </div>
      ) : null}

      {!operationsLoading &&
      hasSearchQuery &&
      filteredOperations.length > 0 ? (
        <p className="mt-4 text-xs text-faint">
          Showing {filteredOperations.length} of {visibleOperations.length}{" "}
          operations
        </p>
      ) : null}
    </section>
  );
}
