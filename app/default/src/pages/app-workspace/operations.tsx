import { useParams } from "@tanstack/react-router";
import {
  useCallback,
  useDeferredValue,
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
  operationResourcePrefix,
  operationResourceSectionId,
  type OperationResourceGroup,
} from "@/lib/operationGroups";
import { cn } from "@/lib/cn";
import {
  extractSearchSnippet,
  searchTokensMissingFromText,
  textContainsAllSearchTokens,
} from "@/lib/search-highlight";
import { useScrollSpy } from "@/hooks/use-scroll-spy";
import { Badge } from "@/components/ui/badge";
import { Code } from "@/components/ui/code";
import {
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupInput,
} from "@/components/ui/input-group";
import { CloseIcon, SearchIcon, SpinnerIcon } from "@/components/icons";
import { SearchHighlight } from "@/components/ui/search-highlight";
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

/** Shared column grid for the whole operations catalog (one table, many groups). */
const OPERATIONS_COLUMN_WIDTHS = {
  operation: "34%",
  method: "6.5rem",
  roles: "11rem",
} as const;

function operationMethodVariant(
  method: string,
): "muted" | "info" | "secondary" {
  const upper = method.trim().toUpperCase();
  if (upper === "GET" || upper === "HEAD") return "muted";
  if (upper === "POST" || upper === "PUT" || upper === "PATCH") return "info";
  return "secondary";
}

function OperationIdCell({
  operation,
  highlightQuery,
}: {
  operation: IntegrationOperation;
  highlightQuery: string;
}) {
  const description = operation.description?.trim() ?? "";
  const showSnippet = Boolean(
    highlightQuery.trim()
      && description
      && !textContainsAllSearchTokens(operation.id, highlightQuery),
  );
  const snippet = showSnippet
    ? extractSearchSnippet(
        description,
        highlightQuery,
        48,
        searchTokensMissingFromText(operation.id, highlightQuery),
      )
    : null;

  return (
    <div className="min-w-0">
      <Code className="align-baseline">
        <SearchHighlight
          text={operation.id}
          query={highlightQuery}
          variant="vivid"
        />
      </Code>
      {snippet ? (
        <div className="mt-0.5 truncate text-xs text-muted-foreground">
          <SearchHighlight text={snippet} query={highlightQuery} variant="vivid" />
        </div>
      ) : null}
    </div>
  );
}

function OperationMethod({
  method,
  highlightQuery,
}: {
  method: string;
  highlightQuery: string;
}) {
  return (
    <Badge
      variant={operationMethodVariant(method)}
      size="sm"
      className="align-baseline"
    >
      <SearchHighlight
        text={method.trim().toUpperCase()}
        query={highlightQuery}
        variant="vivid"
      />
    </Badge>
  );
}

function OperationRow({
  operation,
  highlightedOperationId,
  highlightQuery,
}: {
  operation: IntegrationOperation;
  highlightedOperationId: string | null;
  highlightQuery: string;
}) {
  const rolesLabel =
    operation.allowedRoles && operation.allowedRoles.length > 0
      ? operation.allowedRoles.join(", ")
      : null;

  return (
    <TableRow
      id={appOperationElementId(operation.id)}
      data-operation-id={operation.id}
      className={cn(
        "scroll-mt-28 transition-[background-color,box-shadow] duration-reveal",
        highlightedOperationId === operation.id &&
          "bg-accent-subtle ring-2 ring-inset ring-accent-solid",
      )}
    >
      <TableCell className="align-baseline">
        <OperationIdCell operation={operation} highlightQuery={highlightQuery} />
      </TableCell>
      <TableCell className="align-baseline">
        {operation.method ? (
          <OperationMethod method={operation.method} highlightQuery={highlightQuery} />
        ) : (
          <span className="text-faint">—</span>
        )}
      </TableCell>
      <TableCell className="align-baseline text-muted-foreground">
        {operation.description?.trim() ? (
          <SearchHighlight
            text={operation.description.trim()}
            query={highlightQuery}
            variant="vivid"
          />
        ) : (
          "—"
        )}
      </TableCell>
      <TableCell className="align-baseline text-muted-foreground">
        {rolesLabel ? (
          <SearchHighlight text={rolesLabel} query={highlightQuery} variant="vivid" />
        ) : (
          <span className="text-faint">—</span>
        )}
      </TableCell>
    </TableRow>
  );
}

function OperationsCatalogTable({
  resourceGroups,
  highlightedOperationId,
  highlightQuery,
}: {
  resourceGroups: OperationResourceGroup[];
  highlightedOperationId: string | null;
  highlightQuery: string;
}) {
  return (
    <Table variant="line" className="table-fixed">
      <colgroup>
        <col style={{ width: OPERATIONS_COLUMN_WIDTHS.operation }} />
        <col style={{ width: OPERATIONS_COLUMN_WIDTHS.method }} />
        <col />
        <col style={{ width: OPERATIONS_COLUMN_WIDTHS.roles }} />
      </colgroup>
      <TableHeader>
        <TableRow>
          <TableHead>Operation</TableHead>
          <TableHead>Method</TableHead>
          <TableHead>Description</TableHead>
          <TableHead>Roles</TableHead>
        </TableRow>
      </TableHeader>
      {resourceGroups.map((group, groupIndex) => (
        <TableBody key={group.prefix}>
          <TableRow
            className="border-b-0 hover:bg-transparent active:bg-transparent"
            data-testid={`ops-resource-${group.prefix}`}
          >
            <TableCell
              colSpan={4}
              className={cn(
                "scroll-mt-28 px-3 pb-2",
                groupIndex === 0 ? "pt-4" : "pt-10",
              )}
            >
              <h2
                id={group.sectionId}
                className="text-xl font-heading text-foreground"
              >
                <SearchHighlight
                  text={group.label}
                  query={highlightQuery}
                  variant="vivid"
                />
              </h2>
            </TableCell>
          </TableRow>
          {group.operations.map((operation) => (
            <OperationRow
              key={operation.id}
              operation={operation}
              highlightedOperationId={highlightedOperationId}
              highlightQuery={highlightQuery}
            />
          ))}
        </TableBody>
      ))}
    </Table>
  );
}

export default function AppWorkspaceOperationsPage() {
  const { app } = useParams({ from: "/apps/$app/operations" });
  const [operations, setOperations] = useState<IntegrationOperation[]>([]);
  const [operationsLoading, setOperationsLoading] = useState(true);
  const [operationsError, setOperationsError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const deferredHighlightQuery = useDeferredValue(searchQuery);
  const [highlightedOperationId, setHighlightedOperationId] = useState<
    string | null
  >(null);
  const searchInputRef = useRef<HTMLInputElement>(null);

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
    return resourceGroups.map((group) => ({
      id: group.sectionId,
      title: group.label,
      depth: 1,
    }));
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
    const rawHash = window.location.hash.replace(/^#/, "");
    if (!rawHash || operationsLoading) return;

    let hash = rawHash;
    try {
      hash = decodeURIComponent(rawHash);
    } catch {
      return;
    }

    if (!visibleOperations.some((op) => op.id === hash)) return;

    if (
      searchQuery.trim() &&
      !filterOperations(visibleOperations, searchQuery).some((op) => op.id === hash)
    ) {
      setSearchQuery("");
      return;
    }

    const el = document.querySelector(
      `[data-operation-id="${CSS.escape(hash)}"]`,
    );
    if (!el) return;

    activate(
      operationResourceSectionId(operationResourcePrefix(hash)),
    );
    el.scrollIntoView({ block: "start", behavior: "smooth" });
    setHighlightedOperationId(hash);
    const timer = window.setTimeout(() => setHighlightedOperationId(null), 2500);
    return () => window.clearTimeout(timer);
  }, [activate, operationsLoading, searchQuery, visibleOperations]);

  const hasSearchQuery = searchQuery.trim().length > 0;

  return (
    <section aria-label="Operations">
      <h1 className="text-2xl font-heading text-foreground">Operations</h1>
      <p className="mt-1 text-sm text-muted-foreground text-pretty">
        Callable operation catalog for this app — grouped by resource, with
        method summaries and deep links for agents and the CLI.
      </p>

      {!operationsLoading && visibleOperations.length > 0 ? (
        <div className="mt-5 max-w-md space-y-2">
          <InputGroup>
            <InputGroupAddon align="inline-start">
              <SearchIcon aria-hidden />
            </InputGroupAddon>
            <InputGroupInput
              ref={searchInputRef}
              type="search"
              value={searchQuery}
              onChange={(event) => setSearchQuery(event.target.value)}
              placeholder="Search operations…"
              aria-label="Search operations"
              autoComplete="off"
              data-testid="app-operations-search"
              className="[&::-webkit-search-cancel-button]:hidden"
            />
            {searchQuery.trim().length > 0 ? (
              <InputGroupAddon align="inline-end">
                <InputGroupButton
                  aria-label="Clear operation search"
                  onMouseDown={(event) => {
                    event.preventDefault();
                  }}
                  onClick={(event) => {
                    event.preventDefault();
                    setSearchQuery("");
                    searchInputRef.current?.focus();
                  }}
                >
                  <CloseIcon className="size-4" />
                </InputGroupButton>
              </InputGroupAddon>
            ) : null}
          </InputGroup>
          {hasSearchQuery && filteredOperations.length > 0 ? (
            <p className="text-xs text-faint">
              Showing {filteredOperations.length} of {visibleOperations.length}{" "}
              operations
            </p>
          ) : null}
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
            <div className="min-w-0 flex-1" data-testid="app-operations-list">
              <OperationsCatalogTable
                resourceGroups={resourceGroups}
                highlightedOperationId={highlightedOperationId}
                highlightQuery={deferredHighlightQuery}
              />
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
                    highlightQuery={deferredHighlightQuery}
                  />
                </div>
              </aside>
            ) : null}
        </div>
      ) : null}
    </section>
  );
}
