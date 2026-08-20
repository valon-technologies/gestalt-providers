import { Link } from "@tanstack/react-router";
import {
  useCallback,
  useDeferredValue,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { useIntegrationOperationsQuery } from "@/lib/queries";
import { appOperationElementId } from "@/lib/appAdminPaths";
import { getIntegrationLabel } from "@/lib/integrationSearch";
import {
  groupOperationsByResource,
  operationResourcePrefix,
  operationResourceSectionId,
  type OperationResourceGroup,
} from "@/lib/operationGroups";
import { CONNECTION_NAV_LABEL } from "@/lib/accountCopy";
import { userFacingError } from "@/lib/user-facing-error";
import { cn } from "@/lib/cn";
import { useScrollSpy } from "@/hooks/use-scroll-spy";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Code } from "@/components/ui/code";
import { CopyIconButton } from "@/components/ui/copy-button";
import {
  InputGroup,
  InputGroupAddon,
  InputGroupButton,
  InputGroupInput,
} from "@/components/ui/input-group";
import { Link as UiLink } from "@/components/ui/link";
import { TooltipProvider } from "@/components/ui/tooltip";
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
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  TableOfContents,
  type TableOfContentsItem,
} from "@/components/ui/table-of-contents";
import {
  Alert,
  AlertActions,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import ErrorNotice from "@/components/ErrorNotice";
import { useAppWorkspace } from "@/features/app-workspace/app-workspace-context";
import {
  AUTHORIZATION_DOCS_PATH,
  INVOKE_DOCS_PATH,
  catalogEntriesFromOperations,
  filterCatalogEntries,
  operationInvokeCliCommand,
  readOperationHash,
  resolveOperationFocus,
  type OperationCatalogEntry,
  type OperationFocus,
} from "@/features/app-workspace/operations";

const TOC_ACTIVATION_OFFSET = 112;
const HIGHLIGHT_MS = 2500;

/** Shared column grid applied to every resource-section table. */
const OPERATIONS_COLUMN_WIDTHS = {
  method: "6.5rem",
  roles: "11rem",
  actions: "3rem",
} as const;

function OperationsTableColgroup() {
  return (
    <colgroup>
      <col />
      <col style={{ width: OPERATIONS_COLUMN_WIDTHS.method }} />
      <col style={{ width: OPERATIONS_COLUMN_WIDTHS.roles }} />
      <col style={{ width: OPERATIONS_COLUMN_WIDTHS.actions }} />
    </colgroup>
  );
}

function operationMethodVariant(
  method: string,
): "outline" | "info" | "destructive" {
  const upper = method.trim().toUpperCase();
  if (upper === "POST" || upper === "PUT" || upper === "PATCH") return "info";
  if (upper === "DELETE") return "destructive";
  return "outline";
}

function OperationIdCell({
  entry,
  highlightQuery,
}: {
  entry: OperationCatalogEntry;
  highlightQuery: string;
}) {
  return (
    <div className="min-w-0">
      {entry.title ? (
        <p className="text-sm font-medium text-foreground text-pretty">
          <SearchHighlight
            text={entry.title}
            query={highlightQuery}
            variant="vivid"
          />
        </p>
      ) : null}
      <div className={cn(entry.title && "mt-1")}>
        <Code className="align-baseline">
          <SearchHighlight
            text={entry.id}
            query={highlightQuery}
            variant="vivid"
          />
        </Code>
      </div>
      {entry.description ? (
        <div className="mt-2 text-sm text-muted-foreground text-pretty">
          <SearchHighlight
            text={entry.description}
            query={highlightQuery}
            variant="vivid"
          />
        </div>
      ) : null}
      {entry.path ? (
        <p className="mt-1 font-mono text-xs text-muted-foreground">
          <SearchHighlight
            text={entry.path}
            query={highlightQuery}
            variant="vivid"
          />
        </p>
      ) : null}
      {entry.readOnly ? (
        <p className="mt-1 text-xs text-muted-foreground">Read-only</p>
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

function OperationHandoffs({
  appName,
  operationId,
}: {
  appName: string;
  operationId: string;
}) {
  return (
    <CopyIconButton
      value={() => operationInvokeCliCommand(appName, operationId)}
      tooltip={`Copy invoke command for ${operationId}`}
      copiedLabel={`Copied invoke command for ${operationId}`}
      data-testid={`ops-copy-cli-${operationId}`}
    />
  );
}

function OperationRow({
  entry,
  appName,
  highlightedOperationId,
  highlightQuery,
}: {
  entry: OperationCatalogEntry;
  appName: string;
  highlightedOperationId: string | null;
  highlightQuery: string;
}) {
  return (
    <TableRow
      id={appOperationElementId(entry.id)}
      data-operation-id={entry.id}
      tabIndex={-1}
      className={cn(
        "scroll-mt-28 transition-[background-color,box-shadow] duration-reveal outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-accent-solid",
        highlightedOperationId === entry.id &&
          "bg-accent-subtle ring-2 ring-inset ring-accent-solid",
      )}
    >
      <TableCell className="align-baseline">
        <OperationIdCell entry={entry} highlightQuery={highlightQuery} />
      </TableCell>
      <TableCell className="align-baseline">
        {entry.method ? (
          <OperationMethod method={entry.method} highlightQuery={highlightQuery} />
        ) : (
          <span className="text-muted-foreground">—</span>
        )}
      </TableCell>
      <TableCell className="align-baseline text-muted-foreground">
        {entry.rolesLabel ? (
          <SearchHighlight
            text={entry.rolesLabel}
            query={highlightQuery}
            variant="vivid"
          />
        ) : (
          <span className="text-muted-foreground">—</span>
        )}
      </TableCell>
      <TableCell className="align-baseline">
        <OperationHandoffs appName={appName} operationId={entry.id} />
      </TableCell>
    </TableRow>
  );
}

type CatalogResourceGroup = {
  prefix: string;
  label: string;
  sectionId: string;
  entries: OperationCatalogEntry[];
};

function groupCatalogEntries(
  entries: OperationCatalogEntry[],
): CatalogResourceGroup[] {
  const bySource = groupOperationsByResource(entries.map((entry) => entry.source));
  const byId = new Map(entries.map((entry) => [entry.id, entry]));

  return bySource.map((group: OperationResourceGroup) => ({
    prefix: group.prefix,
    label: group.label,
    sectionId: group.sectionId,
    entries: group.operations
      .map((operation) => byId.get(operation.id))
      .filter((entry): entry is OperationCatalogEntry => Boolean(entry)),
  }));
}

function OperationResourceSection({
  group,
  appName,
  highlightedOperationId,
  highlightQuery,
}: {
  group: CatalogResourceGroup;
  appName: string;
  highlightedOperationId: string | null;
  highlightQuery: string;
}) {
  return (
    <section
      id={group.sectionId}
      className="scroll-mt-28"
      aria-labelledby={`${group.sectionId}-heading`}
      data-testid={`ops-resource-${group.prefix}`}
    >
      <h2
        id={`${group.sectionId}-heading`}
        className="text-xl font-heading text-foreground"
      >
        <SearchHighlight text={group.label} query={highlightQuery} variant="vivid" />
      </h2>

      <Table variant="line" className="mt-4 table-fixed">
        <OperationsTableColgroup />
        <TableHeader>
          <TableRow>
            <TableHead>Operation</TableHead>
            <TableHead>Method</TableHead>
            <TableHead>Roles</TableHead>
            <TableHead>
              <span className="sr-only">Actions</span>
            </TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {group.entries.map((entry) => (
            <OperationRow
              key={entry.id}
              entry={entry}
              appName={appName}
              highlightedOperationId={highlightedOperationId}
              highlightQuery={highlightQuery}
            />
          ))}
        </TableBody>
      </Table>
    </section>
  );
}

function FocusNotice({
  focus,
  onClearSearch,
  onClearHash,
}: {
  focus: OperationFocus;
  onClearSearch: () => void;
  onClearHash: () => void;
}) {
  if (focus.status === "unknown") {
    return (
      <Alert variant="warning" className="mt-5" data-testid="ops-focus-unknown">
        <AlertTitle>Operation not found</AlertTitle>
        <AlertDescription>
          <code className="font-mono text-xs">{focus.operationId}</code> is not
          available in this app&apos;s catalog.
        </AlertDescription>
        <AlertActions>
          <Button type="button" variant="outline" size="sm" onClick={onClearHash}>
            Clear link
          </Button>
        </AlertActions>
      </Alert>
    );
  }

  if (focus.status === "hidden" && focus.reason === "filtered") {
    return (
      <Alert variant="info" className="mt-5" data-testid="ops-focus-filtered">
        <AlertTitle>Hidden by search</AlertTitle>
        <AlertDescription>
          <code className="font-mono text-xs">{focus.operationId}</code> is in
          this catalog but does not match the current search.
        </AlertDescription>
        <AlertActions>
          <Button type="button" variant="outline" size="sm" onClick={onClearSearch}>
            Clear search
          </Button>
        </AlertActions>
      </Alert>
    );
  }

  return null;
}

export default function AppWorkspaceOperationsPage() {
  const { app, integration } = useAppWorkspace();
  const appLabel = integration ? getIntegrationLabel(integration) : app;
  const {
    data: operations = [],
    isLoading: operationsLoading,
    isFetching: operationsFetching,
    error: operationsQueryError,
    refetch,
  } = useIntegrationOperationsQuery(app);

  const operationsError = operationsQueryError
    ? userFacingError(
        operationsQueryError,
        "Couldn't load operations. Try again.",
      )
    : null;

  const [searchQuery, setSearchQuery] = useState("");
  const deferredHighlightQuery = useDeferredValue(searchQuery);
  const [highlightedOperationId, setHighlightedOperationId] = useState<
    string | null
  >(null);
  const [operationHash, setOperationHash] = useState<string | null>(() =>
    readOperationHash(),
  );
  const [highlightAnnouncement, setHighlightAnnouncement] = useState("");
  const searchInputRef = useRef<HTMLInputElement>(null);
  const lastScrolledFocusKey = useRef<string | null>(null);

  const catalogEntries = useMemo(
    () => catalogEntriesFromOperations(operations),
    [operations],
  );

  const filteredEntries = useMemo(
    () => filterCatalogEntries(catalogEntries, searchQuery),
    [catalogEntries, searchQuery],
  );

  const resourceGroups = useMemo(
    () => groupCatalogEntries(filteredEntries),
    [filteredEntries],
  );

  const visibleIds = useMemo(
    () => new Set(catalogEntries.map((entry) => entry.id)),
    [catalogEntries],
  );
  const filteredIds = useMemo(
    () => new Set(filteredEntries.map((entry) => entry.id)),
    [filteredEntries],
  );

  const focus = useMemo(
    () =>
      resolveOperationFocus({
        hash: operationHash,
        loading: operationsLoading,
        visibleIds,
        filteredIds,
        sectionIdForOperation: (operationId) =>
          operationResourceSectionId(operationResourcePrefix(operationId)),
      }),
    [filteredIds, operationHash, operationsLoading, visibleIds],
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

  const clearHash = useCallback(() => {
    const { pathname, search } = window.location;
    window.history.replaceState(null, "", `${pathname}${search}`);
    setOperationHash(null);
  }, []);

  useEffect(() => {
    const syncHash = () => setOperationHash(readOperationHash());
    window.addEventListener("hashchange", syncHash);
    return () => window.removeEventListener("hashchange", syncHash);
  }, []);

  useEffect(() => {
    if (focus.status !== "matched") {
      lastScrolledFocusKey.current = null;
      return;
    }

    const focusKey = `${focus.operationId}:${focus.sectionId}`;
    if (lastScrolledFocusKey.current === focusKey) return;
    lastScrolledFocusKey.current = focusKey;

    const el = document.querySelector<HTMLElement>(
      `[data-operation-id="${CSS.escape(focus.operationId)}"]`,
    );
    if (!el) return;

    activate(focus.sectionId);
    el.scrollIntoView({ block: "start", behavior: "smooth" });
    el.focus({ preventScroll: true });
    setHighlightedOperationId(focus.operationId);
    setHighlightAnnouncement(`Showing operation ${focus.operationId}`);
    const timer = window.setTimeout(() => {
      setHighlightedOperationId(null);
      setHighlightAnnouncement("");
    }, HIGHLIGHT_MS);
    return () => window.clearTimeout(timer);
  }, [activate, focus]);

  const hasSearchQuery = searchQuery.trim().length > 0;
  const catalogCount = catalogEntries.length;

  return (
    <section aria-label="Operations">
      <div className="sr-only" aria-live="polite">
        {highlightAnnouncement}
      </div>
      <PageHeader>
        <PageHeaderContent size="md">
          <PageHeaderTitle>Operations</PageHeaderTitle>
          <PageHeaderDescription className="text-pretty">
            Callable operations for {appLabel} — grouped by resource, with
            methods, roles, and CLI commands for agents.
          </PageHeaderDescription>
          <p className="mt-2 flex flex-wrap items-center gap-x-2 gap-y-1 text-sm">
            <UiLink asChild>
              <Link to={INVOKE_DOCS_PATH}>How to invoke</Link>
            </UiLink>
            <span className="text-muted-foreground" aria-hidden>
              ·
            </span>
            <UiLink asChild>
              <Link to={AUTHORIZATION_DOCS_PATH}>Grant App Access</Link>
            </UiLink>
          </p>
        </PageHeaderContent>
      </PageHeader>

      <FocusNotice
        focus={focus}
        onClearSearch={() => setSearchQuery("")}
        onClearHash={clearHash}
      />

      {!operationsLoading && catalogCount > 0 ? (
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
          <p className="text-xs text-muted-foreground">
            {hasSearchQuery
              ? `Showing ${filteredEntries.length} of ${catalogCount} operations`
              : `${catalogCount} operation${catalogCount === 1 ? "" : "s"}`}
          </p>
        </div>
      ) : null}

      {operationsLoading ? (
        <p className="mt-5 flex items-center gap-1.5 text-sm text-faint">
          <SpinnerIcon className="size-4 animate-spin" aria-hidden />
          Loading operations…
        </p>
      ) : null}

      {operationsError ? (
        <ErrorNotice
          className="mt-5"
          message={operationsError}
          onRetry={() => {
            void refetch();
          }}
          retrying={operationsFetching && !operationsLoading}
        />
      ) : null}

      {!operationsLoading && !operationsError && catalogCount === 0 ? (
        <div className="mt-5 space-y-2 text-sm text-muted-foreground">
          <p>No operations are available for this app.</p>
          <p>
            If you expected a catalog, check{" "}
            <UiLink asChild>
              <Link to="/apps/$app/connection" params={{ app }}>
                {CONNECTION_NAV_LABEL}
              </Link>
            </UiLink>{" "}
            or read{" "}
            <UiLink asChild>
              <Link to={INVOKE_DOCS_PATH}>how to invoke operations</Link>
            </UiLink>
            .
          </p>
        </div>
      ) : null}

      {!operationsLoading &&
      !operationsError &&
      catalogCount > 0 &&
      filteredEntries.length === 0 ? (
        <p className="mt-5 text-sm text-muted-foreground">
          No operations match{" "}
          <span className="font-medium text-foreground">{searchQuery.trim()}</span>.
        </p>
      ) : null}

      {!operationsLoading && filteredEntries.length > 0 ? (
        <TooltipProvider delayDuration={0}>
          <div
            className="mt-8 flex gap-8"
            data-testid="app-operations-reference"
          >
            <div className="min-w-0 flex-1 space-y-10" data-testid="app-operations-list">
              {resourceGroups.map((group) => (
                <OperationResourceSection
                  key={group.prefix}
                  group={group}
                  appName={app}
                  highlightedOperationId={highlightedOperationId}
                  highlightQuery={deferredHighlightQuery}
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
                    highlightQuery={deferredHighlightQuery}
                  />
                </div>
              </aside>
            ) : null}
          </div>
        </TooltipProvider>
      ) : null}
    </section>
  );
}
