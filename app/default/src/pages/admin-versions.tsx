import { useMemo, useState, type MouseEvent, type ReactNode } from "react";
import { Link } from "@tanstack/react-router";
import PluginSearchBar from "@/components/PluginSearchBar";
import {
  DATA_TABLE_REGISTRY_SEVERITY_GUTTER_CLASS,
  DataTableRegistryCell,
  DataTableRegistryPrimaryLine,
  DataTableRegistrySecondaryLine,
} from "@/components/ui/data-table";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { SearchHighlight } from "@/components/ui/search-highlight";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { TableStatusIndicator } from "@/components/ui/table-status-indicator";
import {
  APP_VERSIONS_EMPTY_DESCRIPTION,
  APP_VERSIONS_EMPTY_TITLE,
  APP_VERSIONS_LOAD_ERROR,
  APP_VERSIONS_NOT_INSTALLED,
  APP_VERSIONS_PAGE_DESCRIPTION,
  APP_VERSIONS_PAGE_TITLE,
  APP_VERSIONS_SEARCH_EMPTY,
} from "@/features/admin-access/admin-access-copy";
import {
  adminFleetBadge,
  adminFleetIndicatorVariant,
  adminFreshReplicasAsAppAdmin,
  adminVersionsRowMetaLine,
  filterAdminVersionsApps,
} from "@/features/admin-access/admin-versions-presentation";
import { RolloutBadge } from "@/features/registry/rollout-badge";
import { RegistryCode } from "@/features/registry/registry-code";
import { SnapshotRowLiveReplicas } from "@/features/registry/snapshot-live-replicas";
import { useDocumentTitle } from "@/hooks/use-document-title";
import type { AdminRegistryAppSummary } from "@/lib/api";
import { cn } from "@/lib/cn";
import { isInteractiveTarget } from "@/lib/nested-interactive";
import { rowLinkClickIntent } from "@/lib/row-link";
import {
  useAdminRegistryAppDetailsQueries,
  useAdminRegistryAppsQuery,
} from "@/lib/queries";

const ADMIN_VERSIONS_SKELETON_ROW_COUNT = 8;
const ADMIN_VERSIONS_SKELETON_APP_WIDTHS = [
  "w-40",
  "w-32",
  "w-44",
  "w-28",
  "w-36",
  "w-48",
  "w-32",
  "w-40",
] as const;
const ADMIN_VERSIONS_STATUS_GUTTER_CLASS = cn(
  DATA_TABLE_REGISTRY_SEVERITY_GUTTER_CLASS,
  "pr-2.5",
);

function onAdminVersionsRowLinkClick(event: MouseEvent<HTMLAnchorElement>) {
  const intent = rowLinkClickIntent({
    button: event.button,
    metaKey: event.metaKey,
    ctrlKey: event.ctrlKey,
    shiftKey: event.shiftKey,
    altKey: event.altKey,
    targetIsInteractive: isInteractiveTarget(
      event.target,
      event.currentTarget,
    ),
  });
  if (intent === "suppress") {
    event.preventDefault();
  }
}

function AdminVersionsTable({
  loading = false,
  children,
}: {
  loading?: boolean;
  children: ReactNode;
}) {
  return (
    <div
      className="overflow-hidden rounded-lg border border-border bg-card text-card-foreground"
      aria-busy={loading || undefined}
      data-testid="admin-versions-table"
    >
      {loading ? <span className="sr-only">Loading app versions</span> : null}
      <Table aria-label={APP_VERSIONS_PAGE_TITLE}>
        <TableHeader className="sr-only">
          <TableRow>
            <TableHead>Status</TableHead>
            <TableHead>App</TableHead>
            <TableHead>Rollout</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>{children}</TableBody>
      </Table>
    </div>
  );
}

function AdminVersionsSkeletonRow({ index }: { index: number }) {
  const titleWidth =
    ADMIN_VERSIONS_SKELETON_APP_WIDTHS[
      index % ADMIN_VERSIONS_SKELETON_APP_WIDTHS.length
    ];
  return (
    <TableRow
      aria-hidden
      className="pointer-events-none"
      data-testid="admin-versions-skeleton-row"
    >
      <TableCell
        className={`${ADMIN_VERSIONS_STATUS_GUTTER_CLASS} align-top`}
      >
        <DataTableRegistryPrimaryLine className="justify-end">
          <Skeleton className="size-5 rounded-full" />
        </DataTableRegistryPrimaryLine>
      </TableCell>
      <TableCell className="pl-0 align-top">
        <DataTableRegistryCell>
          <DataTableRegistryPrimaryLine>
            <Skeleton className={cn("h-5", titleWidth)} />
          </DataTableRegistryPrimaryLine>
          <DataTableRegistrySecondaryLine>
            <Skeleton className="h-4 w-56 max-w-full" />
          </DataTableRegistrySecondaryLine>
          <DataTableRegistrySecondaryLine>
            <Skeleton className="h-3 w-52 max-w-full" />
          </DataTableRegistrySecondaryLine>
        </DataTableRegistryCell>
      </TableCell>
      <TableCell className="align-top">
        <DataTableRegistryPrimaryLine>
          <Skeleton className="h-5 w-16 rounded-full" />
        </DataTableRegistryPrimaryLine>
      </TableCell>
    </TableRow>
  );
}

function AdminVersionsAppRow({
  app,
  replicas,
  query,
}: {
  app: AdminRegistryAppSummary;
  replicas: ReturnType<typeof adminFreshReplicasAsAppAdmin>;
  query: string;
}) {
  const fleet = adminFleetBadge(app.fleetState);
  const version = app.desiredVersion || APP_VERSIONS_NOT_INSTALLED;
  const metaLine = adminVersionsRowMetaLine(
    replicas,
    app.cohort,
    app.rollout?.state,
  );
  const meta = metaLine ? (
    <SearchHighlight text={metaLine} query={query} variant="vivid" />
  ) : null;
  return (
    <TableRow
      className="relative isolate"
      data-testid={`admin-versions-row-${app.app}`}
    >
      <TableCell
        className={`${ADMIN_VERSIONS_STATUS_GUTTER_CLASS} align-top`}
      >
        <DataTableRegistryPrimaryLine className="justify-end">
          <TableStatusIndicator
            variant={adminFleetIndicatorVariant(app.fleetState)}
            iconOnly
            size="md"
            label={fleet.label}
          />
        </DataTableRegistryPrimaryLine>
      </TableCell>
      <TableCell className="pl-0 align-top">
        <DataTableRegistryCell>
          <DataTableRegistryPrimaryLine>
            <Link
              to="/admin/versions/$app"
              params={{ app: app.app }}
              data-row-link=""
              className={cn(
                "font-medium text-foreground no-underline hover:text-foreground visited:text-foreground",
                "after:absolute after:inset-0 after:z-[1] after:content-['']",
                "focus-visible:outline-none focus-visible:after:outline-3",
                "focus-visible:after:outline-offset-2 focus-visible:after:outline-ring",
              )}
              onClick={onAdminVersionsRowLinkClick}
            >
              <SearchHighlight text={app.app} query={query} variant="vivid" />
            </Link>
          </DataTableRegistryPrimaryLine>
          <DataTableRegistrySecondaryLine>
            <RegistryCode>
              <SearchHighlight text={version} query={query} variant="vivid" />
            </RegistryCode>
          </DataTableRegistrySecondaryLine>
          {replicas.length > 0 ? (
            <SnapshotRowLiveReplicas
              replicas={replicas}
              className="relative z-10 mt-0"
              hoverScope={`admin-versions:${app.app}`}
              heartbeatTtlSeconds={app.fleetState.heartbeatTtlSeconds}
              summary={meta}
            />
          ) : meta ? (
            <DataTableRegistrySecondaryLine>{meta}</DataTableRegistrySecondaryLine>
          ) : null}
        </DataTableRegistryCell>
      </TableCell>
      <TableCell className="align-top">
        <DataTableRegistryPrimaryLine>
          <RolloutBadge app={app} />
        </DataTableRegistryPrimaryLine>
      </TableCell>
    </TableRow>
  );
}

export default function AdminVersionsPage() {
  useDocumentTitle(APP_VERSIONS_PAGE_TITLE);
  const [query, setQuery] = useState("");
  const appsQuery = useAdminRegistryAppsQuery();
  const apps = appsQuery.data ?? [];
  const detailQueries = useAdminRegistryAppDetailsQueries(
    appsQuery.isSuccess ? apps.map((app) => app.app) : [],
  );
  const filteredApps = useMemo(
    () => filterAdminVersionsApps(apps, query),
    [apps, query],
  );
  const replicasByApp = useMemo(() => {
    return new Map(
      apps.map((app, index) => [
        app.app,
        adminFreshReplicasAsAppAdmin(
          detailQueries[index]?.data?.freshReplicas,
          app.desiredVersion,
        ),
      ]),
    );
  }, [apps, detailQueries]);

  return (
    <div className="space-y-6">
      <PageHeader>
        <PageHeaderContent size="md">
          <PageHeaderTitle>{APP_VERSIONS_PAGE_TITLE}</PageHeaderTitle>
          <PageHeaderDescription>
            {APP_VERSIONS_PAGE_DESCRIPTION}
          </PageHeaderDescription>
        </PageHeaderContent>
      </PageHeader>

      <PluginSearchBar
        query={query}
        onQueryChange={setQuery}
        disabled={appsQuery.isPending}
      />

      {appsQuery.isPending ? (
        <AdminVersionsTable loading>
          {Array.from(
            { length: ADMIN_VERSIONS_SKELETON_ROW_COUNT },
            (_, index) => (
              <AdminVersionsSkeletonRow key={index} index={index} />
            ),
          )}
        </AdminVersionsTable>
      ) : appsQuery.isError ? (
        <p className="text-sm text-destructive">{APP_VERSIONS_LOAD_ERROR}</p>
      ) : filteredApps.length === 0 ? (
        query.trim() ? (
          <p className="text-sm text-muted-foreground">{APP_VERSIONS_SEARCH_EMPTY}</p>
        ) : (
          <div className="rounded-lg border border-border bg-card p-6 text-card-foreground">
            <p className="text-sm font-medium text-foreground">
              {APP_VERSIONS_EMPTY_TITLE}
            </p>
            <p className="mt-1 text-sm text-muted-foreground">
              {APP_VERSIONS_EMPTY_DESCRIPTION}
            </p>
          </div>
        )
      ) : (
        <AdminVersionsTable>
          {filteredApps.map((app) => (
            <AdminVersionsAppRow
              key={app.app}
              app={app}
              query={query}
              replicas={replicasByApp.get(app.app) ?? []}
            />
          ))}
        </AdminVersionsTable>
      )}
    </div>
  );
}
