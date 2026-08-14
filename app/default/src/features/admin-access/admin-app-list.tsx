import { useMemo, useState } from "react";
import { Link } from "@tanstack/react-router";
import type { Integration } from "@/lib/api";
import { getIntegrationLabel, filterIntegrations } from "@/lib/integrationSearch";
import IntegrationIcon from "@/components/IntegrationIcon";
import PluginSearchBar from "@/components/PluginSearchBar";
import { SearchHighlight } from "@/components/ui/search-highlight";
import { Skeleton } from "@/components/ui/skeleton";
import { listItemInteraction } from "@/lib/list-item-interaction";
import {
  isInteractiveTarget,
  nestedInteractiveSuppress,
} from "@/lib/nested-interactive";
import { rowLinkClickIntent } from "@/lib/row-link";
import { cn } from "@/lib/cn";
import {
  useAppAuthorizationMembersQueries,
  useAuthorizationResourceTypesQuery,
} from "@/lib/queries";
import {
  resourceTypeHasDefaultRole,
  summarizeAppAccessList,
} from "./admin-access";
import { ACCESS_LIST_STATUS, accessListStatus } from "./admin-access-copy";
import { AdminAccessStatus } from "./admin-access-status";

const ADMIN_APP_SKELETON_ROW_COUNT = 8;
const ADMIN_APP_SKELETON_NAME_WIDTHS = [
  "w-24",
  "w-32",
  "w-20",
  "w-36",
  "w-28",
  "w-40",
  "w-24",
  "w-32",
] as const;

const ADMIN_APP_ROW_CLASS = cn(
  "group relative flex items-center justify-between gap-4 px-4 py-3 text-sm",
);

function AdminAppSkeletonRow({ index }: { index: number }) {
  const nameWidth =
    ADMIN_APP_SKELETON_NAME_WIDTHS[index % ADMIN_APP_SKELETON_NAME_WIDTHS.length];
  return (
    <li
      aria-hidden
      className="pointer-events-none"
      data-testid="admin-app-skeleton-row"
    >
      <div className={ADMIN_APP_ROW_CLASS}>
        <div className="flex min-w-0 flex-1 items-center gap-3">
          <Skeleton className="size-8 shrink-0 rounded-lg" />
          <Skeleton className={cn("h-5", nameWidth)} />
        </div>
        <Skeleton className="h-5 w-16 shrink-0 rounded-full" />
      </div>
    </li>
  );
}

export function AdminAppList({
  integrations,
  loading,
  error,
}: {
  integrations: Integration[];
  loading: boolean;
  error: string | null;
}) {
  const [query, setQuery] = useState("");
  const filtered = useMemo(
    () => filterIntegrations(integrations, query),
    [integrations, query],
  );
  const appNames = useMemo(
    () => filtered.map((item) => item.name),
    [filtered],
  );
  const resourceTypesQuery = useAuthorizationResourceTypesQuery({
    enabled: appNames.length > 0,
  });
  const memberQueries = useAppAuthorizationMembersQueries(appNames);
  const hasDefaultRole = resourceTypeHasDefaultRole(
    resourceTypesQuery.data ?? [],
  );

  return (
    <div className="mt-8">
      <PluginSearchBar query={query} onQueryChange={setQuery} disabled={loading} />

      {loading ? (
        <ul
          className="mt-6 divide-y divide-border rounded-lg border border-border"
          aria-busy="true"
          aria-label="Loading apps"
          data-testid="admin-app-list"
        >
          {Array.from({ length: ADMIN_APP_SKELETON_ROW_COUNT }, (_, index) => (
            <AdminAppSkeletonRow key={index} index={index} />
          ))}
        </ul>
      ) : null}

      {error ? (
        <p className="mt-6 text-sm text-destructive">{error}</p>
      ) : null}

      {!loading && !error && filtered.length === 0 ? (
        <p className="mt-6 text-sm text-muted-foreground">
          {query.trim()
            ? "No apps match that search."
            : "No apps to choose from yet."}
        </p>
      ) : null}

      {!loading && filtered.length > 0 ? (
        <ul
          className="mt-6 divide-y divide-border rounded-lg border border-border"
          data-testid="admin-app-list"
        >
          {filtered.map((integration, index) => {
            const label = getIntegrationLabel(integration);
            const membersQuery = memberQueries[index];
            const listState = membersQuery?.error
              ? summarizeAppAccessList({
                  members: membersQuery.data,
                  membersError: membersQuery.error,
                  hasDefaultRole,
                })
              : resourceTypesQuery.isPending || membersQuery?.isPending
                ? { kind: "loading" as const }
                : summarizeAppAccessList({
                    members: membersQuery?.data,
                    membersError: membersQuery?.error,
                    hasDefaultRole,
                  });
            const readyStatus =
              listState.kind === "ready"
                ? {
                    status: accessListStatus({
                      rule: listState.rule,
                      groups: listState.groups.length,
                      people: listState.people.length,
                    }),
                    groups: listState.groups,
                    people: listState.people,
                  }
                : listState.kind === "unavailable" || listState.kind === "error"
                  ? {
                      status: ACCESS_LIST_STATUS.unavailable,
                      groups: [],
                      people: [],
                    }
                  : null;
            return (
              <li key={integration.name}>
                <div
                  className={cn(
                    ADMIN_APP_ROW_CLASS,
                    listItemInteraction({ pointer: "css" }),
                    nestedInteractiveSuppress.selectableRowSiblingControl,
                  )}
                  data-testid={`admin-app-row-${integration.name}`}
                >
                  <Link
                    to="/admin/apps/$app"
                    params={{ app: integration.name }}
                    data-row-link=""
                    className={cn(
                      "flex min-w-0 flex-1 items-center gap-3 no-underline",
                      "after:absolute after:inset-0 after:z-[1] after:content-['']",
                      "focus-visible:outline-none focus-visible:after:outline-3",
                      "focus-visible:after:outline-offset-2 focus-visible:after:outline-ring",
                    )}
                    onClick={(event) => {
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
                    }}
                  >
                    <IntegrationIcon
                      iconSvg={integration.iconSvg}
                      name={integration.name}
                      displayName={integration.displayName}
                      size="sm"
                    />
                    <span className="min-w-0 truncate font-medium text-foreground">
                      <SearchHighlight
                        text={label}
                        query={query}
                        variant="vivid"
                      />
                    </span>
                  </Link>
                  {readyStatus ? (
                    <div className="relative z-10 shrink-0">
                      <AdminAccessStatus
                        status={readyStatus.status}
                        groups={readyStatus.groups}
                        people={readyStatus.people}
                      />
                    </div>
                  ) : listState.kind === "loading" ? (
                    <Skeleton className="relative z-10 h-5 w-16 shrink-0 rounded-full" />
                  ) : null}
                </div>
              </li>
            );
          })}
        </ul>
      ) : null}
    </div>
  );
}
