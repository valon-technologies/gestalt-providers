import { useMemo, useState } from "react";
import { Link } from "@tanstack/react-router";
import type { AppAuthorizationMember, Integration } from "@/lib/api";
import { isAPIErrorStatus } from "@/lib/api";
import { getIntegrationLabel, filterIntegrations } from "@/lib/integrationSearch";
import IntegrationIcon from "@/components/IntegrationIcon";
import PluginSearchBar from "@/components/PluginSearchBar";
import { SearchHighlight } from "@/components/ui/search-highlight";
import { SpinnerIcon } from "@/components/icons";
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
  inferAppAccessRule,
  partitionAccessEntries,
  resourceTypeHasDefaultRole,
  type AppAccessEntry,
} from "./admin-access";
import { accessListStatus } from "./admin-access-copy";
import { AdminAccessStatus } from "./admin-access-status";

function summaryForApp(
  members: AppAuthorizationMember[] | undefined,
  membersError: unknown,
  hasDefaultRole: boolean,
): {
  status: string;
  groups: AppAccessEntry[];
  people: AppAccessEntry[];
} | null {
  if (membersError && isAPIErrorStatus(membersError, 403)) {
    return hasDefaultRole
      ? {
          status: accessListStatus({ rule: "everyone", groups: 0, people: 0 }),
          groups: [],
          people: [],
        }
      : null;
  }
  if (!members) return null;
  const { groups, people } = partitionAccessEntries(members);
  const rule = inferAppAccessRule({ hasDefaultRole, members });
  return {
    status: accessListStatus({
      rule,
      groups: groups.length,
      people: people.length,
    }),
    groups,
    people,
  };
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
        <p className="mt-6 flex items-center gap-1.5 text-sm text-muted-foreground">
          <SpinnerIcon className="size-4 animate-spin" aria-hidden />
          Loading apps…
        </p>
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
            const summary = summaryForApp(
              membersQuery?.data,
              membersQuery?.error,
              hasDefaultRole,
            );
            return (
              <li key={integration.name}>
                <div
                  className={cn(
                    "relative flex items-center justify-between gap-4 px-4 py-3 text-sm",
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
                  {summary ? (
                    <div className="relative z-10 shrink-0">
                      <AdminAccessStatus
                        status={summary.status}
                        groups={summary.groups}
                        people={summary.people}
                      />
                    </div>
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
