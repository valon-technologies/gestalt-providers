import { useMemo } from "react";
import { APIError, isAPIErrorStatus } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { SpinnerIcon } from "@/components/icons";
import { useAppWorkspace } from "@/features/app-workspace/app-workspace-context";
import { SummaryStat } from "@/features/app-workspace/app-workspace-shared";
import { useAppAdminIdentitiesQuery } from "@/lib/queries";

export default function AppAdminAgentIdentitiesPage() {
  const { app } = useAppWorkspace();
  const identitiesQuery = useAppAdminIdentitiesQuery(app);
  const identities = identitiesQuery.data ?? [];
  const loading = identitiesQuery.isPending;
  const forbidden =
    identitiesQuery.isError && isAPIErrorStatus(identitiesQuery.error, 403);
  const loadError =
    identitiesQuery.isError && !forbidden
      ? identitiesQuery.error instanceof APIError
        ? identitiesQuery.error.message
        : identitiesQuery.error instanceof Error
          ? identitiesQuery.error.message
          : "Failed to load identities"
      : null;

  const counts = useMemo(() => {
    const effective = identities.filter((row) => row.effective !== false).length;
    const staticCount = identities.filter((row) => row.source === "static").length;
    const dynamicCount = identities.filter(
      (row) => row.source === "dynamic",
    ).length;
    return { effective, staticCount, dynamicCount };
  }, [identities]);

  return (
    <section aria-label="Agent identities">
      <PageHeader>
        <PageHeaderContent size="md">
          <PageHeaderTitle>Agent identities</PageHeaderTitle>
          <PageHeaderDescription>
            Service accounts with an authorization grant on this app — typically
            the <code className="font-mono text-xs">runAs</code> subject for
            schedules and automation. Grants are defined in deployment policy.
          </PageHeaderDescription>
        </PageHeaderContent>
      </PageHeader>

      {!loading && !forbidden && !loadError ? (
        <div className="mt-5 grid grid-cols-2 gap-3 sm:grid-cols-3">
          <SummaryStat label="Effective" value={String(counts.effective)} />
          <SummaryStat label="Static" value={String(counts.staticCount)} />
          <SummaryStat label="Dynamic" value={String(counts.dynamicCount)} />
        </div>
      ) : null}

      {loading ? (
        <p className="mt-5 flex items-center gap-1.5 text-sm text-muted-foreground">
          <SpinnerIcon className="size-4 animate-spin" aria-hidden />
          Loading identities…
        </p>
      ) : null}

      {forbidden ? (
        <p
          className="mt-5 text-sm text-muted-foreground"
          data-testid="app-admin-access-denied"
        >
          Agent identities require app authorization admin access.
        </p>
      ) : null}

      {loadError ? (
        <p className="mt-5 text-sm text-ember-500">{loadError}</p>
      ) : null}

      {!loading && !forbidden && !loadError && identities.length === 0 ? (
        <p className="mt-5 text-sm text-muted-foreground">
          No agent identities have a grant for this app yet.
        </p>
      ) : null}

      {!loading && !forbidden && !loadError && identities.length > 0 ? (
        <ul
          className="mt-5 divide-y divide-border rounded-lg border border-border"
          data-testid="app-agent-identities-list"
        >
          {identities.map((identity, index) => (
            <li
              key={`${identity.subjectId}:${identity.role}:${identity.source}:${index}`}
              className="flex flex-col gap-2 px-4 py-3 sm:flex-row sm:items-center sm:justify-between"
            >
              <div className="min-w-0">
                <p className="truncate text-sm font-medium text-foreground">
                  {identity.displayName || identity.subjectId}
                </p>
                <p className="mt-0.5 font-mono text-xs text-muted-foreground">
                  {identity.subjectId}
                </p>
                {identity.effective === false && identity.shadowedBy ? (
                  <p className="mt-1 text-xs text-muted-foreground">
                    Shadowed by {identity.shadowedBy}
                  </p>
                ) : null}
              </div>
              <div className="flex flex-wrap gap-1.5">
                <Badge variant="secondary" size="sm">
                  {identity.role || "role"}
                </Badge>
                <Badge
                  variant={identity.source === "static" ? "muted" : "outline"}
                  size="sm"
                >
                  {identity.source || "unknown"}
                  {identity.mutable === false ? " · locked" : ""}
                </Badge>
                <Badge
                  variant={identity.effective !== false ? "success" : "warning"}
                  size="sm"
                >
                  {identity.effective !== false ? "Effective" : "Shadowed"}
                </Badge>
              </div>
            </li>
          ))}
        </ul>
      ) : null}
    </section>
  );
}
