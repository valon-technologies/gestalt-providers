import { managedIdentityLocalId } from "@/lib/managed-identity-paths";
import { Link as RouterLink } from "@tanstack/react-router";
import { useEffect, useState } from "react";
import {
  getManagedIdentities,
  getManagedIdentityGrants,
  type ManagedIdentity,
  type ManagedIdentityGrant,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Link } from "@/components/ui/link";
import {
  PageHeader,
  PageHeaderActions,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { SpinnerIcon } from "@/components/icons";
import { useAppWorkspace } from "@/features/app-workspace/app-workspace-context";

type AppAccessGrant = {
  identity: ManagedIdentity;
  grant: ManagedIdentityGrant;
};

export default function AppAdminAgentIdentitiesPage() {
  const { app } = useAppWorkspace();
  const [accessGrants, setAccessGrants] = useState<AppAccessGrant[]>([]);
  const [accessLoading, setAccessLoading] = useState(true);
  const [accessError, setAccessError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    setAccessLoading(true);
    setAccessError(null);

    getManagedIdentities()
      .then(async (identities) => {
        const grants = await Promise.all(
          identities.map(async (identity) => {
            try {
              const identityGrants = await getManagedIdentityGrants(
                identity.subjectId,
              );
              return identityGrants
                .filter((grant) => grant.plugin === app)
                .map((grant) => ({ identity, grant }));
            } catch {
              return [] as AppAccessGrant[];
            }
          }),
        );
        if (!active) return;
        setAccessGrants(grants.flat());
      })
      .catch((err) => {
        if (!active) return;
        setAccessError(
          err instanceof Error ? err.message : "Failed to load access",
        );
        setAccessGrants([]);
      })
      .finally(() => {
        if (active) setAccessLoading(false);
      });

    return () => {
      active = false;
    };
  }, [app]);

  return (
    <section aria-label="Agent identities">
      <PageHeader>
        <PageHeaderContent>
          <PageHeaderTitle>Agent identities</PageHeaderTitle>
          <PageHeaderDescription>
            Managed identities with an authorization grant for this app —
            usually the <code className="font-mono text-xs">runAs</code> subject
            for schedules.
          </PageHeaderDescription>
        </PageHeaderContent>
        <PageHeaderActions>
          <Link asChild>
            <RouterLink to="/settings/identities">Manage identities</RouterLink>
          </Link>
        </PageHeaderActions>
      </PageHeader>

      {accessLoading ? (
        <p className="mt-5 flex items-center gap-1.5 text-sm text-muted-foreground">
          <SpinnerIcon className="size-4 animate-spin" aria-hidden />
          Loading access…
        </p>
      ) : null}

      {accessError ? (
        <p className="mt-5 text-sm text-ember-500">{accessError}</p>
      ) : null}

      {!accessLoading && !accessError && accessGrants.length === 0 ? (
        <p className="mt-5 text-sm text-muted-foreground">
          No agent identities have a grant for this app yet.
        </p>
      ) : null}

      {!accessLoading && accessGrants.length > 0 ? (
        <ul className="mt-5 divide-y divide-border rounded-lg border border-border">
          {accessGrants.map(({ identity, grant }) => (
            <li
              key={`${identity.subjectId}:${grant.plugin}`}
              className="flex flex-col gap-1 px-4 py-3 sm:flex-row sm:items-center sm:justify-between"
            >
              <div className="min-w-0">
                <Link asChild>
                  <RouterLink
                    to="/settings/identities/$identityLocalId"
                    params={{
                      identityLocalId: managedIdentityLocalId(identity.subjectId),
                    }}
                  >
                    {identity.displayName || identity.subjectId}
                  </RouterLink>
                </Link>
                <p className="mt-0.5 font-mono text-xs text-muted-foreground">
                  {identity.subjectId}
                </p>
              </div>
              <Badge variant="secondary" size="sm">
                {grant.role}
                {grant.source === "static" ? " · static" : ""}
              </Badge>
            </li>
          ))}
        </ul>
      ) : null}
    </section>
  );
}
