import { isAPIErrorStatus } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Link } from "@/components/ui/link";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import { SpinnerIcon } from "@/components/icons";
import { useAppWorkspace } from "@/features/app-workspace/app-workspace-context";
import {
  SERVICE_ACCOUNTS_COPY,
  serviceAccountsLoadErrorMessage,
  toAgentIdentityRowView,
} from "@/features/app-workspace/app-agent-identity-presentation";
import {
  AUTHORIZATION_DOCS_PATH,
  AUTHORIZATION_DOCS_SERVICE_ACCOUNTS_HASH,
} from "@/features/app-workspace/operations/handoffs";
import { useAppAdminIdentitiesQuery } from "@/lib/queries";
import { Link as RouterLink } from "@tanstack/react-router";
import { useMemo } from "react";

export default function AppAdminAgentIdentitiesPage() {
  const { app } = useAppWorkspace();
  const identitiesQuery = useAppAdminIdentitiesQuery(app);
  const identities = identitiesQuery.data ?? [];
  const loading = identitiesQuery.isPending;
  const forbidden =
    identitiesQuery.isError && isAPIErrorStatus(identitiesQuery.error, 403);
  const loadError =
    identitiesQuery.isError && !forbidden
      ? serviceAccountsLoadErrorMessage(identitiesQuery.error)
      : null;

  const rows = useMemo(
    () => identities.map((identity, index) => toAgentIdentityRowView(identity, index)),
    [identities],
  );

  return (
    <section aria-label={SERVICE_ACCOUNTS_COPY.sectionAriaLabel}>
      <PageHeader>
        <PageHeaderContent size="md">
          <PageHeaderTitle>{SERVICE_ACCOUNTS_COPY.title}</PageHeaderTitle>
          <PageHeaderDescription>
            {SERVICE_ACCOUNTS_COPY.descriptionBeforeLink}{" "}
            <Link asChild>
              <RouterLink
                to={AUTHORIZATION_DOCS_PATH}
                hash={AUTHORIZATION_DOCS_SERVICE_ACCOUNTS_HASH}
              >
                {SERVICE_ACCOUNTS_COPY.docsLinkLabel}
              </RouterLink>
            </Link>{" "}
            {SERVICE_ACCOUNTS_COPY.descriptionAfterLink}
          </PageHeaderDescription>
        </PageHeaderContent>
      </PageHeader>

      {loading ? (
        <p className="mt-5 flex items-center gap-1.5 text-sm text-muted-foreground">
          <SpinnerIcon className="size-4 animate-spin" aria-hidden />
          {SERVICE_ACCOUNTS_COPY.loading}
        </p>
      ) : null}

      {forbidden ? (
        <p
          className="mt-5 text-sm text-muted-foreground"
          data-testid="app-admin-access-denied"
        >
          {SERVICE_ACCOUNTS_COPY.forbidden}
        </p>
      ) : null}

      {loadError ? (
        <p className="mt-5 text-sm text-destructive">{loadError}</p>
      ) : null}

      {!loading && !forbidden && !loadError && rows.length === 0 ? (
        <p className="mt-5 text-sm text-muted-foreground">
          {SERVICE_ACCOUNTS_COPY.empty}
        </p>
      ) : null}

      {!loading && !forbidden && !loadError && rows.length > 0 ? (
        <ul
          className="mt-5 divide-y divide-border rounded-lg border border-border"
          data-testid={SERVICE_ACCOUNTS_COPY.listTestId}
        >
          {rows.map((row) => (
            <li
              key={row.key}
              className="flex flex-col gap-2 px-4 py-3 sm:flex-row sm:items-center sm:justify-between"
            >
              <div className="min-w-0">
                <p className="truncate text-sm font-medium text-foreground">
                  {row.title}
                </p>
                {row.showAccountId ? (
                  <p className="mt-0.5 font-mono text-xs text-muted-foreground">
                    Account ID · {row.accountId}
                  </p>
                ) : null}
                {row.exception ? (
                  <p className="mt-1 text-xs text-muted-foreground">
                    {row.exception.detail}
                  </p>
                ) : null}
              </div>
              <div className="flex flex-wrap gap-1.5">
                <Badge variant="secondary" size="default">
                  {row.roleLabel}
                </Badge>
                {row.exception ? (
                  <Badge variant="warning" size="default">
                    {row.exception.label}
                  </Badge>
                ) : null}
              </div>
            </li>
          ))}
        </ul>
      ) : null}
    </section>
  );
}
