import { isAPIErrorStatus } from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Link } from "@/components/ui/link";
import { MemberAccess } from "@/components/ui/member-access";
import {
  AUTHORIZATION_DOCS_GRANT_HASH,
  AUTHORIZATION_DOCS_PATH,
} from "@/features/app-workspace/operations/handoffs";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  SectionHeader,
  SectionHeaderContent,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { SpinnerIcon } from "@/components/icons";
import { useAppWorkspace } from "@/features/app-workspace/app-workspace-context";
import {
  rolesForMembers,
  toMemberAccessPerson,
} from "@/features/app-workspace/app-member-access";
import {
  SERVICE_ACCOUNTS_COPY,
  SERVICE_ACCOUNTS_ROUTE,
} from "@/features/app-workspace/app-agent-identity-presentation";
import { partitionAppMembers } from "@/features/app-workspace/app-workspace-shared";
import { useAppAuthorizationMembersQuery } from "@/lib/queries";
import { Link as RouterLink } from "@tanstack/react-router";
import { useMemo, useState } from "react";

function membersLoadErrorMessage(_error: unknown): string {
  return "Couldn’t load members. Check your connection and refresh the page.";
}

export default function AppAdminMembersPage() {
  const { app } = useAppWorkspace();
  const membersQuery = useAppAuthorizationMembersQuery(app);
  const members = membersQuery.data ?? [];
  const membersLoading = membersQuery.isPending;
  const membersForbidden =
    membersQuery.isError && isAPIErrorStatus(membersQuery.error, 403);
  const membersError =
    membersQuery.isError && !membersForbidden
      ? membersLoadErrorMessage(membersQuery.error)
      : null;

  const [inviteValue, setInviteValue] = useState("");
  const [inviteRole, setInviteRole] = useState("viewer");

  const peopleMembers = useMemo(
    () => partitionAppMembers(members).people,
    [members],
  );
  const people = useMemo(
    () => peopleMembers.map(toMemberAccessPerson),
    [peopleMembers],
  );
  const roles = useMemo(() => rolesForMembers(peopleMembers), [peopleMembers]);

  const showRoster =
    !membersLoading && !membersForbidden && !membersError;

  return (
    <section aria-label="Members">
      <PageHeader>
        <PageHeaderContent size="md">
          <PageHeaderTitle>Members</PageHeaderTitle>
          <PageHeaderDescription>
            People and groups with an authorization grant on this app. This
            roster is read-only — use{" "}
            <Link asChild>
              <RouterLink
                to={AUTHORIZATION_DOCS_PATH}
                hash={AUTHORIZATION_DOCS_GRANT_HASH}
              >
                How to grant access
              </RouterLink>
            </Link>{" "}
            to add or change access. Service accounts appear under{" "}
            <Link asChild>
              <RouterLink to={SERVICE_ACCOUNTS_ROUTE} params={{ app }}>
                {SERVICE_ACCOUNTS_COPY.navLabel}
              </RouterLink>
            </Link>
            .
          </PageHeaderDescription>
        </PageHeaderContent>
      </PageHeader>

      {membersLoading ? (
        <p className="mt-5 flex items-center gap-1.5 text-sm text-muted-foreground">
          <SpinnerIcon className="size-4 animate-spin" aria-hidden />
          Loading members…
        </p>
      ) : null}

      {membersForbidden ? (
        <p
          className="mt-5 text-sm text-muted-foreground"
          data-testid="app-admin-access-denied"
        >
          You don&apos;t have permission to view this roster. See{" "}
          <Link asChild>
            <RouterLink
              to={AUTHORIZATION_DOCS_PATH}
              hash={AUTHORIZATION_DOCS_GRANT_HASH}
            >
              How to grant access
            </RouterLink>
          </Link>{" "}
          for admin requirements, or ask a Gestalt admin.
        </p>
      ) : null}

      {membersError ? (
        <p className="mt-5 text-sm text-destructive">{membersError}</p>
      ) : null}

      {showRoster ? (
        <div className="mt-8 flex flex-col gap-10" data-testid="app-members-list">
          <section aria-label="People" className="flex flex-col gap-4">
            <SectionHeader>
              <SectionHeaderContent size="sm">
                <SectionHeaderTitle
                  as="h2"
                  className="inline-flex items-baseline gap-1.5"
                >
                  People
                  <Badge variant="secondary" size="sm">
                    {peopleMembers.length}
                  </Badge>
                </SectionHeaderTitle>
              </SectionHeaderContent>
            </SectionHeader>
            {peopleMembers.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                No people have been granted access yet.
              </p>
            ) : (
              <MemberAccess
                people={people}
                roles={roles}
                invite={{
                  value: inviteValue,
                  role: inviteRole,
                  onValueChange: setInviteValue,
                  onRoleChange: setInviteRole,
                  onInvite: () => {},
                  searchPeople: async () => [],
                  allowCustomValue: true,
                  placeholder: "Select person",
                }}
                onRoleChange={() => {}}
                onRemove={() => {}}
                disabled
              />
            )}
          </section>
        </div>
      ) : null}
    </section>
  );
}
