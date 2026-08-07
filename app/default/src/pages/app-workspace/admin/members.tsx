import { Link as RouterLink } from "@tanstack/react-router";
import { useMemo, useState } from "react";
import {
  APIError,
  isAPIErrorStatus,
  type AppAuthorizationMember,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Link } from "@/components/ui/link";
import {
  MemberAccess,
  type MemberAccessPerson,
  type MemberAccessRole,
} from "@/components/ui/member-access";
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
  memberLabel,
  memberMeta,
  partitionAppMembers,
} from "@/features/app-workspace/app-workspace-shared";
import { useAppAuthorizationMembersQuery } from "@/lib/queries";

const DEFAULT_ROLES: MemberAccessRole[] = [
  { value: "admin", label: "Admin" },
  { value: "viewer", label: "Viewer" },
];

function toMemberAccessPerson(
  member: AppAuthorizationMember,
  index: number,
): MemberAccessPerson {
  const label = memberLabel(member);
  const meta = memberMeta(member);
  const shadowed =
    !member.effective && member.shadowedBy
      ? `Shadowed by ${member.shadowedBy}`
      : null;
  const subtitle = [meta || null, shadowed].filter(Boolean).join(" · ");
  return {
    id:
      member.subjectId?.trim() ||
      member.selectorValue?.trim() ||
      member.email?.trim() ||
      `member-${index}`,
    name: label,
    email: subtitle || member.email?.trim() || undefined,
    role: member.role?.trim() || "viewer",
    locked: member.mutable === false,
  };
}

function rolesForMembers(members: AppAuthorizationMember[]): MemberAccessRole[] {
  const known = new Map(DEFAULT_ROLES.map((role) => [role.value, role]));
  for (const member of members) {
    const value = member.role?.trim();
    if (!value || known.has(value)) continue;
    known.set(value, {
      value,
      label: value.charAt(0).toUpperCase() + value.slice(1),
    });
  }
  return Array.from(known.values());
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
      ? membersQuery.error instanceof APIError
        ? membersQuery.error.message
        : membersQuery.error instanceof Error
          ? membersQuery.error.message
          : "Failed to load members"
      : null;

  const [inviteValue, setInviteValue] = useState("");
  const [inviteRole, setInviteRole] = useState("viewer");

  const { peopleMembers, serviceAccountMembers } = useMemo(() => {
    const partitioned = partitionAppMembers(members);
    return {
      peopleMembers: partitioned.people,
      serviceAccountMembers: partitioned.serviceAccounts,
    };
  }, [members]);

  const people = useMemo(
    () => peopleMembers.map(toMemberAccessPerson),
    [peopleMembers],
  );
  const serviceAccounts = useMemo(
    () => serviceAccountMembers.map(toMemberAccessPerson),
    [serviceAccountMembers],
  );
  const roles = useMemo(() => rolesForMembers(members), [members]);

  const showRoster =
    !membersLoading && !membersForbidden && !membersError;

  return (
    <section aria-label="Members">
      <PageHeader>
        <PageHeaderContent size="md">
          <PageHeaderTitle>Members</PageHeaderTitle>
          <PageHeaderDescription>
            People and service accounts with an authorization grant on this app.
            This roster is read-only — use{" "}
            <Link asChild>
              <RouterLink
                to={AUTHORIZATION_DOCS_PATH}
                hash={AUTHORIZATION_DOCS_GRANT_HASH}
              >
                How to grant access
              </RouterLink>
            </Link>{" "}
            to add or change access. To create or edit service account identity
            records, use{" "}
            <Link asChild>
              <RouterLink
                to="/apps/$app/admin/agent-identities"
                params={{ app }}
              >
                Agent identities
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
        <p className="mt-5 text-sm text-ember-500">{membersError}</p>
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
            ) : null}
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
          </section>

          <section aria-label="Service accounts" className="flex flex-col gap-4">
            <SectionHeader>
              <SectionHeaderContent size="sm">
                <SectionHeaderTitle
                  as="h2"
                  className="inline-flex items-baseline gap-1.5"
                >
                  Service accounts
                  <Badge variant="secondary" size="sm">
                    {serviceAccountMembers.length}
                  </Badge>
                </SectionHeaderTitle>
              </SectionHeaderContent>
            </SectionHeader>
            {serviceAccountMembers.length === 0 ? (
              <p className="text-sm text-muted-foreground">
                No service accounts have been granted access yet.
              </p>
            ) : (
              <MemberAccess
                people={serviceAccounts}
                roles={roles}
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
