import { useMemo } from "react";
import { APIError, isAPIErrorStatus } from "@/lib/api";
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
import {
  memberLabel,
  memberMeta,
  SummaryStat,
} from "@/features/app-workspace/app-workspace-shared";
import { useAppAuthorizationMembersQuery } from "@/lib/queries";

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

  const memberCounts = useMemo(() => {
    const effective = members.filter((row) => row.effective).length;
    const staticCount = members.filter((row) => row.source === "static").length;
    const dynamicCount = members.filter((row) => row.source === "dynamic").length;
    const shadowed = members.filter(
      (row) => row.source === "dynamic" && !row.effective,
    ).length;
    return { effective, staticCount, dynamicCount, shadowed };
  }, [members]);

  return (
    <section aria-label="Members">
      <PageHeader>
        <PageHeaderContent>
          <PageHeaderTitle>Members</PageHeaderTitle>
          <PageHeaderDescription>
            Who has access to this app (static policy + dynamic grants). Same
            roster as the admin authorization tab.
          </PageHeaderDescription>
        </PageHeaderContent>
        <PageHeaderActions>
          <Link href="/admin/" underlineVariant="always">
            Open admin authorization
          </Link>
        </PageHeaderActions>
      </PageHeader>

      {!membersLoading && !membersForbidden && !membersError ? (
        <div className="mt-5 grid grid-cols-2 gap-3 sm:grid-cols-4">
          <SummaryStat label="Effective" value={String(memberCounts.effective)} />
          <SummaryStat label="Static" value={String(memberCounts.staticCount)} />
          <SummaryStat label="Dynamic" value={String(memberCounts.dynamicCount)} />
          <SummaryStat label="Shadowed" value={String(memberCounts.shadowed)} />
        </div>
      ) : null}

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
          Member roster requires app authorization admin access. Manage members in{" "}
          <Link href="/admin/" underlineVariant="always">
            /admin/
          </Link>{" "}
          or ask a Gestalt admin.
        </p>
      ) : null}

      {membersError ? (
        <p className="mt-5 text-sm text-ember-500">{membersError}</p>
      ) : null}

      {!membersLoading && !membersForbidden && !membersError && members.length === 0 ? (
        <p className="mt-5 text-sm text-muted-foreground">No members found for this app.</p>
      ) : null}

      {!membersLoading && members.length > 0 ? (
        <ul
          className="mt-5 divide-y divide-border rounded-lg border border-border"
          data-testid="app-members-list"
        >
          {members.map((member, index) => (
            <li
              key={`${memberLabel(member)}:${member.role}:${index}`}
              className="flex flex-col gap-2 px-4 py-3 sm:flex-row sm:items-center sm:justify-between"
            >
              <div className="min-w-0">
                <p className="truncate text-sm font-medium text-foreground">
                  {memberLabel(member)}
                </p>
                {memberMeta(member) ? (
                  <p className="mt-0.5 font-mono text-xs text-muted-foreground">
                    {memberMeta(member)}
                  </p>
                ) : null}
                {!member.effective && member.shadowedBy ? (
                  <p className="mt-1 text-xs text-muted-foreground">
                    Shadowed by {member.shadowedBy}
                  </p>
                ) : null}
              </div>
              <div className="flex flex-wrap gap-1.5">
                <Badge variant="secondary" size="sm">
                  {member.role || "role"}
                </Badge>
                <Badge
                  variant={member.source === "static" ? "muted" : "outline"}
                  size="sm"
                >
                  {member.source || "unknown"}
                  {member.mutable === false ? " · locked" : ""}
                </Badge>
                <Badge
                  variant={member.effective ? "success" : "warning"}
                  size="sm"
                >
                  {member.effective ? "Effective" : "Shadowed"}
                </Badge>
              </div>
            </li>
          ))}
        </ul>
      ) : null}
    </section>
  );
}
