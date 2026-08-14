import { useMemo, useState } from "react";
import { toast } from "sonner";
import type { AppAuthorizationMember } from "@/lib/api";
import { isAPIErrorStatus } from "@/lib/api";
import { Button } from "@/components/ui/button";
import {
  PageHeader,
  PageHeaderContent,
  PageHeaderDescription,
  PageHeaderTitle,
} from "@/components/ui/page-header";
import {
  SectionHeader,
  SectionHeaderActions,
  SectionHeaderContent,
  SectionHeaderTitle,
} from "@/components/ui/section-header";
import { SpinnerIcon } from "@/components/icons";
import { useDocumentTitle } from "@/hooks/use-document-title";
import {
  DEFAULT_PLATFORM_ADMIN_ROLE,
  groupRelationshipTupleForResource,
  parseGroupSelector,
  partitionAccessEntries,
  personRelationshipTupleForResource,
  relationshipTupleForMemberOnResource,
} from "@/features/admin-access/admin-access";
import {
  ADD_GROUP_DIALOG_TITLE,
  ADD_GROUP_FIELD_HINT,
  ADD_GROUP_FIELD_LABEL,
  ADD_GROUP_LABEL,
  ADD_PERSON_DIALOG_TITLE,
  ADD_PERSON_FIELD_HINT,
  ADD_PERSON_FIELD_LABEL,
  ADD_PERSON_LABEL,
  GROUPS_SECTION_TITLE,
  PEOPLE_SECTION_TITLE,
  PLATFORM_ADMINS_ADD_GROUP_DESCRIPTION,
  PLATFORM_ADMINS_ADD_PERSON_DESCRIPTION,
  PLATFORM_ADMINS_EMPTY_GROUPS,
  PLATFORM_ADMINS_EMPTY_PEOPLE,
  PLATFORM_ADMINS_FORBIDDEN,
  PLATFORM_ADMINS_LOAD_ERROR,
  PLATFORM_ADMINS_PAGE_DESCRIPTION,
  PLATFORM_ADMINS_PAGE_TITLE,
  PLATFORM_ADMINS_SAVED,
  PLATFORM_ADMINS_UNAVAILABLE,
  REMOVE_ADMIN_CONFIRM_DESCRIPTION,
} from "@/features/admin-access/admin-access-copy";
import {
  AccessEntryRow,
  AddAccessDialog,
  accessActionErrorMessage,
} from "@/features/admin-access/admin-access-roster";
import {
  useAddPlatformAdminMutation,
  useAdminPlatformAdminsQuery,
  useDeletePlatformAdminMutation,
} from "@/lib/queries";

export default function AdminPlatformAdminsPage() {
  useDocumentTitle(PLATFORM_ADMINS_PAGE_TITLE);
  const rosterQuery = useAdminPlatformAdminsQuery();
  const addMutation = useAddPlatformAdminMutation();
  const deleteMutation = useDeletePlatformAdminMutation();
  const [groupOpen, setGroupOpen] = useState(false);
  const [personOpen, setPersonOpen] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);

  const resource = rosterQuery.data?.resource;
  const role =
    rosterQuery.data?.role?.trim() || DEFAULT_PLATFORM_ADMIN_ROLE;
  const members = rosterQuery.data?.members ?? [];
  const { groups, people } = useMemo(
    () => partitionAccessEntries(members),
    [members],
  );
  const busy = addMutation.isPending || deleteMutation.isPending;
  const forbidden =
    rosterQuery.isError && isAPIErrorStatus(rosterQuery.error, 403);
  const unavailable =
    rosterQuery.isError && isAPIErrorStatus(rosterQuery.error, 503);
  const loadError =
    rosterQuery.isError && !forbidden && !unavailable
      ? accessActionErrorMessage(rosterQuery.error, PLATFORM_ADMINS_LOAD_ERROR)
      : null;

  async function handleRemove(member: AppAuthorizationMember) {
    if (!resource) return;
    const tuple = relationshipTupleForMemberOnResource(resource, member);
    if (!tuple) return;
    setActionError(null);
    try {
      await deleteMutation.mutateAsync(tuple);
    } catch (error) {
      setActionError(accessActionErrorMessage(error, "Couldn't remove that admin."));
    }
  }

  return (
    <div className="space-y-12">
      <PageHeader>
        <PageHeaderContent size="md">
          <PageHeaderTitle>{PLATFORM_ADMINS_PAGE_TITLE}</PageHeaderTitle>
          <PageHeaderDescription>
            {PLATFORM_ADMINS_PAGE_DESCRIPTION}
          </PageHeaderDescription>
        </PageHeaderContent>
      </PageHeader>

      {rosterQuery.isPending ? (
        <p className="flex items-center gap-1.5 text-sm text-muted-foreground">
          <SpinnerIcon
            className="size-4 motion-safe:animate-spin motion-reduce:animate-none"
            aria-hidden
          />
          Loading platform admins…
        </p>
      ) : null}

      {forbidden ? (
        <p className="text-sm text-muted-foreground">{PLATFORM_ADMINS_FORBIDDEN}</p>
      ) : null}

      {unavailable ? (
        <p className="text-sm text-muted-foreground">{PLATFORM_ADMINS_UNAVAILABLE}</p>
      ) : null}

      {loadError ? (
        <p className="text-sm text-destructive" role="alert">
          {loadError}
        </p>
      ) : null}

      {actionError ? (
        <p className="text-sm text-destructive" role="alert">
          {actionError}
        </p>
      ) : null}

      {!rosterQuery.isPending && !forbidden && !unavailable && !loadError ? (
        <>
          <section aria-labelledby="admin-platform-groups">
            <SectionHeader>
              <SectionHeaderContent>
                <SectionHeaderTitle id="admin-platform-groups">
                  {GROUPS_SECTION_TITLE}
                </SectionHeaderTitle>
              </SectionHeaderContent>
              <SectionHeaderActions>
                <Button
                  type="button"
                  size="sm"
                  onClick={() => setGroupOpen(true)}
                  disabled={busy || !resource}
                >
                  {ADD_GROUP_LABEL}
                </Button>
              </SectionHeaderActions>
            </SectionHeader>
            {groups.length === 0 ? (
              <p className="mt-3 text-sm text-muted-foreground">
                {PLATFORM_ADMINS_EMPTY_GROUPS}
              </p>
            ) : (
              <ul className="mt-3 divide-y divide-border rounded-lg border border-border">
                {groups.map((entry, index) => (
                  <AccessEntryRow
                    key={`${entry.label}:${entry.role}:${index}`}
                    entry={entry}
                    busy={busy}
                    testId="admin-platform-admin-entry"
                    confirmDescription={REMOVE_ADMIN_CONFIRM_DESCRIPTION}
                    onRemove={() => void handleRemove(entry.member)}
                  />
                ))}
              </ul>
            )}
          </section>

          <section aria-labelledby="admin-platform-people">
            <SectionHeader>
              <SectionHeaderContent>
                <SectionHeaderTitle id="admin-platform-people">
                  {PEOPLE_SECTION_TITLE}
                </SectionHeaderTitle>
              </SectionHeaderContent>
              <SectionHeaderActions>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  onClick={() => setPersonOpen(true)}
                  disabled={busy || !resource}
                >
                  {ADD_PERSON_LABEL}
                </Button>
              </SectionHeaderActions>
            </SectionHeader>
            {people.length === 0 ? (
              <p className="mt-3 text-sm text-muted-foreground">
                {PLATFORM_ADMINS_EMPTY_PEOPLE}
              </p>
            ) : (
              <ul className="mt-3 divide-y divide-border rounded-lg border border-border">
                {people.map((entry, index) => (
                  <AccessEntryRow
                    key={`${entry.label}:${entry.role}:${index}`}
                    entry={entry}
                    busy={busy}
                    testId="admin-platform-admin-entry"
                    confirmDescription={REMOVE_ADMIN_CONFIRM_DESCRIPTION}
                    onRemove={() => void handleRemove(entry.member)}
                  />
                ))}
              </ul>
            )}
          </section>
        </>
      ) : null}

      <AddAccessDialog
        open={groupOpen}
        title={ADD_GROUP_DIALOG_TITLE}
        description={PLATFORM_ADMINS_ADD_GROUP_DESCRIPTION}
        fieldLabel={ADD_GROUP_FIELD_LABEL}
        fieldHint={ADD_GROUP_FIELD_HINT}
        inputType="text"
        placeholder="eng"
        confirmLabel={ADD_GROUP_LABEL}
        busy={addMutation.isPending}
        onOpenChange={setGroupOpen}
        onSubmit={async (value) => {
          if (!resource) return;
          await addMutation.mutateAsync(
            groupRelationshipTupleForResource(resource, value, role),
          );
          toast.success(
            PLATFORM_ADMINS_SAVED(parseGroupSelector(value).id || value),
          );
        }}
      />
      <AddAccessDialog
        open={personOpen}
        title={ADD_PERSON_DIALOG_TITLE}
        description={PLATFORM_ADMINS_ADD_PERSON_DESCRIPTION}
        fieldLabel={ADD_PERSON_FIELD_LABEL}
        fieldHint={ADD_PERSON_FIELD_HINT}
        inputType="email"
        placeholder="name@example.com"
        confirmLabel={ADD_PERSON_LABEL}
        busy={addMutation.isPending}
        onOpenChange={setPersonOpen}
        onSubmit={async (value) => {
          if (!resource) return;
          await addMutation.mutateAsync(
            personRelationshipTupleForResource(resource, value, role),
          );
          toast.success(PLATFORM_ADMINS_SAVED(value));
        }}
      />
    </div>
  );
}
