import type { AppAuthorizationMember } from "@/lib/api";

export const APP_SECTION_CARD =
  "rounded-lg border border-border bg-card p-6";

/** Canonical subject-id prefix for service-account principals. */
export const SERVICE_ACCOUNT_SUBJECT_PREFIX = "service_account:";

/**
 * Strip the wire `service_account:` prefix for user-facing account ids.
 * Non-prefixed values pass through unchanged.
 */
export function serviceAccountLocalId(subjectId: string): string {
  const trimmed = subjectId.trim();
  if (
    trimmed.toLowerCase().startsWith(SERVICE_ACCOUNT_SUBJECT_PREFIX)
  ) {
    return trimmed.slice(SERVICE_ACCOUNT_SUBJECT_PREFIX.length);
  }
  return trimmed;
}

/** App-access grant principal kind — derived from canonical subject id. */
export type AppMemberSubjectKind = "person" | "service_account";

/**
 * Classify a member for People vs Service accounts sections.
 * Service accounts are `service_account:…` subjects; everything else is a person
 * (user UUID, email-form user id, or unresolved human selector).
 */
export function appMemberSubjectKind(
  member: AppAuthorizationMember,
): AppMemberSubjectKind {
  const id = (
    member.subjectId?.trim() ||
    member.selectorValue?.trim() ||
    ""
  ).toLowerCase();
  if (id.startsWith(SERVICE_ACCOUNT_SUBJECT_PREFIX)) return "service_account";
  return "person";
}

/** Split roster rows into People vs Service accounts for Members sections. */
export function partitionAppMembers(members: AppAuthorizationMember[]): {
  people: AppAuthorizationMember[];
  serviceAccounts: AppAuthorizationMember[];
} {
  const people: AppAuthorizationMember[] = [];
  const serviceAccounts: AppAuthorizationMember[] = [];
  for (const member of members) {
    if (appMemberSubjectKind(member) === "service_account") {
      serviceAccounts.push(member);
    } else {
      people.push(member);
    }
  }
  return { people, serviceAccounts };
}

export function memberLabel(member: AppAuthorizationMember): string {
  if (member.email?.trim()) return member.email.trim();
  if (member.selectorValue?.trim()) return member.selectorValue.trim();
  if (member.subjectId?.trim()) return member.subjectId.trim();
  return "Unknown";
}

export function memberMeta(member: AppAuthorizationMember): string {
  if (member.selectorKind === "subject_id" && member.selectorValue?.trim()) {
    return `subject_id: ${member.selectorValue.trim()}`;
  }
  if (member.subjectId?.trim() && member.email) {
    return member.subjectId.trim();
  }
  return member.selectorKind || "";
}

export function SummaryStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-border bg-muted px-3 py-2">
      <p className="text-xs font-medium text-muted-foreground">{label}</p>
      <p className="mt-0.5 text-lg font-heading text-foreground">{value}</p>
    </div>
  );
}
