import type { AppAuthorizationMember } from "@/lib/api";

export const APP_SECTION_CARD =
  "rounded-lg border border-border bg-card p-6";

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
