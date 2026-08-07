import type { AppAuthorizationMember } from "@/lib/api";
import type {
  MemberAccessPerson,
  MemberAccessRole,
} from "@/components/ui/member-access";
import { memberLabel, memberMeta } from "./app-workspace-shared";

const DEFAULT_ROLES: MemberAccessRole[] = [
  { value: "admin", label: "Admin" },
  { value: "viewer", label: "Viewer" },
];

/** Project an authorization member row into MemberAccess person chrome. */
export function toMemberAccessPerson(
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

/** Role options for MemberAccess — defaults plus any unknown roles on the roster. */
export function rolesForMembers(
  members: AppAuthorizationMember[],
): MemberAccessRole[] {
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
