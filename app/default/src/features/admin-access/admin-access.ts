import {
  isAPIErrorStatus,
  type AppAuthorizationMember,
  type AuthorizationRelationshipTarget,
  type AuthorizationRelationshipTuple,
  type AuthorizationResource,
  type AuthorizationResourceType,
} from "@/lib/api";

/** Who can use this app — one rule, stored as defaultRole + members. */
export type AppAccessRule = "everyone" | "specific" | "no_one";

export type AppAccessKind = "group" | "person";

export type AppAccessEntry = {
  kind: AppAccessKind;
  label: string;
  role: string;
  mutable: boolean;
  member: AppAuthorizationMember;
};

export const DEFAULT_APP_ACCESS_ROLE = "viewer";
export const DEFAULT_APP_RESOURCE_TYPE = "app";
export const DEFAULT_GROUP_RESOURCE_TYPE = "group";
export const DEFAULT_GROUP_RELATION = "member";
export const SUBJECT_TYPE = "subject";

export function authorizationResourceForApp(
  appName: string,
): AuthorizationResource {
  return { type: DEFAULT_APP_RESOURCE_TYPE, id: appName };
}

export function resourceTypeHasDefaultRole(
  resourceTypes: AuthorizationResourceType[],
  resourceTypeName = DEFAULT_APP_RESOURCE_TYPE,
): boolean {
  const match = resourceTypes.find((item) => item.name === resourceTypeName);
  return Boolean(match?.defaultRole?.trim());
}

export function isGroupMember(member: AppAuthorizationMember): boolean {
  return member.selectorKind === "subject_set";
}

export function isPersonMember(member: AppAuthorizationMember): boolean {
  if (isGroupMember(member)) return false;
  return (
    member.selectorKind === "subject_id" ||
    Boolean(member.email?.trim() || member.subjectId?.trim())
  );
}

export function groupLabel(member: AppAuthorizationMember): string {
  const parsed = parseGroupSelector(member.selectorValue ?? "");
  return parsed.id || member.selectorValue?.trim() || "Group";
}

export function personLabel(member: AppAuthorizationMember): string {
  const email = member.email?.trim();
  if (email) return email;
  const subject = (member.subjectId || member.selectorValue || "").trim();
  if (subject.startsWith("user:")) return subject.slice("user:".length);
  return subject || "Person";
}

export function parseGroupSelector(raw: string): {
  type: string;
  id: string;
  relation: string;
} {
  const trimmed = raw.trim();
  const hash = trimmed.indexOf("#");
  const relation =
    hash >= 0
      ? trimmed.slice(hash + 1).trim() || DEFAULT_GROUP_RELATION
      : DEFAULT_GROUP_RELATION;
  const resource = hash >= 0 ? trimmed.slice(0, hash).trim() : trimmed;
  const colon = resource.indexOf(":");
  if (colon >= 0) {
    return {
      type: resource.slice(0, colon).trim() || DEFAULT_GROUP_RESOURCE_TYPE,
      id: resource.slice(colon + 1).trim(),
      relation,
    };
  }
  return {
    type: DEFAULT_GROUP_RESOURCE_TYPE,
    id: resource,
    relation,
  };
}

export function personSubjectId(emailOrSubject: string): string {
  const trimmed = emailOrSubject.trim();
  if (trimmed.startsWith("user:")) return trimmed;
  return `user:${trimmed}`;
}

export function inferAppAccessRule(options: {
  hasDefaultRole: boolean;
  members: AppAuthorizationMember[];
}): AppAccessRule {
  if (options.hasDefaultRole) return "everyone";
  if (options.members.some((member) => isGroupMember(member) || isPersonMember(member))) {
    return "specific";
  }
  return "no_one";
}

/** One row per principal. Extra roles for the same group or person collapse. */
function accessEntryIdentity(entry: AppAccessEntry): string {
  if (entry.kind === "group") {
    const parsed = parseGroupSelector(entry.member.selectorValue ?? "");
    return `group:${parsed.type}:${parsed.id}`;
  }
  const id = (
    entry.member.subjectId ||
    entry.member.selectorValue ||
    entry.member.email ||
    entry.label
  ).trim();
  return `person:${id}`;
}

function collapseAccessEntries(entries: AppAccessEntry[]): AppAccessEntry[] {
  const byIdentity = new Map<string, AppAccessEntry>();
  for (const entry of entries) {
    const key = accessEntryIdentity(entry);
    const existing = byIdentity.get(key);
    if (!existing || (entry.mutable && !existing.mutable)) {
      byIdentity.set(key, entry);
    }
  }
  return [...byIdentity.values()];
}

export function partitionAccessEntries(
  members: AppAuthorizationMember[],
): { groups: AppAccessEntry[]; people: AppAccessEntry[] } {
  const groups: AppAccessEntry[] = [];
  const people: AppAccessEntry[] = [];
  for (const member of members) {
    if (isGroupMember(member)) {
      groups.push({
        kind: "group",
        label: groupLabel(member),
        role: member.role?.trim() || DEFAULT_APP_ACCESS_ROLE,
        mutable: member.mutable !== false,
        member,
      });
      continue;
    }
    if (isPersonMember(member)) {
      people.push({
        kind: "person",
        label: personLabel(member),
        role: member.role?.trim() || DEFAULT_APP_ACCESS_ROLE,
        mutable: member.mutable !== false,
        member,
      });
    }
  }
  return {
    groups: collapseAccessEntries(groups),
    people: collapseAccessEntries(people),
  };
}

export function relationshipTargetForMember(
  member: AppAuthorizationMember,
): AuthorizationRelationshipTarget | null {
  if (isGroupMember(member)) {
    const parsed = parseGroupSelector(member.selectorValue ?? "");
    if (!parsed.id) return null;
    return {
      subjectSet: {
        resource: { type: parsed.type, id: parsed.id },
        relation: parsed.relation,
      },
    };
  }
  const subjectId = (
    member.subjectId ||
    member.selectorValue ||
    (member.email ? personSubjectId(member.email) : "")
  ).trim();
  if (!subjectId) return null;
  return {
    subject: { type: SUBJECT_TYPE, id: subjectId },
  };
}

export const DEFAULT_PLATFORM_ADMIN_ROLE = "admin";

export function relationshipTupleForMemberOnResource(
  resource: AuthorizationResource,
  member: AppAuthorizationMember,
): AuthorizationRelationshipTuple | null {
  const target = relationshipTargetForMember(member);
  const relation = member.role?.trim();
  if (!target || !relation) return null;
  return {
    resource,
    relation,
    target,
  };
}

export function relationshipTupleForMember(
  appName: string,
  member: AppAuthorizationMember,
): AuthorizationRelationshipTuple | null {
  return relationshipTupleForMemberOnResource(
    authorizationResourceForApp(appName),
    member,
  );
}

export function personRelationshipTupleForResource(
  resource: AuthorizationResource,
  email: string,
  role: string,
): AuthorizationRelationshipTuple {
  return {
    resource,
    relation: role,
    target: {
      subject: { type: SUBJECT_TYPE, id: personSubjectId(email) },
    },
  };
}

export function groupRelationshipTupleForResource(
  resource: AuthorizationResource,
  groupInput: string,
  role: string,
): AuthorizationRelationshipTuple {
  const parsed = parseGroupSelector(groupInput);
  return {
    resource,
    relation: role,
    target: {
      subjectSet: {
        resource: { type: parsed.type, id: parsed.id },
        relation: parsed.relation,
      },
    },
  };
}

export function personRelationshipTuple(
  appName: string,
  email: string,
  role = DEFAULT_APP_ACCESS_ROLE,
): AuthorizationRelationshipTuple {
  return personRelationshipTupleForResource(
    authorizationResourceForApp(appName),
    email,
    role,
  );
}

export function groupRelationshipTuple(
  appName: string,
  groupInput: string,
  role = DEFAULT_APP_ACCESS_ROLE,
): AuthorizationRelationshipTuple {
  return groupRelationshipTupleForResource(
    authorizationResourceForApp(appName),
    groupInput,
    role,
  );
}

export function ruleChoiceEnabled(
  rule: AppAccessRule,
  inferred: AppAccessRule,
): boolean {
  if (inferred === "everyone") {
    return rule === "everyone";
  }
  return rule !== "everyone";
}

export function mutableAccessEntries(
  entries: AppAccessEntry[],
): AppAccessEntry[] {
  return entries.filter((entry) => entry.mutable);
}

/**
 * List-row access summary. A members 403 is `unavailable`, not "Everyone":
 * forbidden visibility is not an access rule.
 */
export type AppAccessListState =
  | { kind: "loading" }
  | { kind: "unavailable" }
  | { kind: "error" }
  | {
      kind: "ready";
      rule: AppAccessRule;
      groups: AppAccessEntry[];
      people: AppAccessEntry[];
    };

export function summarizeAppAccessList(options: {
  members: AppAuthorizationMember[] | undefined;
  membersError: unknown;
  hasDefaultRole: boolean;
}): AppAccessListState {
  if (options.membersError) {
    if (isAPIErrorStatus(options.membersError, 403)) {
      return { kind: "unavailable" };
    }
    return { kind: "error" };
  }
  if (!options.members) return { kind: "loading" };
  const { groups, people } = partitionAccessEntries(options.members);
  return {
    kind: "ready",
    rule: inferAppAccessRule({
      hasDefaultRole: options.hasDefaultRole,
      members: options.members,
    }),
    groups,
    people,
  };
}
