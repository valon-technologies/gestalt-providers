import { describe, expect, test } from "vitest";
import type { AppAuthorizationMember } from "@/lib/api";
import {
  accessCountLabel,
  accessListStatus,
  ACCESS_LIST_STATUS,
  ACCESS_RULE_CHOICES,
  ACCESS_RULE_HEADING,
  ADMIN_PAGE_DESCRIPTION,
  EVERYONE_BLOCKS_REMOVE,
  LOCKED_FROM_CONFIG,
} from "./admin-access-copy";
import {
  groupLabel,
  groupRelationshipTuple,
  inferAppAccessRule,
  isGroupMember,
  partitionAccessEntries,
  personLabel,
  personRelationshipTuple,
  personSubjectId,
  parseGroupSelector,
  relationshipTupleForMember,
  resourceTypeHasDefaultRole,
  ruleChoiceEnabled,
} from "./admin-access";

function member(
  partial: Partial<AppAuthorizationMember> &
    Pick<AppAuthorizationMember, "selectorKind" | "selectorValue">,
): AppAuthorizationMember {
  return {
    role: "viewer",
    source: "dynamic",
    mutable: true,
    effective: true,
    ...partial,
  };
}

describe("who can use this app", () => {
  test("empty members with defaultRole is everyone, not off", () => {
    expect(
      inferAppAccessRule({ hasDefaultRole: true, members: [] }),
    ).toBe("everyone");
  });

  test("empty members without defaultRole is no one", () => {
    expect(
      inferAppAccessRule({ hasDefaultRole: false, members: [] }),
    ).toBe("no_one");
  });

  test("members without defaultRole is specific people", () => {
    expect(
      inferAppAccessRule({
        hasDefaultRole: false,
        members: [
          member({
            selectorKind: "subject_set",
            selectorValue: "group:eng#member",
          }),
        ],
      }),
    ).toBe("specific");
  });

  test("defaultRole wins even when members exist", () => {
    expect(
      inferAppAccessRule({
        hasDefaultRole: true,
        members: [
          member({
            selectorKind: "subject_id",
            selectorValue: "user:a@example.com",
            email: "a@example.com",
            subjectId: "user:a@example.com",
          }),
        ],
      }),
    ).toBe("everyone");
  });

  test("resource type defaultRole is per type name", () => {
    expect(
      resourceTypeHasDefaultRole([{ name: "app", defaultRole: "viewer" }]),
    ).toBe(true);
    expect(resourceTypeHasDefaultRole([{ name: "app", defaultRole: "" }])).toBe(
      false,
    );
    expect(
      resourceTypeHasDefaultRole([{ name: "other", defaultRole: "viewer" }]),
    ).toBe(false);
  });
});

describe("groups and people", () => {
  test("subject_set rows are groups; subject_id rows are people", () => {
    const group = member({
      selectorKind: "subject_set",
      selectorValue: "group:eng#member",
    });
    const person = member({
      selectorKind: "subject_id",
      selectorValue: "user:a@example.com",
      email: "a@example.com",
      subjectId: "user:a@example.com",
    });
    expect(isGroupMember(group)).toBe(true);
    expect(isGroupMember(person)).toBe(false);
    expect(groupLabel(group)).toBe("eng");
    expect(personLabel(person)).toBe("a@example.com");
    const { groups, people } = partitionAccessEntries([group, person]);
    expect(groups).toHaveLength(1);
    expect(people).toHaveLength(1);
  });

  test("parses typed group ids into subject_set tuples", () => {
    expect(parseGroupSelector("eng")).toEqual({
      type: "group",
      id: "eng",
      relation: "member",
    });
    expect(parseGroupSelector("group:eng#member")).toEqual({
      type: "group",
      id: "eng",
      relation: "member",
    });
    expect(groupRelationshipTuple("slack", "eng").target).toEqual({
      subjectSet: {
        resource: { type: "group", id: "eng" },
        relation: "member",
      },
    });
  });

  test("person email becomes user: subject id", () => {
    expect(personSubjectId("a@example.com")).toBe("user:a@example.com");
    expect(personRelationshipTuple("slack", "a@example.com").target).toEqual({
      subject: { type: "subject", id: "user:a@example.com" },
    });
  });

  test("delete tuple reuses role and selector from the member row", () => {
    const tuple = relationshipTupleForMember(
      "slack",
      member({
        role: "viewer",
        selectorKind: "subject_set",
        selectorValue: "group:eng#member",
      }),
    );
    expect(tuple).toEqual({
      resource: { type: "app", id: "slack" },
      relation: "viewer",
      target: {
        subjectSet: {
          resource: { type: "group", id: "eng" },
          relation: "member",
        },
      },
    });
  });
});

describe("rule choices", () => {
  test("everyone from config cannot switch to specific or no one", () => {
    expect(ruleChoiceEnabled("everyone", "everyone")).toBe(true);
    expect(ruleChoiceEnabled("specific", "everyone")).toBe(false);
    expect(ruleChoiceEnabled("no_one", "everyone")).toBe(false);
  });

  test("without defaultRole, everyone stays config-locked", () => {
    expect(ruleChoiceEnabled("everyone", "specific")).toBe(false);
    expect(ruleChoiceEnabled("specific", "no_one")).toBe(true);
    expect(ruleChoiceEnabled("no_one", "specific")).toBe(true);
  });
});

describe("admin copy", () => {
  test("uses who-can-use language, not assignment or authorization", () => {
    expect(ADMIN_PAGE_DESCRIPTION).toBe("Choose who can use each app.");
    expect(ACCESS_RULE_HEADING("Slack")).toBe("Who can use Slack");
    expect(ACCESS_LIST_STATUS.everyone).toBe("Everyone");
    expect(ACCESS_LIST_STATUS.noOne).toBe("No one");
    expect(ACCESS_LIST_STATUS.nobodyYet).toBe("Nobody yet");
    expect(ACCESS_RULE_CHOICES.everyone.label).toBe("Everyone in the workspace");
    expect(LOCKED_FROM_CONFIG).toMatch(/workspace config/i);
    expect(EVERYONE_BLOCKS_REMOVE).toMatch(/Specific people and groups/);
    expect(accessCountLabel(3, 2)).toBe("3 groups and 2 people");
    expect(accessCountLabel(1, 0)).toBe("1 group");
    expect(accessCountLabel(0, 1)).toBe("1 person");
    expect(accessListStatus({ rule: "everyone", groups: 0, people: 0 })).toBe(
      "Everyone",
    );
    expect(accessListStatus({ rule: "no_one", groups: 0, people: 0 })).toBe(
      "No one",
    );
    expect(accessListStatus({ rule: "specific", groups: 1, people: 0 })).toBe(
      "1 group",
    );
    expect(accessListStatus({ rule: "specific", groups: 0, people: 0 })).toBe(
      "Nobody yet",
    );
  });
});
