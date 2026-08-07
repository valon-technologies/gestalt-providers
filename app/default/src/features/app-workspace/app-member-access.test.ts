import { describe, expect, test } from "vitest";

import type { AppAuthorizationMember } from "@/lib/api";
import {
  rolesForMembers,
  toMemberAccessPerson,
} from "./app-member-access";

function member(
  partial: Partial<AppAuthorizationMember>,
): AppAuthorizationMember {
  return partial;
}

describe("toMemberAccessPerson", () => {
  test("maps email label, locked static grant, and meta subtitle", () => {
    expect(
      toMemberAccessPerson(
        member({
          email: "alice@example.com",
          role: "admin",
          source: "static",
          mutable: false,
          effective: true,
          selectorKind: "subject_id",
          selectorValue: "user:alice",
          subjectId: "user:alice",
        }),
        0,
      ),
    ).toEqual({
      id: "user:alice",
      name: "alice@example.com",
      email: "subject_id: user:alice",
      role: "admin",
      locked: true,
    });
  });

  test("appends shadowed-by copy for ineffective dynamic grants", () => {
    const person = toMemberAccessPerson(
      member({
        email: "shadowed@example.com",
        role: "viewer",
        source: "dynamic",
        mutable: true,
        effective: false,
        shadowedBy: "static viewer grant",
        selectorKind: "subject_id",
        selectorValue: "user:shadowed",
        subjectId: "user:shadowed",
      }),
      1,
    );
    expect(person.locked).toBe(false);
    expect(person.email).toContain("Shadowed by static viewer grant");
  });

  test("service account without email uses subject id as name", () => {
    expect(
      toMemberAccessPerson(
        member({
          role: "viewer",
          subjectId: "service_account:slack-bot",
          selectorKind: "subject_id",
          selectorValue: "service_account:slack-bot",
          mutable: true,
          effective: true,
        }),
        2,
      ),
    ).toMatchObject({
      id: "service_account:slack-bot",
      name: "service_account:slack-bot",
      role: "viewer",
      locked: false,
    });
  });
});

describe("rolesForMembers", () => {
  test("keeps defaults and adds unknown roles from the roster", () => {
    const roles = rolesForMembers([
      member({ role: "admin" }),
      member({ role: "editor" }),
    ]);
    expect(roles.map((role) => role.value)).toEqual([
      "admin",
      "viewer",
      "editor",
    ]);
  });
});
