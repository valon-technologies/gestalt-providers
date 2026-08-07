import { describe, expect, test } from "vitest";

import type { AppAuthorizationMember } from "@/lib/api";
import { appMemberSubjectKind } from "./app-workspace-shared";

function member(
  partial: Partial<AppAuthorizationMember>,
): AppAuthorizationMember {
  return partial;
}

describe("appMemberSubjectKind", () => {
  test("classifies service_account: subject ids", () => {
    expect(
      appMemberSubjectKind(
        member({ subjectId: "service_account:slack-bot" }),
      ),
    ).toBe("service_account");
    expect(
      appMemberSubjectKind(
        member({
          subjectId: "SERVICE_ACCOUNT:Workflow-Runner",
          selectorValue: "user:ignored",
        }),
      ),
    ).toBe("service_account");
  });

  test("falls back to selectorValue when subjectId is empty", () => {
    expect(
      appMemberSubjectKind(
        member({ selectorValue: "service_account:runner" }),
      ),
    ).toBe("service_account");
  });

  test("treats user subjects and unresolved humans as person", () => {
    expect(
      appMemberSubjectKind(
        member({ subjectId: "user:7770003b-127b-44dc-b5aa-38c0310e4e23" }),
      ),
    ).toBe("person");
    expect(
      appMemberSubjectKind(member({ email: "gio@valon.com" })),
    ).toBe("person");
    expect(appMemberSubjectKind(member({}))).toBe("person");
  });
});
