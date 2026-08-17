import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const BADGE = readFileSync(join(HERE, "access-group-badge.tsx"), "utf8");
const STATUS = readFileSync(join(HERE, "admin-access-status.tsx"), "utf8");
const ROSTER = readFileSync(join(HERE, "admin-access-roster.tsx"), "utf8");

describe("AccessGroupBadge", () => {
  test("is the group principal chrome on the admin list and access roster", () => {
    expect(BADGE).toContain('variant="secondary"');
    expect(STATUS).toContain("AccessGroupBadge");
    expect(STATUS).not.toContain('from "@/components/ui/badge"');
    expect(ROSTER).toContain("AccessGroupBadge");
    expect(ROSTER).toContain('entry.kind === "group"');
  });
});
