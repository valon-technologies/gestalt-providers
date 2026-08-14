import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "avatar-group.tsx"),
  "utf8",
);

describe("AvatarGroup vendor contract", () => {
  test("imports cn from @/lib/cn (console vendor convention)", () => {
    expect(SOURCE).toContain('from "@/lib/cn"');
    expect(SOURCE).not.toContain('from "@/lib/utils"');
  });

  test("AvatarGroupCount matches solid filled-chip token", () => {
    expect(SOURCE).toContain("bg-muted-strong");
    expect(SOURCE).not.toContain("bg-neutral-dark-hover");
  });

  test("motion variant transitions translate, scale, and overlap", () => {
    expect(SOURCE).toContain('variant = "motion"');
    expect(SOURCE).toContain("duration-overshoot");
    expect(SOURCE).toContain("ease-out-back");
    expect(SOURCE).toContain("group/avatar-group");
  });
});
