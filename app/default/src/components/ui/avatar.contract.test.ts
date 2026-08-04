import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "avatar.tsx"),
  "utf8",
);

describe("Avatar surface / size contract", () => {
  test("solid uses muted-strong, not muted or a hover role", () => {
    expect(SOURCE).toContain('solid: "bg-muted-strong"');
    expect(SOURCE).not.toMatch(/solid:\s*"bg-muted"/);
    expect(SOURCE).not.toContain("bg-neutral-dark-hover");
  });

  test("size band includes xl for account chrome with leading matched to the box", () => {
    expect(SOURCE).toContain('sm: "size-6 text-[0.625rem] leading-6"');
    expect(SOURCE).toContain('default: "size-7 text-xs leading-7"');
    expect(SOURCE).toContain('lg: "size-8 text-sm leading-8"');
    expect(SOURCE).toContain('xl: "size-10 text-sm leading-10"');
  });

  test("fallback centers via block + inherited leading, not flex items-center", () => {
    expect(SOURCE).toContain(
      "block size-full text-center font-medium text-muted-foreground",
    );
    expect(SOURCE).not.toMatch(
      /avatar-fallback[\s\S]*flex size-full items-center justify-center/,
    );
  });

  test("root keeps overflow visible so baseline participates", () => {
    expect(SOURCE).toContain(
      '"relative inline-flex shrink-0 select-none rounded-full"',
    );
    expect(SOURCE).toMatch(/deliberately has NO overflow-hidden/i);
    expect(SOURCE).not.toMatch(
      /"relative[^"]*overflow-hidden[^"]*rounded-full"/,
    );
  });
});
