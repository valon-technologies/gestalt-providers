import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const LAYOUT = readFileSync(join(HERE, "SettingsLayout.tsx"), "utf8");
const CREATE = readFileSync(join(HERE, "SettingsTokenCreate.tsx"), "utf8");

describe("Settings page measure", () => {
  test("create token uses the shared reading column; the list stays full width", () => {
    expect(LAYOUT).toContain("PAGE_LAYOUT_READING_COLUMN_CLASS");
    expect(LAYOUT).toContain("nested ? (");
    expect(LAYOUT).not.toContain("w-[calc(2/8*100%)]");
  });

  test("create token title uses the same PageHeader size as the token list", () => {
    expect(LAYOUT).toContain('<PageHeaderContent size="lg">');
    expect(CREATE).toContain('<PageHeaderContent size="lg">');
    expect(CREATE).not.toContain('<PageHeaderContent size="md">');
  });
});
