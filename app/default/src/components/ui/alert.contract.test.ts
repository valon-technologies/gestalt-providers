import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "alert.tsx"),
  "utf8",
);

describe("Alert (toolshed#4191)", () => {
  test("exposes outline quiet chrome and collapsible disclosure", () => {
    expect(SOURCE).toContain('outline: "border border-border bg-card"');
    expect(SOURCE).toContain("collapsible: true");
    expect(SOURCE).toContain("function AlertTrigger");
    expect(SOURCE).toContain("function AlertCollapsibleContent");
    expect(SOURCE).toContain("animateSize");
  });

  test("Description is primary until Title is present", () => {
    expect(SOURCE).toContain(
      "text-sm text-foreground group-has-[[data-slot=alert-title]]/alert:text-muted-foreground",
    );
  });

  test("imports Collapsible and cn from console aliases", () => {
    expect(SOURCE).toContain('from "@/components/ui/collapsible"');
    expect(SOURCE).toContain('from "@/lib/cn"');
    expect(SOURCE).not.toContain('from "@/lib/utils"');
  });
});
