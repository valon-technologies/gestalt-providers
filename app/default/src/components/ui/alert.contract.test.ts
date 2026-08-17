import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "alert.tsx"),
  "utf8",
);

describe("Alert", () => {
  test("imports Collapsible and cn from console aliases", () => {
    expect(SOURCE).toContain('from "@/components/ui/collapsible"');
    expect(SOURCE).toContain('from "@/lib/cn"');
    expect(SOURCE).not.toContain('from "@/lib/utils"');
    expect(SOURCE).not.toContain('"use client"');
  });

  test("exposes outline quiet chrome and collapsible disclosure", () => {
    expect(SOURCE).toContain('outline: "border border-border bg-card"');
    expect(SOURCE).toContain("collapsible: true");
    expect(SOURCE).toContain("function AlertTrigger");
    expect(SOURCE).toContain("function AlertCollapsibleContent");
    expect(SOURCE).toContain("animateSize");
  });

  test("live is independent of layout and defaults from layout plus variant", () => {
    expect(SOURCE).toContain("live: liveProp");
    expect(SOURCE).toContain(
      'liveProp ?? (resolvedLayout === "default" && variant !== "outline")',
    );
    expect(SOURCE).toContain('role={resolvedLive ? "alert" : undefined}');
    expect(SOURCE).toContain('Omit<React.ComponentProps<"div">, "role">');
    expect(SOURCE).not.toContain(
      'const live = resolvedLayout === "default" && variant !== "outline"',
    );
  });

  test("default and banner baseline first-line copy when AlertActions are present", () => {
    expect(SOURCE).toMatch(
      /default:\s*"grid[\s\S]*has-\[\[data-slot=alert-actions\]\]:items-baseline/,
    );
    expect(SOURCE).toMatch(
      /banner:\s*"grid[\s\S]*has-\[\[data-slot=alert-actions\]\]:items-baseline/,
    );
    expect(SOURCE).toContain("@container/alert");
    expect(SOURCE).toContain('data-slot="alert-content"');
    expect(SOURCE).toContain(
      'resolvedLayout === "banner" && "@container/alert"',
    );
    expect(SOURCE).not.toContain('className="@container/alert w-full"');
    expect(SOURCE).not.toContain("minmax(0,1fr)");
    expect(SOURCE).not.toContain(
      "group-data-[layout=default]/alert:translate-y-1",
    );
    expect(SOURCE).not.toContain(
      "group-data-[layout=banner]/alert:translate-y-1",
    );
    expect(SOURCE).toContain(
      "@max-[480px]/alert:has-[[data-slot=alert-actions]]",
    );
    expect(SOURCE).toContain(
      "@max-[480px]/alert:has-[[data-slot=alert-actions]]:has-[[data-slot=alert-title]]:has-[[data-slot=alert-description]]",
    );
  });

  test("banner keeps copy in a CSS-owned column without reparenting children", () => {
    expect(SOURCE).toContain("@container/alert");
    expect(SOURCE).toContain("alertLayoutVariants");
    expect(SOURCE).toContain(
      "group-data-[layout=banner]/alert:group-has-[[data-slot=alert-title]]/alert:row-start-2",
    );
    expect(SOURCE).not.toContain("function groupBannerCopy");
    expect(SOURCE).not.toContain('data-slot="alert-copy"');
  });

  test("Description is primary on default; banner mutes under Title", () => {
    expect(SOURCE).toContain(
      "min-w-0 wrap-break-word text-sm text-foreground text-pretty",
    );
    expect(SOURCE).toContain(
      "group-data-[layout=banner]/alert:group-has-[[data-slot=alert-title]]/alert:text-muted-foreground",
    );
    expect(SOURCE).not.toMatch(
      /AlertDescription[\s\S]*?"text-sm text-muted-foreground"/,
    );
  });

  test("default callout stacks icon with title then a type-scale body gap", () => {
    expect(SOURCE).toContain("function AlertIcon");
    expect(SOURCE).toContain('data-slot="alert-icon"');
    expect(SOURCE).toContain('aria-hidden="true"');
    expect(SOURCE).toContain(
      '"w-full has-[[data-slot=alert-title]]:has-[[data-slot=alert-description]]:grid-rows-[auto_auto] has-[[data-slot=alert-title]]:has-[[data-slot=alert-description]]:items-start"',
    );
    expect(SOURCE).toContain(
      "group-has-[[data-slot=alert-title]]/alert:mt-1.5",
    );
    expect(SOURCE).toContain(
      "min-w-0 wrap-break-word font-semibold tracking-tight",
    );
    expect(SOURCE).not.toContain("font-semibold leading-none tracking-tight");
    expect(SOURCE).toContain(
      "@max-[480px]/alert:has-[[data-slot=alert-actions]]:[&>[data-slot=alert-actions]]:mt-2",
    );
    expect(SOURCE).not.toContain(
      "group-data-[layout=default]/alert:group-has-[[data-slot=alert-title]]/alert:col-start-1",
    );
    expect(SOURCE).not.toContain(
      "group-data-[layout=default]/alert:group-has-[[data-slot=alert-title]]/alert:col-end-3",
    );
    expect(SOURCE).toContain("has-[>[data-slot=alert-icon]]");
    expect(SOURCE).not.toMatch(
      /default:\s*"grid[^"]*gap-y-0\.5/,
    );
  });

  test("AlertTrigger composes CollapsibleTrigger (focus-ring + open chevron stay there)", () => {
    expect(SOURCE).toContain(
      "CollapsibleTrigger owns focus-ring + Neutral idle hover/press",
    );
    expect(SOURCE).toMatch(
      /function AlertTrigger[\s\S]*?<CollapsibleTrigger[\s\S]*?<\/CollapsibleTrigger>/,
    );
  });

  test("owns native collapsible disclosure hairline on the wash ramp", () => {
    expect(SOURCE).not.toContain('data-slot="alert-collapsible-content"');
    expect(SOURCE).toContain(
      "group-data-[variant=info]/alert:border-blue-200 dark:group-data-[variant=info]/alert:border-blue-800",
    );
    expect(SOURCE).toContain(
      "group-data-[variant=success]/alert:border-green-200 dark:group-data-[variant=success]/alert:border-green-800",
    );
    expect(SOURCE).toContain(
      "group-data-[variant=warning]/alert:border-yellow-200 dark:group-data-[variant=warning]/alert:border-yellow-800",
    );
    expect(SOURCE).toContain(
      "group-data-[variant=destructive]/alert:border-red-200 dark:group-data-[variant=destructive]/alert:border-red-800",
    );
  });
});
