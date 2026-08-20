import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "IntegrationCard.tsx"),
  "utf8",
);

/**
 * Catalog tiles with nested Open/Add/More must use the stretched heading link
 * (cards.md + nested-interactive.md), not a title-only hit target or a
 * role="link" div that loses middle-click / cmd-click.
 */
describe("IntegrationCard navigation contract", () => {
  test("imports catalog activate route and stretch-link primitives", () => {
    expect(SOURCE).toContain("catalogCardActivateRoute");
    expect(SOURCE).toContain('from "@/lib/nested-interactive"');
    expect(SOURCE).toContain('data-row-link=""');
    expect(SOURCE).toContain("nestedInteractiveSuppress.solidNeutralHoverStretchLink");
    expect(SOURCE).toContain('"relative h-full rounded-xl bg-neutral-hover');
    expect(SOURCE).toContain('from "@/lib/row-link"');
    expect(SOURCE).toContain("integrationCardActionPolicy");
    expect(SOURCE).toContain("policy.allowConnect");
    expect(SOURCE).toContain('actions = "manage"');
    expect(SOURCE).toContain("policy.density === \"compact\"");
    expect(SOURCE).toContain("connectActionLabel = connectLabel ?? connectAppActionLabel(label)");
    expect(SOURCE).toContain("connectAppActionLabel(label)");
    expect(SOURCE).toContain("signInAgainActionAriaLabel(label)");
    expect(SOURCE).toContain(
      'useCardClickActivate && !showAddButton ? "link"',
    );
    expect(SOURCE).not.toContain('showAddButton ? "button" : "link"');
    expect(SOURCE).toContain("connectEntryPlan");
    expect(SOURCE).toContain("catalogCardDescription");
    expect(SOURCE).toContain("mt-0.5 text-xs text-pretty");
    expect(SOURCE).not.toContain("line-clamp-1");
  });

  test("stretch overlay covers the relative card surface", () => {
    expect(SOURCE).toContain('"relative h-full rounded-xl bg-neutral-hover');
    expect(SOURCE).toContain("after:absolute after:inset-0 after:z-[1]");
    expect(SOURCE).toContain("after:rounded-xl after:content-['']");
  });

  test("nested action chrome sits above the stretch overlay", () => {
    expect(SOURCE).toContain("NESTED_INTERACTIVE_OPT_OUT_ATTR");
    expect(SOURCE).toContain("policy.allowOverflow");
    expect(SOURCE).toContain("integration-card-more-");
    expect(SOURCE).toContain(
      "relative z-10 flex shrink-0 flex-col items-end gap-1.5",
    );
  });

  test("Open app is a bottom-right primary CTA, not trailing ghost chrome", () => {
    expect(SOURCE).toContain('className="relative z-10 mt-3 flex justify-end"');
    expect(SOURCE).toMatch(
      /data-testid=\{`open-app-\$\{integration\.name\}`\}[\s\S]*?variant="default"|variant="default"[\s\S]*?data-testid=\{`open-app-\$\{integration\.name\}`\}/,
    );
    expect(SOURCE).not.toMatch(
      /showOpenAppButton[\s\S]{0,200}variant="ghost"/,
    );
  });

  test("connected uses OutcomeStatusIndicator success, not a selection check", () => {
    expect(SOURCE).toContain("OutcomeStatusIndicator");
    expect(SOURCE).toContain('status="success"');
    expect(SOURCE).toContain("iconOnly");
    expect(SOURCE).toContain("CONNECTION_CONNECTED_LABEL");
    expect(SOURCE).not.toContain("SelectionCheck");
  });

  test("does not hand-roll :has() suppress or title-only navigation", () => {
    expect(SOURCE).not.toContain("hover:has-[button:hover");
    expect(SOURCE).not.toContain('className="block rounded-sm focus-ring"');
    // Whole-card activate must not silently send needs_attention to connection.
    expect(SOURCE).not.toContain(
      'installState === "needs_attention" ? "/apps/$app/connection"',
    );
  });
});
