import { createElement, isValidElement, type ReactElement, type ReactNode } from "react";
import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  badgeLabelClassName,
  partitionBadgeChildren,
} from "@/components/ui/badge";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "badge.tsx"),
  "utf8",
);

describe("Badge paint contract", () => {
  test("base styles snap — no color-transition utilities (toolshed#4057 / #4081)", () => {
    const baseMatch = SOURCE.match(
      /const badgeVariants = cva\(\s*(?:\/\/[^\n]*\n\s*)*"([^"]+)"/,
    );
    expect(baseMatch?.[1]).toBeTruthy();
    expect(baseMatch![1]).not.toContain("transition-colors");
    expect(baseMatch![1]).not.toContain("transition-[");
  });

  test("label text box owns cap/alphabetic trim — not the flex chrome (toolshed#4113)", () => {
    const baseMatch = SOURCE.match(
      /const badgeVariants = cva\(\s*(?:(?:\/\/[^\n]*\n)\s*)*"([^"]+)"/,
    );
    expect(baseMatch?.[1]).toBeTruthy();
    expect(baseMatch![1]).not.toContain("[text-box:trim-both_cap_alphabetic]");
    expect(baseMatch![1]).toContain("inline-flex");
    expect(SOURCE).toMatch(
      /export const badgeLabelClassName = "\[text-box:trim-both_cap_alphabetic\]"/,
    );
    expect(SOURCE).toContain('data-slot="badge-label"');
    expect(SOURCE).toContain("partitionBadgeChildren");
  });

  test("size owns type / pad / icon ladder (toolshed#4119)", () => {
    expect(SOURCE).toContain(
      'sm: "gap-0.5 px-1 py-1 text-2xs leading-none [&>svg]:size-2.5"',
    );
    expect(SOURCE).toContain(
      'default: "gap-1 px-1.5 py-1.5 text-xs leading-none [&>svg]:size-3"',
    );
    expect(SOURCE).toContain(
      'lg: "gap-1.5 px-2 py-2 text-sm leading-none [&>svg]:size-3.5"',
    );
  });

  test("ghost shares press-feedback quiet chrome; muted climbs neutral-dark", () => {
    expect(SOURCE).toContain("ghostQuietChromeClassName");
    expect(SOURCE).toContain("hover:bg-neutral-dark-hover");
    expect(SOURCE).not.toContain("hover:bg-muted/80");
    expect(SOURCE).not.toContain("hover:bg-accent");
  });

  test("status variants stay on --badge-* (not shell --success grove)", () => {
    expect(SOURCE).toContain("bg-badge-success text-badge-success-foreground");
    expect(SOURCE).toContain(
      "bg-badge-destructive text-badge-destructive-foreground",
    );
    expect(SOURCE).not.toMatch(/success:\s*"bg-success/);
  });
});

describe("partitionBadgeChildren", () => {
  test("coalesces adjacent primitives into one trimmed label", () => {
    const out = partitionBadgeChildren([3, " selected"]);
    expect(Array.isArray(out)).toBe(true);
    const nodes = out as ReactNode[];
    expect(nodes).toHaveLength(1);
    expect(isValidElement(nodes[0])).toBe(true);
    const el = nodes[0] as ReactElement<{
      className?: string;
      "data-slot"?: string;
      children?: ReactNode;
    }>;
    expect(el.props["data-slot"]).toBe("badge-label");
    expect(el.props.className).toBe(badgeLabelClassName);
    expect(el.props.children).toBe("3 selected");
  });

  test("keeps element children as flex siblings beside a coalesced label", () => {
    const icon = createElement("span", { "data-testid": "icon" });
    const out = partitionBadgeChildren([icon, "Done"]);
    const nodes = out as ReactNode[];
    expect(nodes).toHaveLength(2);
    expect(isValidElement(nodes[0])).toBe(true);
    expect(
      (nodes[0] as ReactElement<{ "data-testid"?: string }>).props["data-testid"],
    ).toBe("icon");
    expect(isValidElement(nodes[1])).toBe(true);
    const label = nodes[1] as ReactElement<{ children?: ReactNode }>;
    expect(label.props.children).toBe("Done");
  });
});
