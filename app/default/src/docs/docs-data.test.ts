import { describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  DOCS_AUTHORIZATION_PATH,
  DOCS_NAV_GROUPS,
  DOCS_SETTINGS_TOKENS_HREF,
  docsNavItems,
  getActiveDocsNavItem,
  getDocsJourneyEdges,
  docsSubsectionLabel,
} from "./docs-data";
import { ASSISTANT_OVERLAP_TITLE } from "@/lib/assistantConnectionCopy";

describe("docs IA invariants", () => {
  it("keeps unique hrefs and ids", () => {
    const hrefs = docsNavItems.map((item) => item.href);
    const ids = docsNavItems.map((item) => item.id);
    const subsectionIds = docsNavItems.flatMap((item) =>
      item.subsections.map((subsection) => subsection.id),
    );
    expect(new Set(hrefs).size).toBe(hrefs.length);
    expect(new Set(ids).size).toBe(ids.length);
    expect(new Set(subsectionIds).size).toBe(subsectionIds.length);
  });

  it("assigns every item to a declared group", () => {
    const groupIds = new Set(DOCS_NAV_GROUPS.map((group) => group.id));
    for (const item of docsNavItems) {
      expect(groupIds.has(item.group)).toBe(true);
    }
  });

  it("never labels Settings tokens as Authorization", () => {
    expect(DOCS_SETTINGS_TOKENS_HREF).toBe("/settings/tokens");
    expect(DOCS_AUTHORIZATION_PATH).toBe("/docs/authorization");
    expect(DOCS_SETTINGS_TOKENS_HREF).not.toBe(DOCS_AUTHORIZATION_PATH);

    const grant = docsNavItems.find((item) => item.id === "authorization");
    expect(grant?.label).toBe("Grant App Access");
    expect(grant?.audience).toBe("admin");
    expect(grant?.href).toBe(DOCS_AUTHORIZATION_PATH);
  });

  it("derives journey edges from sidebar order", () => {
    // Footer Next/Previous follow the left-rail sequence so readers are not
    // skipped past a page that already appears between two destinations.
    for (let i = 0; i < docsNavItems.length; i++) {
      const item = docsNavItems[i]!;
      const edges = getDocsJourneyEdges(item);
      const predecessor = i > 0 ? docsNavItems[i - 1] : undefined;
      const following =
        i < docsNavItems.length - 1 ? docsNavItems[i + 1] : undefined;
      expect(edges.previous).toEqual(
        predecessor
          ? { href: predecessor.href, label: predecessor.label }
          : null,
      );
      expect(edges.next).toEqual(
        following ? { href: following.href, label: following.label } : null,
      );
    }
    expect(docsNavItems.every((item) => !("next" in item))).toBe(true);
  });

  it("resolves /docs and /docs/getting-started to Getting Started", () => {
    expect(getActiveDocsNavItem("/docs").id).toBe("getting-started");
    expect(getActiveDocsNavItem("/docs/getting-started").id).toBe(
      "getting-started",
    );
    expect(getActiveDocsNavItem("/docs/mcp").id).toBe("mcp");
  });

  it("keeps TOC subsections as real heading ids only", () => {
    const mcp = docsNavItems.find((item) => item.id === "mcp");
    const invoke = docsNavItems.find((item) => item.id === "invoke");
    // Hash-backed SegmentedControl options must not appear as TOC targets —
    // DocsOptionSwitcher intentionally omits matching DOM ids.
    expect(mcp?.subsections.map((s) => s.id)).toEqual(["mcp-overlap"]);
    expect(mcp?.subsections[0]?.label).toBe(ASSISTANT_OVERLAP_TITLE);
    expect(mcp?.subsections.some((s) => s.id.startsWith("mcp-claude"))).toBe(
      false,
    );
    expect(invoke?.subsections).toEqual([]);
    const gettingStarted = docsNavItems.find(
      (item) => item.id === "getting-started",
    );
    expect(gettingStarted?.subsections.map((s) => s.id)).toContain("install");
  });

  it("wires every TOC subsection id to a DocsContent heading owned by docs-data", () => {
    const content = readFileSync(
      join(dirname(fileURLToPath(import.meta.url)), "DocsContent.tsx"),
      "utf8",
    );
    expect(content).toContain("docsSubsectionLabel");
    expect(content).not.toMatch(/<Subheading[^>]*title=/);
    for (const item of docsNavItems) {
      for (const subsection of item.subsections) {
        expect(content).toContain(`<Subheading id="${subsection.id}" />`);
        expect(docsSubsectionLabel(subsection.id)).toBe(subsection.label);
      }
    }
  });

  it("renders the admin audience callout from DocsShell for admin pages", () => {
    const shell = readFileSync(
      join(dirname(fileURLToPath(import.meta.url)), "DocsShell.tsx"),
      "utf8",
    );
    expect(shell).toContain('audience === "admin"');
    expect(shell).toContain("DocsAudienceCallout");
    expect(docsNavItems.some((item) => item.audience === "admin")).toBe(true);
  });
});
