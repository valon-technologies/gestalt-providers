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
} from "./docs-data";

describe("docs IA invariants", () => {
  it("keeps unique hrefs and ids", () => {
    const hrefs = docsNavItems.map((item) => item.href);
    const ids = docsNavItems.map((item) => item.id);
    expect(new Set(hrefs).size).toBe(hrefs.length);
    expect(new Set(ids).size).toBe(ids.length);
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

  it("keeps journey next links inside the docs set", () => {
    const hrefs = new Set(docsNavItems.map((item) => item.href));
    for (const item of docsNavItems) {
      if (item.next) {
        expect(hrefs.has(item.next.href)).toBe(true);
      }
      for (const prereq of item.prerequisites ?? []) {
        expect(hrefs.has(prereq.href)).toBe(true);
      }
    }
  });

  it("keeps pager next edges aligned with sidebar order", () => {
    // Footer Next should follow the left-rail sequence so readers are not
    // skipped past a page that already appears between two destinations.
    for (let i = 0; i < docsNavItems.length - 1; i++) {
      const item = docsNavItems[i]!;
      const following = docsNavItems[i + 1]!;
      expect(item.next).toEqual({
        href: following.href,
        label: following.label,
      });
    }
    expect(docsNavItems.at(-1)?.next).toBeUndefined();
  });

  it("derives journey Previous as the inverse of next (not prerequisites)", () => {
    for (let i = 1; i < docsNavItems.length; i++) {
      const item = docsNavItems[i]!;
      const predecessor = docsNavItems[i - 1]!;
      expect(getDocsJourneyEdges(item).previous).toEqual({
        href: predecessor.href,
        label: predecessor.label,
      });
    }
    expect(getDocsJourneyEdges(docsNavItems[0]!).previous).toBeNull();
    // Soft prerequisites may still point at Getting Started while Previous
    // follows the linear chain (Tokens ← Invoke, not ← Getting Started).
    const tokens = docsNavItems.find((item) => item.id === "tokens")!;
    expect(tokens.prerequisites?.[0]?.href).toBe(
      docsNavItems.find((item) => item.id === "getting-started")!.href,
    );
    expect(getDocsJourneyEdges(tokens).previous?.href).toBe(
      docsNavItems.find((item) => item.id === "invoke")!.href,
    );
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
    expect(mcp?.subsections).toEqual([]);
    expect(invoke?.subsections).toEqual([]);
    const gettingStarted = docsNavItems.find(
      (item) => item.id === "getting-started",
    );
    expect(gettingStarted?.subsections.map((s) => s.id)).toContain("install");
  });

  it("wires every TOC subsection id to a DocsContent heading", () => {
    const content = readFileSync(
      join(dirname(fileURLToPath(import.meta.url)), "DocsContent.tsx"),
      "utf8",
    );
    for (const item of docsNavItems) {
      for (const subsection of item.subsections) {
        expect(content).toContain(`id="${subsection.id}"`);
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
