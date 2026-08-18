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
    expect(getActiveDocsNavItem("/docs/cli").id).toBe("cli");
  });

  it("keeps TOC subsections as real heading ids only", () => {
    const mcp = docsNavItems.find((item) => item.id === "mcp");
    const invoke = docsNavItems.find((item) => item.id === "invoke");
    // MCP destination tabs and client recipes stay hash-backed (no TOC ids).
    // Connect, env, cloud, and verify are real headings. Invoke option
    // switchers still omit matching DOM ids.
    expect(mcp?.subsections.map((s) => s.id)).toEqual([
      "mcp-connect",
      "mcp-overlap",
      "mcp-env",
      "mcp-cloud",
      "mcp-verify",
    ]);
    expect(mcp?.subsections.find((s) => s.id === "mcp-overlap")?.label).toBe(
      ASSISTANT_OVERLAP_TITLE,
    );
    expect(invoke?.subsections).toEqual([]);
    const gettingStarted = docsNavItems.find(
      (item) => item.id === "getting-started",
    );
    expect(gettingStarted?.subsections.map((s) => s.id)).toEqual([
      "connect-apps",
      "create-token",
      "next-steps",
    ]);
    const cli = docsNavItems.find((item) => item.id === "cli");
    expect(cli?.href).toBe("/docs/cli");
    expect(cli?.group).toBe("terminal");
    expect(cli?.subsections.map((s) => s.id)).toEqual([
      "cli-install",
      "cli-point",
      "cli-authenticate",
    ]);
    const connect = docsNavItems.find((item) => item.id === "connect");
    expect(connect?.group).toBe("terminal");
    expect(connect?.subsections.map((s) => s.id)).toEqual(["connect-cli"]);
    expect(
      docsNavItems.filter((item) => item.group === "setup").map((item) => item.id),
    ).toEqual(["getting-started", "tokens"]);
    expect(
      docsNavItems.filter((item) => item.group === "assistants").map((item) => item.id),
    ).toEqual(["mcp"]);
    expect(
      docsNavItems.filter((item) => item.group === "terminal").map((item) => item.id),
    ).toEqual(["cli", "connect", "invoke", "workflows"]);
    expect(
      docsNavItems.filter((item) => item.group === "administer").map((item) => item.id),
    ).toEqual(["authorization"]);
    expect(
      docsNavItems.filter((item) => item.group === "help").map((item) => item.id),
    ).toEqual(["troubleshooting"]);
    expect(DOCS_NAV_GROUPS.map((group) => group.label)).toEqual([
      "Setup",
      "Assistants",
      "Terminal",
      "Administer",
      "Help",
    ]);
    expect(docsNavItems.map((item) => item.id)).toEqual([
      "getting-started",
      "tokens",
      "mcp",
      "cli",
      "connect",
      "invoke",
      "workflows",
      "authorization",
      "troubleshooting",
    ]);
    const cliIndex = docsNavItems.findIndex((item) => item.id === "cli");
    const connectIndex = docsNavItems.findIndex((item) => item.id === "connect");
    expect(cliIndex).toBeGreaterThan(-1);
    expect(connectIndex).toBeGreaterThan(cliIndex);
    expect(docsNavItems.find((item) => item.id === "tokens")?.label).toBe(
      "API Tokens",
    );
    expect(
      docsNavItems.find((item) => item.id === "tokens")?.subsections.map((s) => s.id),
    ).toEqual(["tokens-use", "tokens-cli"]);
    expect(docsNavItems.find((item) => item.id === "mcp")?.label).toBe(
      "MCP Clients",
    );
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

  it("places the admin audience callout in Grant App Access body, not above the title", () => {
    const here = dirname(fileURLToPath(import.meta.url));
    const shell = readFileSync(join(here, "DocsShell.tsx"), "utf8");
    const content = readFileSync(join(here, "DocsContent.tsx"), "utf8");
    const adminIds = docsNavItems
      .filter((item) => item.audience === "admin")
      .map((item) => item.id);
    expect(adminIds).toEqual(["authorization"]);
    expect(shell).not.toContain("DocsAudienceCallout");
    const grantPage = content.slice(
      content.indexOf("export function AuthorizationDocsPage"),
    );
    const introEnd = grantPage.indexOf("to admin authorization commands.");
    const firstHeading = grantPage.indexOf(
      '<Subheading id="authz-plugin-access" />',
    );
    const callout = grantPage.indexOf("<DocsAudienceCallout />");
    expect(introEnd).toBeGreaterThan(-1);
    expect(callout).toBeGreaterThan(introEnd);
    expect(firstHeading).toBeGreaterThan(callout);
  });
});
