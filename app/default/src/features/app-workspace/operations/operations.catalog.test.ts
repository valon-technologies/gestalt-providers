import { describe, expect, it } from "vitest";

import type { IntegrationOperation } from "@/lib/api";
import {
  catalogEntriesFromOperations,
  filterCatalogEntries,
  getCatalogSearchHaystack,
  toCatalogEntry,
} from "./catalog";
import { resolveOperationFocus } from "./focus";
import {
  operationDeepLinkPath,
  operationInvokeCliCommand,
} from "./handoffs";

const sampleOp = (
  id: string,
  extra?: Partial<IntegrationOperation>,
): IntegrationOperation => ({
  id,
  ...extra,
});

describe("operation catalog", () => {
  it("projects displayable fields and omits title when it matches id", () => {
    const entry = toCatalogEntry(
      sampleOp("issues.list", {
        title: "issues.list",
        description: "List issues",
        method: "GET",
        path: "/issues",
        allowedRoles: ["viewer"],
        readOnly: true,
        tags: ["hidden-from-search-unless-shown"],
        transport: "http",
      }),
    );

    expect(entry.title).toBeNull();
    expect(entry.description).toBe("List issues");
    expect(entry.path).toBe("/issues");
    expect(entry.rolesLabel).toBe("viewer");
    expect(entry.readOnly).toBe(true);

    const haystack = getCatalogSearchHaystack(entry);
    expect(haystack).toContain("/issues");
    expect(haystack).not.toContain("hidden-from-search-unless-shown");
    expect(haystack).not.toContain("http");
  });

  it("keeps a distinct human title", () => {
    expect(
      toCatalogEntry(sampleOp("issues.list", { title: "List issues" })).title,
    ).toBe("List issues");
  });

  it("filters visible operations into catalog entries", () => {
    const entries = catalogEntriesFromOperations([
      sampleOp("a.list"),
      sampleOp("b.list", { visible: false }),
      sampleOp(""),
    ]);
    expect(entries.map((entry) => entry.id)).toEqual(["a.list"]);
  });

  it("filters with token-AND over displayable fields only", () => {
    const entries = catalogEntriesFromOperations([
      sampleOp("issues.list", {
        description: "List issues",
        path: "/v1/issues",
      }),
      sampleOp("customers.list", { description: "List customers" }),
    ]);
    expect(filterCatalogEntries(entries, "issues list").map((e) => e.id)).toEqual([
      "issues.list",
    ]);
    expect(filterCatalogEntries(entries, "/v1/issues").map((e) => e.id)).toEqual([
      "issues.list",
    ]);
  });
});

describe("resolveOperationFocus", () => {
  const sectionIdForOperation = (id: string) => `ops-${id}`;

  it("returns idle without a hash", () => {
    expect(
      resolveOperationFocus({
        hash: null,
        loading: false,
        visibleIds: new Set(["a"]),
        filteredIds: new Set(["a"]),
        sectionIdForOperation,
      }),
    ).toEqual({ status: "idle" });
  });

  it("returns pending while loading", () => {
    expect(
      resolveOperationFocus({
        hash: "a.list",
        loading: true,
        visibleIds: new Set(),
        filteredIds: new Set(),
        sectionIdForOperation,
      }),
    ).toEqual({ status: "pending", operationId: "a.list" });
  });

  it("returns unknown when the operation is not in the catalog", () => {
    expect(
      resolveOperationFocus({
        hash: "missing.op",
        loading: false,
        visibleIds: new Set(["a.list"]),
        filteredIds: new Set(["a.list"]),
        sectionIdForOperation,
      }),
    ).toEqual({ status: "unknown", operationId: "missing.op" });
  });

  it("returns hidden when search filters the target out", () => {
    expect(
      resolveOperationFocus({
        hash: "a.list",
        loading: false,
        visibleIds: new Set(["a.list"]),
        filteredIds: new Set(),
        sectionIdForOperation,
      }),
    ).toEqual({
      status: "hidden",
      operationId: "a.list",
      reason: "filtered",
    });
  });

  it("returns matched with section id", () => {
    expect(
      resolveOperationFocus({
        hash: "a.list",
        loading: false,
        visibleIds: new Set(["a.list"]),
        filteredIds: new Set(["a.list"]),
        sectionIdForOperation,
      }),
    ).toEqual({
      status: "matched",
      operationId: "a.list",
      sectionId: "ops-a.list",
    });
  });
});

describe("operation handoffs", () => {
  it("builds deep link paths and CLI invoke commands", () => {
    expect(operationDeepLinkPath("talent-team", "candidates.list")).toBe(
      "/apps/talent-team/operations#candidates.list",
    );
    expect(operationInvokeCliCommand("talent-team", "candidates.list")).toBe(
      "gestalt apps invoke talent-team candidates.list",
    );
  });
});
