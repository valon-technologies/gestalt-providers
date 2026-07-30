import { describe, expect, it } from "vitest";

import type { IntegrationOperation } from "@/lib/api";
import {
  filterOperations,
  formatOperationResourceLabel,
  groupOperationsByResource,
  operationResourcePrefix,
} from "./operationGroups";

const sampleOp = (
  id: string,
  extra?: Partial<IntegrationOperation>,
): IntegrationOperation => ({
  id,
  ...extra,
});

describe("operationGroups", () => {
  it("groups operations by id prefix", () => {
    const groups = groupOperationsByResource([
      sampleOp("issues.list"),
      sampleOp("issues.get"),
      sampleOp("customers.list"),
    ]);
    expect(groups.map((g) => g.prefix)).toEqual(["customers", "issues"]);
    expect(groups[1]?.operations.map((op) => op.id)).toEqual([
      "issues.get",
      "issues.list",
    ]);
  });

  it("formats resource labels from camelCase prefixes", () => {
    expect(formatOperationResourceLabel("contentRevisions")).toBe(
      "Content Revisions",
    );
  });

  it("extracts resource prefix from dotted ids", () => {
    expect(operationResourcePrefix("attachments.create")).toBe("attachments");
  });

  it("filters with token-AND semantics aligned to SearchHighlight", () => {
    const ops = [
      sampleOp("issues.list", { description: "List issues for a project" }),
      sampleOp("customers.list", { description: "List customers" }),
    ];
    expect(filterOperations(ops, "issues list")).toEqual([ops[0]]);
    expect(filterOperations(ops, "list customers")).toEqual([ops[1]]);
  });
});
