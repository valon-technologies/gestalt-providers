import { describe, expect, test } from "vitest";

import {
  buildCatalogAccessTree,
  leafValueFromSelectedApps,
  selectedAppsFromLeafValue,
  type SelectedAppState,
} from "./token-scope-selection";

const OPS = [
  { id: "list", title: "List" },
  { id: "get", title: "Get" },
] as const;

describe("token-scope-selection", () => {
  test("buildCatalogAccessTree uses empty children while ops are loading", () => {
    const tree = buildCatalogAccessTree(
      [{ name: "agent-trace", displayName: "Agent Trace" }],
      { "agent-trace": "loading" },
    );
    expect(tree).toEqual([
      { id: "agent-trace", label: "Agent Trace", children: [] },
    ]);
  });

  test("pre-load app check is a bare leaf; ops arrival expands to operation leaves", () => {
    const selected: Record<string, SelectedAppState> = {
      "agent-trace": { allOperations: true, operationIds: new Set() },
    };

    expect(
      leafValueFromSelectedApps(selected, { "agent-trace": "loading" }),
    ).toEqual(["agent-trace"]);

    expect(
      leafValueFromSelectedApps(selected, {
        "agent-trace": [...OPS],
      }),
    ).toEqual(["agent-trace:list", "agent-trace:get"]);
  });

  test("partial operation selection round-trips through leaf ids", () => {
    const opsByApp = { "agent-trace": [...OPS] };
    const previous: Record<string, SelectedAppState> = {};
    const next = selectedAppsFromLeafValue(
      ["agent-trace:list"],
      previous,
      opsByApp,
    );
    expect(next["agent-trace"]?.allOperations).toBe(false);
    expect([...next["agent-trace"]!.operationIds]).toEqual(["list"]);
    expect(leafValueFromSelectedApps(next, opsByApp)).toEqual([
      "agent-trace:list",
    ]);
  });

  test("all operation leaves collapse back to allOperations", () => {
    const opsByApp = { "agent-trace": [...OPS] };
    const next = selectedAppsFromLeafValue(
      ["agent-trace:list", "agent-trace:get"],
      {},
      opsByApp,
    );
    expect(next["agent-trace"]).toEqual({
      allOperations: true,
      operationIds: new Set(),
    });
  });
});
