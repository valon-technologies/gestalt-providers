import { describe, expect, test } from "vitest";
import {
  groupFleetReplicasByRunningVersion,
  partitionFleetReplicasForSnapshotTable,
  replicaClassDotClass,
  replicaClassMarkTone,
  replicaClassLabel,
  replicaStatusIndicatorKind,
  shortInstanceId,
} from "@/features/registry/fleet-replicas";
import type { AppAdminFleetReplica } from "@/features/registry/types";

function replica(
  overrides: Partial<AppAdminFleetReplica> & Pick<AppAdminFleetReplica, "instanceId">,
): AppAdminFleetReplica {
  return {
    appState: "running",
    class: "on_desired",
    heartbeatAt: "2026-08-03T19:00:00Z",
    ...overrides,
  };
}

describe("groupFleetReplicasByRunningVersion", () => {
  test("groups and sorts by running version", () => {
    const groups = groupFleetReplicasByRunningVersion([
      replica({ instanceId: "b", runningVersion: "v2", class: "on_desired" }),
      replica({ instanceId: "a", runningVersion: "v1", class: "mismatched" }),
      replica({ instanceId: "c", runningVersion: "v2", class: "on_desired" }),
      replica({ instanceId: "d", class: "error" }),
    ]);
    expect(groups.map((g) => g.version)).toEqual(["v2", "v1", ""]);
    expect(groups[0]?.replicas.map((r) => r.instanceId)).toEqual(["b", "c"]);
  });

  test("empty input", () => {
    expect(groupFleetReplicasByRunningVersion(undefined)).toEqual([]);
    expect(groupFleetReplicasByRunningVersion([])).toEqual([]);
  });
});

describe("partitionFleetReplicasForSnapshotTable", () => {
  test("joins matching versions and keeps orphans out of invented rows", () => {
    const { byVersion, orphans } = partitionFleetReplicasForSnapshotTable(
      [
        replica({ instanceId: "a", runningVersion: "v1", class: "on_desired" }),
        replica({ instanceId: "b", runningVersion: "v2", class: "mismatched" }),
        replica({ instanceId: "c", runningVersion: "v9", class: "mismatched" }),
        replica({ instanceId: "d", class: "error" }),
      ],
      ["v1", "v2"],
    );
    expect([...byVersion.keys()].sort()).toEqual(["v1", "v2"]);
    expect(byVersion.get("v1")?.map((r) => r.instanceId)).toEqual(["a"]);
    expect(orphans.map((r) => r.instanceId)).toEqual(["c", "d"]);
  });
});

describe("shortInstanceId", () => {
  test("keeps short ids and truncates uuids", () => {
    expect(shortInstanceId("abc")).toBe("abc");
    expect(shortInstanceId("8dfcdc5b-cea7-4869-a2e8-5a51d29e8996")).toBe(
      "8dfcdc5b",
    );
  });
});

describe("replicaClassLabel", () => {
  test("maps known classes", () => {
    expect(replicaClassLabel("on_desired")).toBe("On desired version");
    expect(replicaClassLabel("mismatched")).toBe("Wrong version");
    expect(replicaClassLabel("error")).toBe("Unhealthy");
  });
});

describe("replicaClassMarkTone / dot class / indicator kind", () => {
  test("maps known classes to tones, fills, and GitHub Actions–style kinds", () => {
    expect(replicaClassMarkTone("on_desired")).toBe("success");
    expect(replicaClassMarkTone("mismatched")).toBe("warning");
    expect(replicaClassMarkTone("error")).toBe("danger");
    expect(replicaClassMarkTone("other")).toBe("muted");

    expect(replicaClassDotClass("on_desired")).toBe("bg-status-indicator-success");
    expect(replicaClassDotClass("mismatched")).toBe("bg-status-indicator-warning");
    expect(replicaClassDotClass("error")).toBe("bg-status-indicator-danger");
    expect(replicaClassDotClass("other")).toBe("bg-status-indicator-muted");

    expect(replicaStatusIndicatorKind("on_desired")).toBe("success");
    expect(replicaStatusIndicatorKind("mismatched")).toBe("pending");
    expect(replicaStatusIndicatorKind("error")).toBe("failure");
    expect(replicaStatusIndicatorKind("other")).toBe("skipped");
  });
});
