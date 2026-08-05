import { describe, expect, test } from "vitest";
import {
  buildReplicaHoverPresentation,
  partitionFleetReplicasForSnapshotTable,
  fleetReplicasPollKey,
  fleetStatePollKey,
  formatReplicaAppState,
  reconcileFleetReplicas,
  reconcileFleetState,
  replicaClassDotClass,
  replicaClassMarkTone,
  replicaClassLabel,
  replicaHoverDensity,
  replicaRowSummary,
  replicaStatusIndicatorKind,
  replicaVersionAlignment,
  shortInstanceId,
  sortReplicasByTriage,
} from "@/features/registry/fleet-replicas";
import type {
  AppAdminFleetReplica,
  AppAdminFleetState,
} from "@/features/registry/types";

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

describe("fleetReplicasPollKey", () => {
  test("ignores heartbeat churn so hover chrome is not remounted every poll", () => {
    const a = [
      replica({
        instanceId: "i-1",
        heartbeatAt: "2026-08-03T19:00:00Z",
      }),
    ];
    const b = [
      replica({
        instanceId: "i-1",
        heartbeatAt: "2026-08-03T19:00:12Z",
      }),
    ];
    expect(fleetReplicasPollKey(a)).toBe(fleetReplicasPollKey(b));
  });

  test("changes when status presentation fields change", () => {
    const a = [replica({ instanceId: "i-1", class: "on_desired" })];
    const b = [replica({ instanceId: "i-1", class: "mismatched" })];
    expect(fleetReplicasPollKey(a)).not.toBe(fleetReplicasPollKey(b));
  });
});

describe("fleetStatePollKey", () => {
  function fleet(
    overrides: Partial<AppAdminFleetState> = {},
  ): AppAdminFleetState {
    return {
      state: "healthy",
      minimumHealthyInstances: 2,
      liveInstances: 2,
      runningDesiredVersion: 2,
      mismatched: 0,
      errors: 0,
      heartbeatTtlSeconds: 90,
      replicas: [],
      ...overrides,
    };
  }

  test("changes when aggregates that drive the strip summary change", () => {
    expect(fleetStatePollKey(fleet({ mismatched: 0 }))).not.toBe(
      fleetStatePollKey(fleet({ mismatched: 1 })),
    );
  });

  test("stays stable for identical presentation aggregates", () => {
    expect(fleetStatePollKey(fleet())).toBe(fleetStatePollKey(fleet()));
  });
});

describe("reconcileFleetReplicas / reconcileFleetState", () => {
  test("reuses replica object identity when only heartbeat changes", () => {
    const previous = [
      replica({
        instanceId: "i-1",
        heartbeatAt: "2026-08-03T19:00:00Z",
      }),
    ];
    const next = [
      replica({
        instanceId: "i-1",
        heartbeatAt: "2026-08-03T19:00:12Z",
      }),
    ];
    const reconciled = reconcileFleetReplicas(previous, next);
    expect(reconciled).not.toBe(previous);
    expect(reconciled?.[0]).not.toBe(next[0]);
    expect(reconciled?.[0].heartbeatAt).toBe("2026-08-03T19:00:12Z");
    expect(reconciled?.[0]).toMatchObject({
      instanceId: "i-1",
      class: "on_desired",
    });
  });

  test("returns previous array when replicas are fully unchanged", () => {
    const previous = [replica({ instanceId: "i-1" })];
    const next = [replica({ instanceId: "i-1" })];
    expect(reconcileFleetReplicas(previous, next)).toBe(previous);
  });

  test("keeps previous fleetState when aggregates and presentation match", () => {
    const previous = {
      state: "healthy",
      minimumHealthyInstances: 1,
      liveInstances: 1,
      runningDesiredVersion: 1,
      mismatched: 0,
      errors: 0,
      heartbeatTtlSeconds: 45,
      evaluatedAt: "2026-08-03T19:00:00Z",
      replicas: [replica({ instanceId: "i-1", heartbeatAt: "2026-08-03T19:00:00Z" })],
    };
    const next = {
      ...previous,
      evaluatedAt: "2026-08-03T19:00:15Z",
      replicas: [
        replica({ instanceId: "i-1", heartbeatAt: "2026-08-03T19:00:00Z" }),
      ],
    };
    expect(reconcileFleetState(previous, next)).toBe(previous);
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
    // Triage order: unhealthy before wrong version.
    expect(orphans.map((r) => r.instanceId)).toEqual(["d", "c"]);
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
  test("maps known classes to tones, fills, and steady drift marks", () => {
    expect(replicaClassMarkTone("on_desired")).toBe("success");
    expect(replicaClassMarkTone("mismatched")).toBe("warning");
    expect(replicaClassMarkTone("error")).toBe("danger");
    expect(replicaClassMarkTone("other")).toBe("muted");

    expect(replicaClassDotClass("on_desired")).toBe("bg-status-indicator-success");
    expect(replicaClassDotClass("mismatched")).toBe("bg-status-indicator-warning");
    expect(replicaClassDotClass("error")).toBe("bg-status-indicator-danger");
    expect(replicaClassDotClass("other")).toBe("bg-status-indicator-muted");

    expect(replicaStatusIndicatorKind("on_desired")).toBe("success");
    expect(replicaStatusIndicatorKind("mismatched")).toBe("warning");
    expect(replicaStatusIndicatorKind("error")).toBe("failure");
    expect(replicaStatusIndicatorKind("other")).toBe("skipped");
  });
});

describe("formatReplicaAppState", () => {
  test("maps known app states to operator language", () => {
    expect(formatReplicaAppState("running")).toBe("Running");
    expect(formatReplicaAppState("error")).toBe("Failed");
    expect(formatReplicaAppState("")).toBe("—");
    expect(formatReplicaAppState(undefined)).toBe("—");
  });
});

describe("sortReplicasByTriage", () => {
  test("surfaces unhealthy and wrong version before on-desired", () => {
    const sorted = sortReplicasByTriage([
      replica({ instanceId: "a", class: "on_desired" }),
      replica({ instanceId: "b", class: "error" }),
      replica({ instanceId: "c", class: "mismatched" }),
      replica({ instanceId: "d", class: "on_desired" }),
    ]);
    expect(sorted.map((r) => r.instanceId)).toEqual(["b", "c", "a", "d"]);
  });
});

describe("replicaVersionAlignment / hover density / presentation", () => {
  test("treats equal running and expected as aligned", () => {
    expect(
      replicaVersionAlignment(
        replica({
          instanceId: "a",
          runningVersion: "v1",
          observedDesiredVersion: "v1",
        }),
      ),
    ).toEqual({ kind: "aligned", version: "v1" });
  });

  test("treats differing versions as diverged", () => {
    expect(
      replicaVersionAlignment(
        replica({
          instanceId: "a",
          runningVersion: "v1",
          observedDesiredVersion: "v2",
        }),
      ),
    ).toEqual({ kind: "diverged", running: "v1", expected: "v2" });
  });

  test("keeps healthy running replicas lean", () => {
    expect(
      replicaHoverDensity(
        replica({
          instanceId: "a",
          runningVersion: "v1",
          observedDesiredVersion: "v1",
        }),
      ),
    ).toBe("lean");
  });

  test("uses dense disclosure for drift, errors, and non-running process", () => {
    expect(
      replicaHoverDensity(
        replica({
          instanceId: "a",
          class: "mismatched",
          runningVersion: "v1",
          observedDesiredVersion: "v2",
        }),
      ),
    ).toBe("dense");
    expect(
      replicaHoverDensity(replica({ instanceId: "b", class: "error" })),
    ).toBe("dense");
    expect(
      replicaHoverDensity(
        replica({ instanceId: "c", appState: "starting" }),
      ),
    ).toBe("dense");
  });

  test("lean presentation omits duplicate version facts", () => {
    const presentation = buildReplicaHoverPresentation(
      replica({
        instanceId: "17003d5c-63a1-4151-9ede-cf90a0f626d8",
        runningVersion: "v1",
        observedDesiredVersion: "v1",
      }),
      { now: Date.parse("2026-08-03T19:00:08Z"), heartbeatTtlSeconds: 45 },
    );
    expect(presentation.density).toBe("lean");
    expect(presentation.alignment).toEqual({ kind: "unknown" });
    expect(presentation.processLabel).toBeNull();
    expect(presentation.lastError).toBeNull();
    expect(presentation.freshness.relative).toBe("8 seconds ago");
    expect(presentation.freshness.stale).toBe(false);
  });

  test("dense mismatched presentation pairs running vs expected with hint", () => {
    const presentation = buildReplicaHoverPresentation(
      replica({
        instanceId: "a",
        class: "mismatched",
        runningVersion: "v-old",
        observedDesiredVersion: "v-new",
      }),
    );
    expect(presentation.density).toBe("dense");
    expect(presentation.alignment).toEqual({
      kind: "diverged",
      running: "v-old",
      expected: "v-new",
    });
    expect(presentation.processLabel).toBe("Running");
    expect(presentation.statusHint).toBe(
      "Process is running; deploy target does not match.",
    );
  });

  test("marks heartbeat stale past the fleet TTL", () => {
    const presentation = buildReplicaHoverPresentation(
      replica({
        instanceId: "a",
        heartbeatAt: "2026-08-03T19:00:00Z",
      }),
      { now: Date.parse("2026-08-03T19:01:00Z"), heartbeatTtlSeconds: 45 },
    );
    expect(presentation.freshness.stale).toBe(true);
  });
});

describe("replicaRowSummary", () => {
  test("summarizes three or more healthy replicas", () => {
    expect(
      replicaRowSummary([
        replica({ instanceId: "a" }),
        replica({ instanceId: "b" }),
        replica({ instanceId: "c" }),
      ]),
    ).toBe("3 on desired");
  });

  test("omits summary for mixed health or small rails", () => {
    expect(
      replicaRowSummary([
        replica({ instanceId: "a" }),
        replica({ instanceId: "b" }),
      ]),
    ).toBeNull();
    expect(
      replicaRowSummary([
        replica({ instanceId: "a" }),
        replica({ instanceId: "b" }),
        replica({ instanceId: "c", class: "mismatched" }),
      ]),
    ).toBeNull();
  });
});
