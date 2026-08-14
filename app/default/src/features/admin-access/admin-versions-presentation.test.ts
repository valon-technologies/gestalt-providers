import { describe, expect, test } from "vitest";
import type {
  AdminFleetReplica,
  AdminFleetState,
  AdminRegistryAppSummary,
} from "@/lib/api";
import {
  adminCohortLabel,
  adminFleetCapacityLabel,
  adminFleetDiagnostic,
  adminFleetIndicatorVariant,
  adminFleetRolloutNote,
  adminFreshReplicasAsAppAdmin,
  adminHeartbeatAgeLabel,
  adminReplicaSourceLabel,
  adminVersionsAppSearchText,
  adminVersionsRowMetaLine,
  filterAdminVersionsApps,
} from "./admin-versions-presentation";

const healthy: AdminFleetState = {
  state: "healthy",
  sourceVersion: "abc",
  desiredVersion: "1.0.0",
  minimumHealthyInstances: 2,
  liveInstances: 2,
  runningDesiredVersion: 2,
  mismatched: 0,
  errors: 0,
  heartbeatTtlSeconds: 45,
  evaluatedAt: "2026-08-13T00:00:00Z",
};

describe("admin versions presentation", () => {
  test("capacity and diagnostic copy", () => {
    expect(adminFleetCapacityLabel(healthy)).toBe("2/2 live");
    expect(adminFleetDiagnostic(healthy)).toBe(
      "2/2 running desired version",
    );
    expect(
      adminFleetCapacityLabel({ ...healthy, minimumHealthyInstances: 0 }),
    ).toBe("2 live");
  });

  test("heartbeat, source, cohort, and rollout note", () => {
    expect(adminHeartbeatAgeLabel(12, true)).toBe("12s ago");
    expect(adminHeartbeatAgeLabel(90, false)).toBe("Stale, 90s ago");
    expect(adminReplicaSourceLabel("current")).toBe("Current source");
    expect(adminReplicaSourceLabel("superseded")).toBe("Superseded source");
    expect(adminCohortLabel(null)).toBe("No data");
    expect(adminCohortLabel({ restarted: 1, acknowledged: 3 })).toBe(
      "1 of 3 reloaded",
    );
    expect(adminFleetRolloutNote(healthy, "failed")).toBe(
      "Current fleet is healthy; the last rollout remains failed.",
    );
  });

  test("maps fleet state onto Registry table status indicators", () => {
    expect(adminFleetIndicatorVariant(healthy)).toBe("success");
    expect(adminFleetIndicatorVariant({ ...healthy, state: "converging" })).toBe(
      "warning",
    );
    expect(adminFleetIndicatorVariant({ ...healthy, state: "degraded" })).toBe(
      "danger",
    );
    expect(adminFleetIndicatorVariant({ ...healthy, state: "unknown" })).toBe(
      "default",
    );
  });

  test("maps fresh current-source replicas onto Registry fleet chips", () => {
    const onDesired: AdminFleetReplica = {
      instanceId: "aaaaaaaa-1111-2222-3333-444444444444",
      sourceVersion: "abc",
      currentSource: true,
      sourceStatus: "current",
      fresh: true,
      heartbeatAt: "2026-08-13T00:00:00Z",
      heartbeatAgeSeconds: 2,
      appObservation: {
        state: "running",
        desiredVersion: "1.0.0",
        runningVersion: "1.0.0",
      },
    };
    const mismatched: AdminFleetReplica = {
      ...onDesired,
      instanceId: "bbbbbbbb-1111-2222-3333-444444444444",
      appObservation: {
        state: "running",
        desiredVersion: "1.0.0",
        runningVersion: "0.9.0",
      },
    };
    const errored: AdminFleetReplica = {
      ...onDesired,
      instanceId: "cccccccc-1111-2222-3333-444444444444",
      appObservation: {
        state: "error",
        desiredVersion: "1.0.0",
        runningVersion: "1.0.0",
        lastError: "crash",
      },
    };
    const stale: AdminFleetReplica = {
      ...onDesired,
      instanceId: "dddddddd-1111-2222-3333-444444444444",
      fresh: false,
    };
    const superseded: AdminFleetReplica = {
      ...onDesired,
      instanceId: "eeeeeeee-1111-2222-3333-444444444444",
      currentSource: false,
      sourceStatus: "superseded",
    };

    const mapped = adminFreshReplicasAsAppAdmin(
      [onDesired, mismatched, errored, stale, superseded],
      "1.0.0",
    );
    expect(mapped.map((replica) => replica.class)).toEqual([
      "on_desired",
      "mismatched",
      "error",
    ]);
    expect(mapped[2]?.lastError).toBe("crash");
  });

  test("filters apps by visible name, registry, version, and rollout copy", () => {
    const viewer: AdminRegistryAppSummary = {
      app: "example-viewer",
      registry: "example-registry",
      desiredVersion: "1.2.0",
      fleetState: healthy,
      cohort: { restarted: 6, acknowledged: 6, materialized: 6, failed: 0 },
    };
    const tracker: AdminRegistryAppSummary = {
      app: "example-tracker",
      registry: "other",
      desiredVersion: "0.9.0",
      rollout: {
        version: "0.9.0",
        state: "failed",
        createdAt: "2026-08-13T00:00:00Z",
        enrollmentEndsAt: "2026-08-13T00:05:00Z",
        deadline: "2026-08-13T00:10:00Z",
      },
      fleetState: healthy,
    };

    expect(filterAdminVersionsApps([viewer, tracker], "viewer")).toEqual([viewer]);
    expect(filterAdminVersionsApps([viewer, tracker], "example-registry")).toEqual([
      viewer,
    ]);
    expect(filterAdminVersionsApps([viewer, tracker], "failed")).toEqual([
      tracker,
    ]);
    expect(adminVersionsAppSearchText(viewer)).toContain("1.2.0");
    expect(adminVersionsAppSearchText(viewer)).toContain(
      "last rollout 6 of 6 reloaded",
    );
  });

  test("combines live on-target count with rollout cohort", () => {
    const live = (
      id: string,
      runningVersion = "1.0.0",
    ): AdminFleetReplica => ({
      instanceId: id,
      sourceVersion: "abc",
      currentSource: true,
      sourceStatus: "current",
      fresh: true,
      heartbeatAt: "2026-08-13T00:00:00Z",
      heartbeatAgeSeconds: 2,
      appObservation: {
        state: "running",
        desiredVersion: "1.0.0",
        runningVersion,
      },
    });
    const five = adminFreshReplicasAsAppAdmin(
      ["a", "b", "c", "d", "e"].map((id) => live(id)),
      "1.0.0",
    );
    const mixed = adminFreshReplicasAsAppAdmin(
      [live("a"), live("b"), live("c"), live("d", "0.9.0"), live("e")],
      "1.0.0",
    );
    expect(
      adminVersionsRowMetaLine(five, { restarted: 6, acknowledged: 6 }),
    ).toBe("5 live on target · last rollout 6 of 6 reloaded");
    expect(
      adminVersionsRowMetaLine(
        five,
        { restarted: 6, acknowledged: 6 },
        "enrolling",
      ),
    ).toBe("5 live on target · rollout 6 of 6 reloaded");
    expect(
      adminVersionsRowMetaLine(
        mixed,
        { restarted: 3, acknowledged: 6 },
        "restarting",
      ),
    ).toBe("4 of 5 live on target · rollout 3 of 6 reloaded");
    expect(
      adminVersionsRowMetaLine(five, { restarted: 4, acknowledged: 6 }),
    ).toBe("5 live on target · rollout 4 of 6 reloaded");
    expect(
      adminVersionsRowMetaLine(five.slice(0, 2), {
        restarted: 6,
        acknowledged: 6,
      }),
    ).toBe("2 live on target · last rollout 6 of 6 reloaded");
    expect(adminVersionsRowMetaLine(five, null)).toBe("5 live on target");
    expect(adminVersionsRowMetaLine([], { restarted: 6, acknowledged: 6 })).toBe(
      "last rollout 6 of 6 reloaded",
    );
    expect(adminVersionsRowMetaLine([], null)).toBeNull();
  });
});
