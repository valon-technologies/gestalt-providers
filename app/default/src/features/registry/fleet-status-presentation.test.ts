import { describe, expect, test } from "vitest";

import { presentFleetStatus } from "@/features/registry/fleet-status-presentation";
import type { AppAdminFleetState } from "@/features/registry/types";

const BASE_FLEET: AppAdminFleetState = {
  state: "healthy",
  sourceVersion: "4f71afddf31d2c452ecd248779a04c905a7b9988",
  desiredVersion: "0.0.0-snapshot.gabc",
  minimumHealthyInstances: 5,
  liveInstances: 5,
  runningDesiredVersion: 5,
  mismatched: 0,
  errors: 0,
  heartbeatTtlSeconds: 45,
  evaluatedAt: "2026-07-30T13:52:20Z",
};

describe("presentFleetStatus", () => {
  test("healthy is a quiet strip: live + evaluated, no zeros or hashes", () => {
    const view = presentFleetStatus({
      desiredVersion: BASE_FLEET.desiredVersion,
      fleetState: BASE_FLEET,
      rollout: { version: "0.0.0-snapshot.gabc", state: "complete" },
    });
    expect(view.density).toBe("quiet");
    expect(view.metrics.map((metric) => metric.id)).toEqual(["live"]);
    expect(view.showIdentity).toBe(false);
    expect(view.showFreshnessWindow).toBe(false);
    expect(view.showEvaluated).toBe(true);
    expect(view.recovery).toBeNull();
  });

  test("converging opens capacity and convergence facts", () => {
    const view = presentFleetStatus({
      desiredVersion: BASE_FLEET.desiredVersion,
      fleetState: {
        ...BASE_FLEET,
        state: "converging",
        runningDesiredVersion: 3,
        mismatched: 2,
      },
      rollout: { version: "0.0.0-snapshot.gabc", state: "rolling" },
    });
    expect(view.density).toBe("diagnostic");
    expect(view.metrics.map((metric) => metric.id)).toEqual([
      "live",
      "minimum",
      "onDesired",
      "wrongVersion",
    ]);
    expect(view.showIdentity).toBe(true);
    expect(view.desiredVersion).toBe(BASE_FLEET.desiredVersion);
  });

  test("degraded includes the full diagnostic counter set", () => {
    const view = presentFleetStatus({
      desiredVersion: BASE_FLEET.desiredVersion,
      fleetState: {
        ...BASE_FLEET,
        state: "degraded",
        runningDesiredVersion: 3,
        mismatched: 1,
        errors: 1,
      },
    });
    expect(view.metrics.map((metric) => metric.id)).toEqual([
      "live",
      "minimum",
      "onDesired",
      "wrongVersion",
      "unhealthy",
    ]);
    expect(view.showIdentity).toBe(true);
  });

  test("unknown with payload keeps capacity + freshness window", () => {
    const view = presentFleetStatus({
      desiredVersion: BASE_FLEET.desiredVersion,
      fleetState: {
        ...BASE_FLEET,
        state: "unknown",
        liveInstances: 4,
        runningDesiredVersion: 4,
      },
    });
    expect(view.density).toBe("unknown");
    expect(view.metrics.map((metric) => metric.id)).toEqual(["live", "minimum"]);
    expect(view.showIdentity).toBe(false);
    expect(view.showFreshnessWindow).toBe(true);
  });

  test("absent fleetState is unknown with no metrics", () => {
    const view = presentFleetStatus({
      desiredVersion: "0.0.0-snapshot.gabc",
    });
    expect(view.density).toBe("unknown");
    expect(view.metrics).toEqual([]);
    expect(view.verdict.label).toBe("Unknown");
    expect(view.showEvaluated).toBe(false);
  });

  test("recovery is orthogonal to the live verdict", () => {
    const recovery = {
      recoveredAt: "2026-07-30T13:52:15Z",
      sourceVersion: "source",
      liveInstances: 5,
      minimumHealthyInstances: 5,
    };
    const view = presentFleetStatus({
      desiredVersion: BASE_FLEET.desiredVersion,
      fleetState: BASE_FLEET,
      rollout: { version: "0.0.0-snapshot.gabc", state: "failed" },
      recovery,
    });
    expect(view.density).toBe("quiet");
    expect(view.recovery).toEqual(recovery);
  });
});
