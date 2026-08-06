import { describe, expect, test } from "vitest";

import {
  formatFleetConvergenceSummary,
  presentFleetStatus,
  resolveFleetPathHint,
} from "@/features/registry/fleet-status-presentation";
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

describe("formatFleetConvergenceSummary", () => {
  test("joins on-desired, wrong, and unhealthy counts", () => {
    expect(
      formatFleetConvergenceSummary({
        ...BASE_FLEET,
        runningDesiredVersion: 3,
        mismatched: 1,
        errors: 1,
      }),
    ).toBe("3 of 5 on desired version · 1 wrong version · 1 unhealthy");
  });
});

describe("resolveFleetPathHint", () => {
  test("converging with errors prioritizes inspect over wait", () => {
    expect(
      resolveFleetPathHint({
        state: "converging",
        errors: 1,
        mismatched: 1,
      }),
    ).toBe("Inspect the unhealthy replica.");
  });

  test("converging without errors asks the operator to wait", () => {
    expect(
      resolveFleetPathHint({
        state: "converging",
        errors: 0,
        mismatched: 2,
      }),
    ).toBe("Wait for replicas to finish updating.");
  });
});

describe("presentFleetStatus", () => {
  test("healthy is a quiet strip: verdict only, no counters or hashes", () => {
    const view = presentFleetStatus({
      desiredVersion: BASE_FLEET.desiredVersion,
      fleetState: BASE_FLEET,
      rollout: { version: "0.0.0-snapshot.gabc", state: "complete" },
    });
    expect(view.density).toBe("quiet");
    expect(view.metrics).toEqual([]);
    expect(view.summaryLine).toBeNull();
    expect(view.showDesiredVersion).toBe(false);
    expect(view.showSourceVersion).toBe(false);
    expect(view.ownsActiveRolloutHeadline).toBe(true);
    expect(view.showFreshnessWindow).toBe(false);
    expect(view.recovery).toBeNull();
  });

  test("healthy still owns the headline when rollout state is stale enrolling", () => {
    const view = presentFleetStatus({
      desiredVersion: BASE_FLEET.desiredVersion,
      fleetState: BASE_FLEET,
      rollout: { version: "0.0.0-snapshot.gnew", state: "enrolling" },
    });
    expect(view.density).toBe("quiet");
    expect(view.ownsActiveRolloutHeadline).toBe(true);
  });

  test("converging with replica chips: summary owns proof, strip owns rollout headline", () => {
    const view = presentFleetStatus({
      desiredVersion: BASE_FLEET.desiredVersion,
      publishedVersions: [
        {
          version: BASE_FLEET.desiredVersion!,
          publishedAt: "2026-07-30T13:00:00Z",
          sourceUrl: "https://github.com/example/app/commit/abc",
        },
      ],
      fleetState: {
        ...BASE_FLEET,
        state: "converging",
        runningDesiredVersion: 3,
        mismatched: 2,
        replicas: [
          {
            instanceId: "a",
            heartbeatAt: "2026-08-03T19:00:00Z",
            appState: "running",
            class: "on_desired",
            runningVersion: "v1",
          },
          {
            instanceId: "b",
            heartbeatAt: "2026-08-03T19:00:00Z",
            appState: "running",
            class: "mismatched",
            runningVersion: "v0",
          },
        ],
      },
      rollout: { version: "0.0.0-snapshot.gabc", state: "rolling" },
    });
    expect(view.density).toBe("diagnostic");
    expect(view.verdict.label).toBe("Rolling out");
    expect(view.summaryLine).toBe("3 of 5 on desired version · 2 wrong version");
    expect(view.metrics).toEqual([]);
    expect(view.showReplicaChips).toBe(true);
    expect(view.showDesiredVersion).toBe(true);
    expect(view.desiredVersionHref).toBe(
      "https://github.com/example/app/commit/abc",
    );
    expect(view.showSourceVersion).toBe(false);
    expect(view.ownsActiveRolloutHeadline).toBe(true);
    expect(view.wrongVersionReplica).toMatchObject({
      shortId: "b",
      runningVersion: "v0",
      desiredVersion: BASE_FLEET.desiredVersion,
    });
    expect(view.pathHint).toBe("Wait for replicas to finish updating.");
  });

  test("converging with unhealthy replica inspects instead of waiting", () => {
    const view = presentFleetStatus({
      desiredVersion: BASE_FLEET.desiredVersion,
      fleetState: {
        ...BASE_FLEET,
        state: "converging",
        runningDesiredVersion: 3,
        mismatched: 1,
        errors: 1,
        replicas: [
          {
            instanceId: "e5d4c3b2",
            heartbeatAt: "2026-08-03T19:00:00Z",
            appState: "error",
            class: "error",
            lastError: "provider failed to start",
          },
          {
            instanceId: "d4c3b2a1",
            heartbeatAt: "2026-08-03T19:00:00Z",
            appState: "running",
            class: "mismatched",
            runningVersion: "v0",
          },
        ],
      },
      rollout: { version: "0.0.0-snapshot.gabc", state: "rolling" },
    });
    expect(view.pathHint).toBe("Inspect the unhealthy replica.");
    expect(view.failingReplica?.shortId).toBe("e5d4c3b2");
    expect(view.wrongVersionReplica?.shortId).toBe("d4c3b2a1");
  });

  test("converging without replicas falls back to metrics (no chip duplication)", () => {
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
    expect(view.showReplicaChips).toBe(false);
    expect(view.summaryLine).toBeNull();
    expect(view.metrics.map((metric) => metric.id)).toEqual([
      "onDesired",
      "wrongVersion",
    ]);
    expect(view.ownsActiveRolloutHeadline).toBe(true);
  });

  test("degraded with replicas: summary + runtime commit, owns headline only while rollout active", () => {
    const withRollout = presentFleetStatus({
      desiredVersion: BASE_FLEET.desiredVersion,
      fleetState: {
        ...BASE_FLEET,
        state: "degraded",
        runningDesiredVersion: 3,
        mismatched: 1,
        errors: 1,
        replicas: [
          {
            instanceId: "e5d4c3b2",
            heartbeatAt: "2026-08-03T19:00:00Z",
            appState: "error",
            class: "error",
            lastError: "provider failed to start",
          },
        ],
      },
      rollout: { version: "0.0.0-snapshot.gabc", state: "restarting" },
    });
    expect(withRollout.summaryLine).toContain("1 unhealthy");
    expect(withRollout.showSourceVersion).toBe(true);
    expect(withRollout.ownsActiveRolloutHeadline).toBe(true);
    expect(withRollout.failingReplica).toMatchObject({
      shortId: "e5d4c3b2",
      error: "provider failed to start",
    });
    expect(withRollout.pathHint).toBe("Inspect the unhealthy replica.");

    const withoutRollout = presentFleetStatus({
      desiredVersion: BASE_FLEET.desiredVersion,
      fleetState: {
        ...BASE_FLEET,
        state: "degraded",
        mismatched: 1,
        errors: 1,
      },
    });
    expect(withoutRollout.ownsActiveRolloutHeadline).toBe(false);
    expect(withoutRollout.metrics.map((m) => m.id)).toEqual([
      "onDesired",
      "wrongVersion",
      "unhealthy",
    ]);
  });

  test("unknown with payload keeps required-minimum + freshness window", () => {
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
    expect(view.metrics.map((metric) => metric.id)).toEqual(["minimum"]);
    expect(view.metrics[0]?.label).toBe("Required minimum");
    expect(view.showDesiredVersion).toBe(false);
    expect(view.showFreshnessWindow).toBe(true);
  });

  test("absent fleetState is unknown with no metrics", () => {
    const view = presentFleetStatus({
      desiredVersion: "0.0.0-snapshot.gabc",
    });
    expect(view.density).toBe("unknown");
    expect(view.metrics).toEqual([]);
    expect(view.verdict.label).toBe("Unknown");
    expect(view.showFreshnessWindow).toBe(false);
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
