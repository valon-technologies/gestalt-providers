import { describe, expect, test } from "vitest";

import {
  fleetStatePresentation,
  hasRecoveredFailedRollout,
} from "@/features/registry/fleet-state";

describe("fleetStatePresentation", () => {
  test.each([
    ["healthy", "Healthy", "success"],
    ["converging", "Rolling out", "warning"],
    ["degraded", "Degraded", "destructive"],
    ["unknown", "Unknown", "muted"],
  ] as const)("maps %s to an independent fleet visual", (state, label, badgeVariant) => {
    expect(
      fleetStatePresentation({
        state,
        minimumHealthyInstances: 5,
        liveInstances: 5,
        runningDesiredVersion: 5,
        mismatched: 0,
        errors: 0,
        heartbeatTtlSeconds: 45,
        evaluatedAt: "2026-07-30T13:52:20Z",
      }),
    ).toMatchObject({ label, badgeVariant });
  });

  test("treats absent and unrecognized fleet state as unknown", () => {
    expect(fleetStatePresentation()).toMatchObject({
      label: "Unknown",
      badgeVariant: "muted",
    });
    expect(
      fleetStatePresentation({
        state: "future-state",
        minimumHealthyInstances: 0,
        liveInstances: 0,
        runningDesiredVersion: 0,
        mismatched: 0,
        errors: 0,
        heartbeatTtlSeconds: 0,
        evaluatedAt: "",
      }),
    ).toMatchObject({ label: "Unknown", badgeVariant: "muted" });
  });
});

describe("hasRecoveredFailedRollout", () => {
  const recovery = {
    recoveredAt: "2026-07-30T13:52:15Z",
    sourceVersion: "source",
    liveInstances: 5,
    minimumHealthyInstances: 5,
  };

  test("requires both an immutable failed outcome and recovery observation", () => {
    expect(
      hasRecoveredFailedRollout({
        rollout: { version: "version", state: "failed" },
        recovery,
      }),
    ).toBe(true);
    expect(
      hasRecoveredFailedRollout({
        rollout: { version: "version", state: "complete" },
        recovery,
      }),
    ).toBe(false);
    expect(
      hasRecoveredFailedRollout({
        rollout: { version: "version", state: "failed" },
      }),
    ).toBe(false);
  });
});
