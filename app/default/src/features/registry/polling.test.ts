import { describe, expect, test } from "vitest";

import {
  APP_ADMIN_FLEET_POLL_INTERVAL_MS,
  APP_ADMIN_POLL_INTERVAL_MS,
  appAdminRegistryPollInterval,
} from "@/features/registry/polling";
import type { AppAdminRegistryResponse } from "@/lib/api";

function registry(
  overrides: Partial<AppAdminRegistryResponse> = {},
): AppAdminRegistryResponse {
  return {
    app: "example",
    registry: "example",
    knownVersions: [],
    publishedVersions: [],
    autoDeploy: { enabled: false },
    selectionDisabled: false,
    ...overrides,
  };
}

describe("appAdminRegistryPollInterval", () => {
  test("refreshes heartbeat projections at the passive fleet cadence", () => {
    expect(
      appAdminRegistryPollInterval(
        registry({
          fleetState: {
            state: "healthy",
            minimumHealthyInstances: 5,
            liveInstances: 5,
            runningDesiredVersion: 5,
            mismatched: 0,
            errors: 0,
            heartbeatTtlSeconds: 45,
            evaluatedAt: "2026-07-30T13:52:20Z",
          },
        }),
        0,
      ),
    ).toBe(APP_ADMIN_FLEET_POLL_INTERVAL_MS);
  });

  test("keeps active rollout polling faster than heartbeat refresh", () => {
    expect(
      appAdminRegistryPollInterval(
        registry({
          rollout: { version: "version", state: "restarting" },
          fleetState: {
            state: "converging",
            minimumHealthyInstances: 5,
            liveInstances: 4,
            runningDesiredVersion: 3,
            mismatched: 1,
            errors: 0,
            heartbeatTtlSeconds: 45,
            evaluatedAt: "2026-07-30T13:52:20Z",
          },
        }),
        0,
      ),
    ).toBe(APP_ADMIN_POLL_INTERVAL_MS);
  });

  test("stops after bootstrap for mixed-version responses without fleet state", () => {
    expect(appAdminRegistryPollInterval(registry(), 0)).toBe(false);
  });
});
