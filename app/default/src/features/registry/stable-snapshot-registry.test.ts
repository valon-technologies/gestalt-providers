import { describe, expect, test } from "vitest";

import { snapshotRegistryPollEqual } from "@/features/registry/stable-snapshot-registry";
import type { AppAdminRegistryResponse } from "@/features/registry/types";

const BASE_REGISTRY: AppAdminRegistryResponse = {
  app: "example-app",
  registry: "example-registry",
  desiredVersion: "0.0.0-snapshot.gabc",
  knownVersions: [],
  publishedVersions: [
    {
      version: "0.0.0-snapshot.gabc",
      publishedAt: "2026-07-21T12:00:00Z",
    },
  ],
  pendingVersions: [
    {
      version: "0.0.0-snapshot.gpending",
      startedAt: "2026-07-23T14:56:00Z",
      updatedAt: "2026-07-23T14:56:00Z",
      phase: "publishing",
      publishingForSeconds: 120,
    },
  ],
  rollout: { version: "0.0.0-snapshot.gabc", state: "complete" },
  autoDeploy: { enabled: true },
  selectionDisabled: false,
};

describe("snapshotRegistryPollEqual", () => {
  test("treats pending poll counter churn as equal", () => {
    const next: AppAdminRegistryResponse = {
      ...BASE_REGISTRY,
      pendingVersions: [
        {
          ...BASE_REGISTRY.pendingVersions![0],
          updatedAt: "2026-07-23T15:00:00Z",
          publishingForSeconds: 360,
        },
      ],
    };

    expect(snapshotRegistryPollEqual(BASE_REGISTRY, next)).toBe(true);
  });

  test("detects rollout state changes", () => {
    const next: AppAdminRegistryResponse = {
      ...BASE_REGISTRY,
      rollout: { version: "0.0.0-snapshot.gabc", state: "enrolling" },
    };

    expect(snapshotRegistryPollEqual(BASE_REGISTRY, next)).toBe(false);
  });
});
