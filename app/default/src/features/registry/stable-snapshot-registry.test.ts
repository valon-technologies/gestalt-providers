import { describe, expect, test } from "vitest";

import {
  reconcileSnapshotRegistryPoll,
  snapshotRegistryPollEqual,
} from "@/features/registry/stable-snapshot-registry";
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

describe("reconcileSnapshotRegistryPoll", () => {
  test("applies fleet updates when the table slice is poll-equal", () => {
    const previous: AppAdminRegistryResponse = {
      ...BASE_REGISTRY,
      fleetState: {
        state: "healthy",
        sourceVersion: "abc123",
        desiredVersion: BASE_REGISTRY.desiredVersion!,
        minimumHealthyInstances: 5,
        liveInstances: 5,
        runningDesiredVersion: 5,
        mismatched: 0,
        errors: 0,
        heartbeatTtlSeconds: 45,
        evaluatedAt: "2026-07-23T14:59:50Z",
      },
    };
    const next: AppAdminRegistryResponse = {
      ...previous,
      pendingVersions: [
        {
          ...BASE_REGISTRY.pendingVersions![0],
          updatedAt: "2026-07-23T15:00:00Z",
          publishingForSeconds: 360,
        },
      ],
      fleetState: {
        ...previous.fleetState!,
        liveInstances: 4,
        runningDesiredVersion: 4,
      },
      knownVersions: [
        {
          version: BASE_REGISTRY.desiredVersion!,
          installedAt: "2026-07-21T13:00:00Z",
        },
      ],
    };

    const reconciled = reconcileSnapshotRegistryPoll(previous, next);

    expect(snapshotRegistryPollEqual(previous, reconciled)).toBe(true);
    expect(reconciled.fleetState?.liveInstances).toBe(4);
    expect(reconciled.knownVersions).toEqual(next.knownVersions);
    expect(reconciled.publishedVersions).toBe(previous.publishedVersions);
    expect(reconciled.pendingVersions).toBe(previous.pendingVersions);
  });
});
