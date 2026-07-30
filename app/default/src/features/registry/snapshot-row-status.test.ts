import { describe, expect, test } from "vitest";

import {
  resolveSnapshotRowStatus,
  snapshotRowStatusPresentation,
  type SnapshotRowStatusId,
} from "@/features/registry/snapshot-row-status";

describe("snapshotRowStatusPresentation", () => {
  test("maps stable ids to distinct badge variants for current vs ready", () => {
    const current = snapshotRowStatusPresentation("current");
    const ready = snapshotRowStatusPresentation("ready_to_deploy");
    expect(current.label).toBe("Current");
    expect(current.badgeVariant).toBe("info");
    expect(ready.label).toBe("Ready to deploy");
    expect(ready.badgeVariant).toBe("success");
  });

  test("sort order prioritizes in-flight and failure states", () => {
    const ids: SnapshotRowStatusId[] = [
      "current",
      "publishing",
      "deploy_failed",
      "ready_to_deploy",
    ];
    const orders = ids.map((id) => snapshotRowStatusPresentation(id).sortOrder);
    expect(orders[1]).toBeLessThan(orders[2]);
    expect(orders[2]).toBeLessThan(orders[0]);
    expect(orders[0]).toBeLessThan(orders[3]);
  });
});

describe("resolveSnapshotRowStatus", () => {
  test("marks failed rollout target as deploy_failed", () => {
    const row = {
      kind: "published" as const,
      version: "0.0.0-snapshot.gabc",
      sortAt: "2026-07-21T12:00:00Z",
      published: {
        version: "0.0.0-snapshot.gabc",
        publishedAt: "2026-07-21T12:00:00Z",
      },
    };
    expect(
      resolveSnapshotRowStatus({
        row,
        desiredVersion: "0.0.0-snapshot.gdef",
        rollout: { version: row.version, state: "failed" },
      }),
    ).toBe("deploy_failed");
  });
});
