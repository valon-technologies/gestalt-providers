import { describe, expect, it } from "vitest";
import {
  isVersionManuallyDeployable,
  publishedVersionRetentionHoverDetail,
  publishedVersionRetentionSubline,
  resolvePublishedVersionRetention,
  versionDeploymentStateLabel,
} from "./version-deployment";

describe("version-deployment", () => {
  it("labels retention states", () => {
    expect(versionDeploymentStateLabel("redeployable")).toBe("Redeployable");
    expect(versionDeploymentStateLabel("locked")).toBe("Locked");
    expect(versionDeploymentStateLabel("desired")).toBe("Desired");
  });

  it("blocks manual deploy for locked, expired, and past deployableUntil", () => {
    expect(isVersionManuallyDeployable("locked")).toBe(false);
    expect(isVersionManuallyDeployable("expired")).toBe(false);
    expect(isVersionManuallyDeployable("redeployable")).toBe(true);
    expect(isVersionManuallyDeployable(undefined)).toBe(true);
    expect(
      isVersionManuallyDeployable({
        deploymentState: "redeployable",
        deployableUntil: "2020-01-01T00:00:00.000Z",
      }),
    ).toBe(false);
  });

  it("keeps the retention subline short; detail goes on hover", () => {
    expect(
      publishedVersionRetentionSubline({
        deploymentState: "redeployable",
        deployableUntil: "2099-01-15T12:00:00.000Z",
      }),
    ).toBe("Redeployable");
    expect(
      publishedVersionRetentionHoverDetail({
        deploymentState: "redeployable",
        deployableUntil: "2099-01-15T12:00:00.000Z",
      }),
    ).toMatch(/^Redeployable until /);
  });

  it("treats a past deployableUntil as Expired for label, hover, and gating", () => {
    const now = Date.parse("2026-08-10T12:00:00.000Z");
    const resolved = resolvePublishedVersionRetention(
      {
        deploymentState: "redeployable",
        deployableUntil: "2026-08-01T12:00:00.000Z",
      },
      now,
    );
    expect(resolved.effectiveState).toBe("expired");
    expect(resolved.manuallyDeployable).toBe(false);
    expect(resolved.subline).toBe("Expired");
    expect(resolved.hoverDetail).toMatch(/^Expired /);
    expect(resolved.actionUnavailableLabel).toBe("Expired");
  });
});
