import { describe, expect, it } from "vitest";
import {
  isVersionManuallyDeployable,
  publishedVersionRetentionHoverDetail,
  publishedVersionRetentionSubline,
  versionDeploymentStateLabel,
} from "./version-deployment";

describe("version-deployment", () => {
  it("labels retention states", () => {
    expect(versionDeploymentStateLabel("redeployable")).toBe("Redeployable");
    expect(versionDeploymentStateLabel("locked")).toBe("Locked");
    expect(versionDeploymentStateLabel("desired")).toBe("Desired");
  });

  it("blocks manual deploy for locked and expired", () => {
    expect(isVersionManuallyDeployable("locked")).toBe(false);
    expect(isVersionManuallyDeployable("expired")).toBe(false);
    expect(isVersionManuallyDeployable("redeployable")).toBe(true);
    expect(isVersionManuallyDeployable(undefined)).toBe(true);
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
});
