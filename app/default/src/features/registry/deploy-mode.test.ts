import { describe, expect, test } from "vitest";

import {
  resolveDeployMode,
  versionsSurfacePresentation,
} from "@/features/registry/deploy-mode";

describe("resolveDeployMode", () => {
  test("maps the auto-deploy flag onto an explicit mode", () => {
    expect(resolveDeployMode(true)).toBe("automatic");
    expect(resolveDeployMode(false)).toBe("manual");
  });
});

describe("versionsSurfacePresentation", () => {
  test("automatic mode owns the promise and hides manual deploy", () => {
    const presentation = versionsSurfacePresentation({
      autoDeployEnabled: true,
      rolloutActive: false,
      hasDesiredVersion: true,
    });
    expect(presentation.mode).toBe("automatic");
    expect(presentation.offerManualDeploy).toBe(false);
    expect(presentation.toggleDescription).toBeNull();
    expect(presentation.pageDescription).toContain(presentation.modePromise);
    expect(presentation.pageDescription).toContain(presentation.framing);
    expect(presentation.manualDeployBlockedReason).toMatch(/automatic deploy/);
    expect(presentation.manualDeployBlockedReason).not.toMatch(/auto-deploy/);
  });

  test("automatic + rollout keeps queue nuance on the toggle only", () => {
    const presentation = versionsSurfacePresentation({
      autoDeployEnabled: true,
      rolloutActive: true,
      hasDesiredVersion: true,
    });
    expect(presentation.modePromise).toMatch(/queue until the current rollout/);
    expect(presentation.toggleDescription).toBe(
      "New versions queue until the current rollout finishes.",
    );
  });

  test("manual mode offers deploy and explains the toggle", () => {
    const presentation = versionsSurfacePresentation({
      autoDeployEnabled: false,
      rolloutActive: false,
      hasDesiredVersion: true,
    });
    expect(presentation.mode).toBe("manual");
    expect(presentation.offerManualDeploy).toBe(true);
    expect(presentation.manualDeployBlockedReason).toBeNull();
    expect(presentation.toggleDescription).toMatch(/Turn on/);
  });

  test("empty inventory always points at Check for new versions", () => {
    const presentation = versionsSurfacePresentation({
      autoDeployEnabled: false,
      rolloutActive: false,
      hasDesiredVersion: false,
    });
    expect(presentation.emptyHint).toMatch(/Check for new versions/);
    expect(presentation.checkForNewVersionsPendingLabel).toBe("Checking…");
  });
});
