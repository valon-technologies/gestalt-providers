import { isActiveRegistryRollout } from "@/features/registry/format";
import type { AppAdminRegistryResponse } from "@/lib/api";

/** Poll briefly after landing on admin so CI-recorded pending shows without refresh. */
export const APP_ADMIN_BOOTSTRAP_POLL_MS = 5 * 60_000;

/** Registry poll interval while bootstrap window is open or publish/rollout is active. */
export const APP_ADMIN_POLL_INTERVAL_MS = 3_000;

/** Passive refresh interval for runtime heartbeat projections. */
export const APP_ADMIN_FLEET_POLL_INTERVAL_MS = 15_000;

export function appAdminRegistryPollInterval(
  registry: AppAdminRegistryResponse,
  bootstrapPollUntilMs: number,
  now = Date.now(),
): number | false {
  if (now < bootstrapPollUntilMs) return APP_ADMIN_POLL_INTERVAL_MS;
  if (registry.selectionDisabled) return APP_ADMIN_POLL_INTERVAL_MS;
  if (registry.rollout && isActiveRegistryRollout(registry.rollout.state)) {
    return APP_ADMIN_POLL_INTERVAL_MS;
  }
  if ((registry.pendingVersions?.length ?? 0) > 0) {
    return APP_ADMIN_POLL_INTERVAL_MS;
  }
  if (registry.autoDeploy?.enabled) return APP_ADMIN_POLL_INTERVAL_MS;
  if (registry.autoDeploy?.pendingVersion) return APP_ADMIN_POLL_INTERVAL_MS;
  if (registry.fleetState) return APP_ADMIN_FLEET_POLL_INTERVAL_MS;
  return false;
}
