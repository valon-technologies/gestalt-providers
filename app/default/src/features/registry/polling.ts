import { isActiveRegistryRollout } from "@/features/registry/format";
import type { AppAdminRegistryResponse } from "@/lib/api";

/** Poll briefly after landing on admin so CI-recorded pending shows without refresh. */
export const APP_ADMIN_BOOTSTRAP_POLL_MS = 5 * 60_000;

export function shouldPollAppAdminRegistry(
  registry: AppAdminRegistryResponse,
  bootstrapPollUntilMs: number,
  now = Date.now(),
): boolean {
  if (now < bootstrapPollUntilMs) return true;
  if (registry.selectionDisabled) return true;
  if (registry.rollout && isActiveRegistryRollout(registry.rollout.state)) return true;
  if ((registry.pendingVersions?.length ?? 0) > 0) return true;
  return false;
}
