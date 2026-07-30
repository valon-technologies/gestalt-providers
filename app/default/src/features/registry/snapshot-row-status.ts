import { isRolloutDeployingAction } from "@/features/registry/rollout-stepper";
import { isActiveRegistryRollout } from "@/features/registry/format";
import type {
  AppAdminSnapshotRow,
  RegistryAppSummary,
} from "@/features/registry/types";
import type { TableStatusIndicatorVariant } from "@/components/ui/table-status-indicator";

export type SnapshotRowStatusId =
  | "publishing"
  | "publish_failed"
  | "queued_to_deploy"
  | "deploy_failed"
  | "current"
  | "rolling_out"
  | "ready_to_deploy";

type SnapshotRowPresentation = {
  label: string;
  badgeVariant: "success" | "warning" | "info" | "destructive";
  indicatorVariant: TableStatusIndicatorVariant;
  sortOrder: number;
};

const SNAPSHOT_ROW_STATUS: Record<SnapshotRowStatusId, SnapshotRowPresentation> = {
  publishing: {
    label: "Publishing",
    badgeVariant: "warning",
    indicatorVariant: "warning",
    sortOrder: 0,
  },
  rolling_out: {
    label: "Rolling out",
    badgeVariant: "warning",
    indicatorVariant: "warning",
    sortOrder: 1,
  },
  queued_to_deploy: {
    label: "Queued to deploy",
    badgeVariant: "warning",
    indicatorVariant: "warning",
    sortOrder: 2,
  },
  deploy_failed: {
    label: "Deploy failed",
    badgeVariant: "destructive",
    indicatorVariant: "danger",
    sortOrder: 3,
  },
  publish_failed: {
    label: "Publish failed",
    badgeVariant: "destructive",
    indicatorVariant: "danger",
    sortOrder: 4,
  },
  current: {
    label: "Current",
    badgeVariant: "info",
    indicatorVariant: "success",
    sortOrder: 5,
  },
  ready_to_deploy: {
    label: "Ready to deploy",
    badgeVariant: "success",
    indicatorVariant: "success",
    sortOrder: 6,
  },
};

export function resolveSnapshotRowStatus({
  row,
  desiredVersion,
  rollout,
  autoDeployPendingVersion,
}: {
  row: AppAdminSnapshotRow;
  desiredVersion?: string;
  rollout?: RegistryAppSummary["rollout"];
  autoDeployPendingVersion?: string;
}): SnapshotRowStatusId {
  if (row.kind === "pending") {
    return "publishing";
  }
  if (row.kind === "failed") {
    return "publish_failed";
  }
  if (
    autoDeployPendingVersion === row.version &&
    !isRolloutDeployingAction(rollout, row.version)
  ) {
    return "queued_to_deploy";
  }
  if (rollout && rollout.version === row.version && rollout.state === "failed") {
    return "deploy_failed";
  }
  const rolloutTargetActive =
    rollout &&
    rollout.version === row.version &&
    isActiveRegistryRollout(rollout.state);
  if (row.version === desiredVersion && !rolloutTargetActive) {
    return "current";
  }
  if (
    rollout &&
    rollout.version === row.version &&
    isActiveRegistryRollout(rollout.state)
  ) {
    return "rolling_out";
  }
  return "ready_to_deploy";
}

export function snapshotRowStatusPresentation(
  statusId: SnapshotRowStatusId,
): SnapshotRowPresentation {
  return SNAPSHOT_ROW_STATUS[statusId];
}
