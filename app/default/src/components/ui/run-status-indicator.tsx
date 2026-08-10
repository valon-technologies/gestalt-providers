/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 * Thin workflow vocabulary adapter over `OutcomeStatusIndicator` (toolshed#4181).
 */

import {
  OutcomeStatusIndicator,
  outcomeStatusIndicatorBadgeVariant,
  outcomeStatusIndicatorVariants,
  type OutcomeStatus,
  type OutcomeStatusIndicatorProps,
  type OutcomeStatusIndicatorSize,
} from "@/components/ui/outcome-status-indicator";

/**
 * Workflow / job / step vocabulary adapter over `OutcomeStatusIndicator`.
 * Prefer `OutcomeStatusIndicator` for non-run domains (connection, deploy, …).
 *
 * Owns run identity on the root: `data-slot="run-status-indicator"` and
 * `data-status` with run vocabulary (`succeeded` / `failed` / `running`, …),
 * overriding the outcome primitive’s domain-neutral `data-status`.
 */
export type RunStatusIndicatorStatus =
  | "succeeded"
  | "failed"
  | "canceled"
  | "skipped"
  | "running"
  | "pending"
  | "unknown";

export type RunStatusIndicatorSize = OutcomeStatusIndicatorSize;

const RUN_TO_OUTCOME: Record<RunStatusIndicatorStatus, OutcomeStatus> = {
  succeeded: "success",
  failed: "failure",
  canceled: "canceled",
  skipped: "skipped",
  running: "in_progress",
  pending: "pending",
  unknown: "unknown",
};

const RUN_DEFAULT_LABEL: Record<RunStatusIndicatorStatus, string> = {
  succeeded: "Succeeded",
  failed: "Failed",
  canceled: "Canceled",
  skipped: "Skipped",
  running: "Running",
  pending: "Pending",
  unknown: "Unknown",
};

export function runStatusToOutcome(
  status: RunStatusIndicatorStatus,
): OutcomeStatus {
  return RUN_TO_OUTCOME[status];
}

/** Prefer `outcomeStatusIndicatorVariants` + `runStatusToOutcome` for new code. */
function runStatusIndicatorVariants({
  status,
  size,
  className,
}: {
  status?: RunStatusIndicatorStatus | null;
  size?: RunStatusIndicatorSize | null;
  className?: string;
} = {}) {
  return outcomeStatusIndicatorVariants({
    status: status ? runStatusToOutcome(status) : undefined,
    size: size ?? undefined,
    className,
  });
}

export function runStatusIndicatorBadgeVariant(
  status: RunStatusIndicatorStatus,
): ReturnType<typeof outcomeStatusIndicatorBadgeVariant> {
  return outcomeStatusIndicatorBadgeVariant(runStatusToOutcome(status));
}

export type RunStatusIndicatorProps = Omit<
  OutcomeStatusIndicatorProps,
  "status"
> & {
  status?: RunStatusIndicatorStatus;
};

function RunStatusIndicator({
  status = "unknown",
  label,
  iconOnly = false,
  ...props
}: RunStatusIndicatorProps) {
  const resolved = status ?? "unknown";
  const customLabel = label != null && label.length > 0 ? label : undefined;

  return (
    <OutcomeStatusIndicator
      {...props}
      status={runStatusToOutcome(resolved)}
      label={customLabel ?? RUN_DEFAULT_LABEL[resolved]}
      iconOnly={iconOnly}
      data-slot="run-status-indicator"
      data-status={resolved}
    />
  );
}

export { RunStatusIndicator, runStatusIndicatorVariants };
