/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 * Registry `table-status-indicator` (toolshed#4287, #4289). Thin `variant`
 * adapter over `OutcomeStatusIndicator`.
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
 * Table-row `variant` adapter over `OutcomeStatusIndicator`.
 * Prefer `OutcomeStatusIndicator` for new table gutters (`status` + `iconOnly`).
 *
 * Kept for the published `variant` API (`danger` / `info` / `default`) used by
 * existing severity columns.
 *
 * Root identity is table vocabulary: `data-slot="table-status-indicator"` and
 * `data-variant`. The primitive's `data-status` is stripped (`undefined`) so
 * table roots do not leak outcome vocabulary. Run overrides `data-status`
 * instead because its public prop is also `status`.
 *
 * `iconOnly` gutters default to `sm` — compact supporting chrome beside the
 * status column, not a second hero glyph.
 */
export type TableStatusIndicatorVariant =
  | "default"
  | "success"
  | "danger"
  | "warning"
  | "info";

export type TableStatusIndicatorSize = OutcomeStatusIndicatorSize;

const VARIANT_TO_OUTCOME: Record<TableStatusIndicatorVariant, OutcomeStatus> = {
  default: "unknown",
  success: "success",
  danger: "failure",
  warning: "warning",
  info: "info",
};

const VARIANT_DEFAULT_LABEL: Record<TableStatusIndicatorVariant, string> = {
  default: "Normal",
  success: "Succeeded",
  danger: "Failed",
  warning: "Caution",
  info: "Information",
};

export function tableVariantToOutcome(
  variant: TableStatusIndicatorVariant,
): OutcomeStatus {
  return VARIANT_TO_OUTCOME[variant];
}

/** Prefer `outcomeStatusIndicatorVariants` + `tableVariantToOutcome` for new code. */
function tableStatusIndicatorVariants({
  variant,
  size,
  className,
}: {
  variant?: TableStatusIndicatorVariant | null;
  size?: TableStatusIndicatorSize | null;
  className?: string;
} = {}) {
  return outcomeStatusIndicatorVariants({
    status: variant ? tableVariantToOutcome(variant) : undefined,
    size: size ?? undefined,
    className,
  });
}

export function tableStatusIndicatorBadgeVariant(
  variant: TableStatusIndicatorVariant,
): ReturnType<typeof outcomeStatusIndicatorBadgeVariant> {
  return outcomeStatusIndicatorBadgeVariant(tableVariantToOutcome(variant));
}

export type TableStatusIndicatorProps = Omit<
  OutcomeStatusIndicatorProps,
  "status"
> & {
  variant?: TableStatusIndicatorVariant;
};

function TableStatusIndicator({
  variant = "default",
  label,
  iconOnly = false,
  size,
  ...props
}: TableStatusIndicatorProps) {
  const resolved = variant ?? "default";
  const customLabel = label != null && label.length > 0 ? label : undefined;
  const resolvedSize = size ?? (iconOnly ? "sm" : undefined);

  return (
    <OutcomeStatusIndicator
      {...props}
      status={tableVariantToOutcome(resolved)}
      label={customLabel ?? VARIANT_DEFAULT_LABEL[resolved]}
      iconOnly={iconOnly}
      size={resolvedSize}
      data-slot="table-status-indicator"
      data-variant={resolved}
      data-status={undefined}
    />
  );
}

export { TableStatusIndicator, tableStatusIndicatorVariants };
