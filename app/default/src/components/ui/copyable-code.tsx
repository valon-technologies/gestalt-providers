
/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { codeVariants } from "@/components/ui/code";
import {
  CopyIconButton,
  SecretRevealButton,
} from "@/components/ui/copy-button";
import { TooltipProvider } from "@/components/ui/tooltip";
import { cn } from "@/lib/cn";

const MASK_HEAD = 4;
const MASK_TAIL = 4;

/**
 * Visible stand-in for a sensitive CopyableCode value. Keeps a letter bookend
 * so the mono baseline does not jump when `*` sits higher than digits, and
 * fills the middle with the same number of stars so chip width stays put.
 * Clipboard still uses the full `value`.
 */
export function maskCopyableValue(value: string): string {
  const length = value.length;
  if (length <= 1) return "*".repeat(length);
  if (length === 2) return `${value[0]}*`;
  if (length <= MASK_HEAD + MASK_TAIL) {
    return `${value[0]}${"*".repeat(length - 2)}${value[length - 1]}`;
  }
  return `${value.slice(0, MASK_HEAD)}${"*".repeat(length - MASK_HEAD - MASK_TAIL)}${value.slice(-MASK_TAIL)}`;
}

/** Replace each secret with `maskCopyableValue(secret)`. Longer secrets first. */
export function maskSecretsInText(
  text: string,
  secrets: readonly string[],
): string {
  const unique = [
    ...new Set(secrets.filter((secret) => secret.length > 0)),
  ].sort((a, b) => b.length - a.length);
  return unique.reduce(
    (current, secret) => current.split(secret).join(maskCopyableValue(secret)),
    text,
  );
}

function copyableDisplayValue(
  value: string,
  unmasked: React.ReactNode,
  revealed: boolean,
  secrets: readonly string[] | undefined,
): React.ReactNode {
  if (revealed) return unmasked;
  const activeSecrets = (secrets ?? []).filter((secret) => secret.length > 0);
  if (activeSecrets.length > 0) {
    const source = typeof unmasked === "string" ? unmasked : value;
    return maskSecretsInText(source, activeSecrets);
  }
  return maskCopyableValue(value);
}

const copyableCodeIconSizeClass = {
  sm: undefined,
  md: "[&_svg:not([class*='size-'])]:size-[1em]",
  lg: "[&_svg:not([class*='size-'])]:size-[1.125em]",
} as const;

// Copyable identifier chip — `codeVariants()` paint on a two-cell shell (text +
// action) so the copy affordance is em-scaled and inset, not a toolbar `icon-xs`
// box jammed inside the border.
const copyableCodeVariants = cva(
  cn(
    codeVariants(),
    "inline-flex max-w-full align-baseline whitespace-nowrap px-0 py-0",
  ),
  {
    variants: {
      size: {
        sm: "",
        md: "text-sm",
        lg: "text-base",
      },
    },
    defaultVariants: {
      size: "sm",
    },
  },
);

const copyableCodeTextVariants = cva(
  "min-w-0 truncate px-[0.25em] py-[0.12em]",
);

const copyableCodeActionVariants = cva(
  "flex shrink-0 items-stretch self-stretch border-l border-border/50",
);

export type CopyableCodeProps = {
  /** Clipboard payload — may differ from visible children when truncated. */
  value: string;
  children?: React.ReactNode;
  className?: string;
  tooltip?: string;
  /**
   * Mask the visible text and add a show/hide control next to copy.
   * Copy still writes the full `value`.
   */
  sensitive?: boolean;
  /**
   * When `sensitive`, mask these substrings instead of the whole `value`
   * (for example keep `Bearer ` visible on an Authorization chip).
   */
  secrets?: readonly string[];
  /** Tooltip / accessible name for the masked-state reveal control. */
  revealLabel?: string;
  /** Tooltip / accessible name for the revealed-state hide control. */
  hideLabel?: string;
} & VariantProps<typeof copyableCodeVariants>;

function CopyableCode({
  value,
  children,
  className,
  tooltip = "Copy",
  sensitive = false,
  secrets,
  revealLabel = "Show",
  hideLabel = "Hide",
  size = "sm",
}: CopyableCodeProps) {
  const [revealed, setRevealed] = React.useState(false);
  const unmasked = children ?? value;
  const display = sensitive
    ? copyableDisplayValue(value, unmasked, revealed, secrets)
    : unmasked;
  const iconSizeClass = copyableCodeIconSizeClass[size ?? "sm"];

  return (
    <span
      data-slot="copyable-code"
      data-size={size ?? "sm"}
      data-sensitive={sensitive ? "true" : undefined}
      data-revealed={sensitive ? (revealed ? "true" : "false") : undefined}
      className={cn(copyableCodeVariants({ size }), className)}
    >
      <code className={copyableCodeTextVariants()}>{display}</code>
      <span className={copyableCodeActionVariants()}>
        <TooltipProvider delayDuration={0}>
          {sensitive ? (
            <>
              <SecretRevealButton
                density="chip"
                data-slot="copyable-code-reveal"
                revealed={revealed}
                onToggle={() => setRevealed((current) => !current)}
                showLabel={revealLabel}
                hideLabel={hideLabel}
                className={iconSizeClass}
              />
              <span
                className="w-px self-stretch bg-border/50"
                aria-hidden
              />
            </>
          ) : null}
          <CopyIconButton
            density="chip"
            value={value}
            className={iconSizeClass}
            // Keep tooltip short — never paste the clipboard payload into the
            // tip (handles can be huge base64url Temporal ids).
            tooltip={tooltip}
          />
        </TooltipProvider>
      </span>
    </span>
  );
}

export {
  CopyableCode,
  copyableCodeVariants,
  copyableCodeTextVariants,
  copyableCodeActionVariants,
};
