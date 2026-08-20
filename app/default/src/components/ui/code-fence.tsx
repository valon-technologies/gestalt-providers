/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
 */

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/cn";

// One code-fence surface for Plate fences and display CodeBlock.
// Highlight colors stay in typeset `.typeset-code-hljs` — this file owns
// only the shared chrome paint (shell / header / pre body), so the two
// presenters cannot drift by copy-pasting Tailwind strings.

// Nested canvas radius: Card / Alert / Callout (and DescriptionList outline,
// which composes cardVariants) publish `--radius-in-panel` on their variant
// classes. The fence consumes it. Standalone fences fall back to `--radius-lg`.
// Call sites do not pass a radius prop. Controls stay `rounded-md`; floating
// overlays stay `rounded-xl`.
const codeFenceShellVariants = cva(
  "overflow-hidden rounded-[var(--radius-in-panel,_var(--radius-lg))]",
  {
    variants: {
      variant: {
        /** Hairline frame, transparent fill — default for page / outline-card placement. */
        outline: "border border-border bg-transparent",
        /** Filled muted band — when a second outline frame is not enough contrast. */
        solid: "bg-muted",
        /**
         * Mid quiet L-step (`bg-muted-strong`) — darker solid fill on an
         * already-muted parent (Alert wash, solid Card). Same warmth as `solid`
         * / muted; only lightness drops.
         */
        "solid-dark": "bg-muted-strong",
      },
    },
    defaultVariants: {
      variant: "outline",
    },
  },
);

/** Default outline shell — one-off fences that skip CodeBlock composition. */
export const codeFenceShellClass = codeFenceShellVariants();

/** Hairline header row above the code body. */
export const codeFenceHeaderClass =
  "flex items-center justify-between gap-2 border-b border-border py-1 pl-2 pr-1";

/**
 * Mono body on the fence. Plate puts `typeset-code-hljs` on the element host;
 * display CodeBlock puts it on `<code>` — both paint the same token theme.
 */
export const codeFencePreClass =
  "overflow-x-auto px-4 py-3 font-mono text-sm leading-snug [tab-size:2]";

/** Token theme host class — colors come from typeset. */
export const codeFenceHighlightClass = "typeset-code-hljs";

/** Left edge for a highlighted source line — pairs with --code-line-emphasis wash. */
export const codeLineEmphasisEdgeClass =
  "shadow-[inset_2px_0_0_0_var(--color-accent-solid)]";

/** Wash fill for a highlighted source line (`--code-line-emphasis` in theme). */
export const codeLineEmphasisWashClass = "bg-code-line-emphasis";

/**
 * Full-bleed horizontal inset shared by every source row (highlighted or not)
 * so gutters stay column-aligned when a row picks up the emphasis wash.
 * Matches `codeFencePreClass` `px-4`.
 */
export const codeLineRowBleedClass = "-mx-4 px-4";

/**
 * Highlighted source row paint only — wash + inset accent edge.
 * Layout bleed lives on every row via `codeLineRowBleedClass` so a 2px
 * `border-l` never shifts the gutter relative to siblings.
 */
export const codeLineEmphasisRowClassName = cn(
  codeLineEmphasisEdgeClass,
  codeLineEmphasisWashClass,
);

/** @deprecated Use `codeLineEmphasisRowClassName` (+ `codeLineRowBleedClass`). */
export function codeLineEmphasisRowClass(_showLineNumbers?: boolean): string {
  return cn(codeLineEmphasisRowClassName, codeLineRowBleedClass);
}

export type CodeFenceShellProps = React.ComponentProps<"div"> &
  VariantProps<typeof codeFenceShellVariants>;

function CodeFenceShell({ className, variant, ...props }: CodeFenceShellProps) {
  return (
    <div
      className={cn(codeFenceShellVariants({ variant }), className)}
      {...props}
      data-slot="code-fence"
    />
  );
}

export type CodeFenceHeaderProps = React.ComponentProps<"div">;

function CodeFenceHeader({ className, ...props }: CodeFenceHeaderProps) {
  return (
    <div
      data-slot="code-fence-header"
      className={cn(codeFenceHeaderClass, className)}
      {...props}
    />
  );
}

export { CodeFenceShell, CodeFenceHeader, codeFenceShellVariants };
