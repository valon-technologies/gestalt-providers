import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";

import { cn } from "@/lib/utils";

// One code-fence surface for Plate fences and display CodeBlock.
// Highlight colors stay in valon-typeset `.typeset-code-hljs` — this file owns
// only the shared chrome paint (shell / header / pre body), so the two
// presenters cannot drift by copy-pasting Tailwind strings.

const codeFenceShellVariants = cva("overflow-hidden rounded-md", {
  variants: {
    variant: {
      /** Hairline frame, transparent fill — default for page / outline-card placement. */
      outline: "border border-border bg-transparent",
      /** Filled muted band — when a second outline frame is not enough contrast. */
      solid: "bg-muted",
    },
  },
  defaultVariants: {
    variant: "outline",
  },
});

/** Default outline shell — InstallCommand and other one-off chrome. */
export const codeFenceShellClass = codeFenceShellVariants();

/** Hairline header row above the code body. */
export const codeFenceHeaderClass =
  "flex items-center justify-between gap-2 border-b border-border py-1 pl-2 pr-1";

/**
 * Mono body on the fence. Plate puts `typeset-code-hljs` on the element host;
 * display CodeBlock puts it on `<code>` — both paint the same token theme.
 */
export const codeFencePreClass =
  "overflow-x-auto px-4 py-3 font-mono text-sm leading-[normal] [tab-size:2]";

/** Token theme host class — colors come from valon-typeset. */
export const codeFenceHighlightClass = "typeset-code-hljs";

/** Left edge for a highlighted source line — pairs with --code-line-emphasis wash. */
export const codeLineEmphasisEdgeClass = "border-l-2 border-accent-vivid";

/** Inset edge for gutter rows — paints the accent stroke without shifting code columns. */
export const codeLineEmphasisInsetEdgeClass =
  "shadow-[inset_2px_0_0_0_var(--color-accent-vivid)]";

/** Wash fill for a highlighted source line (`--code-line-emphasis` in theme). */
export const codeLineEmphasisWashClass = "bg-code-line-emphasis";

/**
 * Highlighted line row inside the pre body. With a line-number gutter, keep the
 * `gap-x-4` lane — do not negative-margin left into the gutter.
 */
export function codeLineEmphasisRowClass(showLineNumbers: boolean): string {
  const wash = codeLineEmphasisWashClass;
  return showLineNumbers
    ? `${wash} ${codeLineEmphasisInsetEdgeClass} -mr-4 pr-4`
    : `${codeLineEmphasisEdgeClass} ${wash} -mx-4 px-4 block`;
}

export type CodeFenceShellProps = React.ComponentProps<"div"> &
  VariantProps<typeof codeFenceShellVariants>;

function CodeFenceShell({ className, variant, ...props }: CodeFenceShellProps) {
  return (
    <div
      data-slot="code-fence"
      className={cn(codeFenceShellVariants({ variant }), className)}
      {...props}
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
