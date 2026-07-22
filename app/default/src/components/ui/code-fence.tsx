import type { ComponentProps } from "react";

import { cn } from "@/lib/cn";

/**
 * Gestalt console vendor of Valon Registry `code-fence`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/code-fence.tsx`). Shared shell for
 * display `CodeBlock`. Highlight colors live in `typeset-code-hljs.css`
 * (`.typeset-code-hljs`). Shell paint uses Registry muted/border recipes.
 */

// One code-fence surface for Plate fences and display CodeBlock.
// Highlight colors stay in valon-typeset `.typeset-code-hljs` — this file owns
// only the shared chrome paint (shell / header / pre body), so the two
// presenters cannot drift by copy-pasting Tailwind strings.

/** Muted rounded shell shared by display CodeBlock. */
export const codeFenceShellClass = "overflow-hidden rounded-md bg-muted/50";

/** Hairline header row above the code body. */
export const codeFenceHeaderClass =
  "flex items-center justify-between gap-2 border-b border-border/50 px-2 py-1";

/**
 * Mono body on the fence. Display CodeBlock puts `typeset-code-hljs` on
 * `<code>` so token colors match Registry / Plate.
 */
export const codeFencePreClass =
  "overflow-x-auto px-4 py-3 font-mono text-sm leading-[normal] [tab-size:2]";

/** Token theme host class — colors come from typeset-code-hljs.css. */
export const codeFenceHighlightClass = "typeset-code-hljs";

export type CodeFenceShellProps = ComponentProps<"div">;

function CodeFenceShell({ className, ...props }: CodeFenceShellProps) {
  return (
    <div
      data-slot="code-fence"
      className={cn(codeFenceShellClass, className)}
      {...props}
    />
  );
}

export type CodeFenceHeaderProps = ComponentProps<"div">;

function CodeFenceHeader({ className, ...props }: CodeFenceHeaderProps) {
  return (
    <div
      data-slot="code-fence-header"
      className={cn(codeFenceHeaderClass, className)}
      {...props}
    />
  );
}

export { CodeFenceShell, CodeFenceHeader };
