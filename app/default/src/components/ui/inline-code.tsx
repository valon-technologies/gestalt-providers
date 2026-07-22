import type { ComponentProps } from "react";
import { cn } from "@/lib/cn";

/**
 * Gestalt console vendor of Registry inline code paint.
 *
 * Ownership: Valon Registry has no standalone `InlineCode` export — the
 * contract lives on Plate `code-node` / `code-node-static`
 * (`valon-tools/apps/registry/ui/src/ui/code-node.tsx`) and typeset
 * `:not(pre) > code`. This is that chrome as a reusable `<code>` for
 * app UI (field hints, docs copy) outside Plate / `.typeset`.
 */

export type InlineCodeProps = ComponentProps<"code">;

function InlineCode({ className, ...props }: InlineCodeProps) {
  return (
    <code
      data-slot="inline-code"
      className={cn(
        "whitespace-pre-wrap rounded-md bg-muted px-[0.3em] py-[0.2em] font-mono text-[0.85em]",
        className,
      )}
      {...props}
    />
  );
}

export { InlineCode };
