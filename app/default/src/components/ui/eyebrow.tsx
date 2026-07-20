import { forwardRef, type HTMLAttributes } from "react";
import { cn } from "@/lib/cn";

/**
 * Gestalt console vendor of Valon Registry `eyebrow`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/eyebrow.tsx`). Token-adapted only —
 * same public API (`tone`). Prefer Registry install when the console consumes
 * Valon registry.
 *
 * @see toolshed/valon-tools/registry/guidelines/eyebrow.md
 * @see RES-20260717-006
 */

type EyebrowTone = "muted" | "brand";

const toneClass: Record<EyebrowTone, string> = {
  // Registry: text-muted-foreground / text-brand
  muted: "text-muted",
  brand: "text-brand",
};

export type EyebrowProps = HTMLAttributes<HTMLSpanElement> & {
  tone?: EyebrowTone;
};

export const Eyebrow = forwardRef<HTMLSpanElement, EyebrowProps>(
  function Eyebrow({ className, tone = "muted", ...props }, ref) {
    return (
      <span
        ref={ref}
        data-slot="eyebrow"
        className={cn(
          // Registry recipe: text-xs font-medium uppercase tracking-wider leading-none
          "text-xs font-medium uppercase tracking-wider leading-none",
          toneClass[tone],
          className,
        )}
        {...props}
      />
    );
  },
);
