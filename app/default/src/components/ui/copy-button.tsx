"use client";

/**
 * Gestalt console vendor of Valon Registry `copy-button`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/copy-button.tsx`).
 * Synced from toolshed origin/main — token adaptation only (`@/lib/cn` path).
 * Do not restyle chrome at call sites; change Registry first.
 */

import * as React from "react";
import { CheckIcon, CopyIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { cn } from "@/lib/cn";

export type CopyIconButtonDensity = "toolbar" | "chip";

export type CopyIconButtonProps = Omit<
  React.ComponentProps<typeof Button>,
  "onClick" | "children" | "value" | "aria-label"
> & {
  value: string | (() => string);
  tooltip?: string;
  copiedLabel?: string;
  /** `toolbar` = fixed `icon-xs` (CodeBlock, InputGroup). `chip` = em-scaled (CopyableCode). */
  density?: CopyIconButtonDensity;
};

function CopyIconButton({
  value,
  tooltip = "Copy",
  copiedLabel = "Copied",
  density = "toolbar",
  className,
  size: sizeProp,
  ...props
}: CopyIconButtonProps) {
  const [copied, setCopied] = React.useState(false);

  React.useEffect(() => {
    if (!copied) return;
    const timer = window.setTimeout(() => setCopied(false), 2000);
    return () => window.clearTimeout(timer);
  }, [copied]);

  const label = copied ? copiedLabel : tooltip;
  const size = sizeProp ?? "icon-xs";

  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button
          type="button"
          variant="ghost"
          size={size}
          {...props}
          className={cn(
            "shrink-0 text-muted-foreground",
            density === "chip" &&
              "text-inherit h-[1.15em] w-[1.15em] min-w-[1.15em] min-h-0 p-0 [&_svg:not([class*='size-'])]:size-[0.7em]",
            className,
          )}
          aria-label={label}
          onClick={() => {
            const text = typeof value === "function" ? value() : value;
            void navigator.clipboard.writeText(text);
            setCopied(true);
          }}
        >
          {copied ? <CheckIcon /> : <CopyIcon />}
        </Button>
      </TooltipTrigger>
      <TooltipContent>{label}</TooltipContent>
    </Tooltip>
  );
}

export { CopyIconButton };
