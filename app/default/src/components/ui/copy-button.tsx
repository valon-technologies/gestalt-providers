/**
 * Vendored Gestalt UI primitive — refresh from the upstream design-system registry when syncing.
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
  // Hover/focus intent from Radix. Confirmation (`copied`) keeps the tip open
  // after click. `pointerDownRef` ignores the pointerdown dismiss that fires
  // *before* click — otherwise open flickers closed and the tip remounts/fades.
  const [intentOpen, setIntentOpen] = React.useState(false);
  const pointerDownRef = React.useRef(false);
  const open = copied || intentOpen;

  React.useEffect(() => {
    if (!copied) return;
    const timer = window.setTimeout(() => setCopied(false), 2000);
    return () => window.clearTimeout(timer);
  }, [copied]);

  const label = copied ? copiedLabel : tooltip;
  const size = sizeProp ?? "icon-xs";

  return (
    <Tooltip
      open={open}
      onOpenChange={(next) => {
        if (!next && (copied || pointerDownRef.current)) return;
        setIntentOpen(next);
      }}
    >
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
          onPointerDownCapture={(event) => {
            pointerDownRef.current = true;
            props.onPointerDownCapture?.(event);
          }}
          onPointerUpCapture={(event) => {
            pointerDownRef.current = false;
            props.onPointerUpCapture?.(event);
          }}
          onPointerCancelCapture={(event) => {
            pointerDownRef.current = false;
            props.onPointerCancelCapture?.(event);
          }}
          onClick={() => {
            const text = typeof value === "function" ? value() : value;
            void navigator.clipboard.writeText(text);
            setCopied(true);
            setIntentOpen(true);
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
