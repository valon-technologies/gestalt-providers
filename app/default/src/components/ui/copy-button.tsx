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
  // via `open = copied || intentOpen` after leave. Suppress only the
  // pointerdown dismiss before click. Do not use pointer capture or leave
  // clears mid-press (those remount Copied on icon-xs slop). End-of-press
  // without click clears intent via window pointerup + setTimeout(0) (runs
  // after click — microtasks flush before click).
  const [intentOpen, setIntentOpen] = React.useState(false);
  const pointerDownRef = React.useRef(false);
  const clickedRef = React.useRef(false);
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
        if (!next && pointerDownRef.current) return;
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
            clickedRef.current = false;
            const endPress = () => {
              window.removeEventListener("pointerup", endPress, true);
              window.removeEventListener("pointercancel", endPress, true);
              // After click (same task queue): keep confirm via `copied`.
              // Microtasks flush *before* click — use setTimeout(0) instead.
              // Without click (released outside): drop intent so the tip cannot stick.
              window.setTimeout(() => {
                pointerDownRef.current = false;
                if (!clickedRef.current) setIntentOpen(false);
              }, 0);
            };
            window.addEventListener("pointerup", endPress, true);
            window.addEventListener("pointercancel", endPress, true);
            props.onPointerDownCapture?.(event);
          }}
          onClick={() => {
            const text = typeof value === "function" ? value() : value;
            void navigator.clipboard.writeText(text);
            clickedRef.current = true;
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
