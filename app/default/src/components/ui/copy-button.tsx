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
  /** Shown briefly when `navigator.clipboard.writeText` rejects. */
  failedLabel?: string;
  /** `toolbar` = fixed `icon-xs` (CodeBlock, InputGroup). `chip` = fills CopyableCode's trailing cell. */
  density?: CopyIconButtonDensity;
};

type CopyFeedback = "idle" | "copied" | "failed";

// Chip copy fills CopyableCode's trailing cell. `before` expands the hit
// target vertically and past the trailing edge — not into the identifier text.
// Button's press scrim already owns `after`.
const COPY_ICON_CHIP_CLASS =
  "size-auto h-auto min-h-0 min-w-[1.15em] w-auto self-stretch rounded-none px-[0.25em] py-0 text-inherit before:absolute before:-top-1.5 before:-right-1.5 before:-bottom-1.5 before:left-0 before:content-[''] [&_svg:not([class*='size-'])]:size-[0.7em]";

function CopyIconButton({
  value,
  tooltip = "Copy",
  copiedLabel = "Copied",
  failedLabel = "Couldn't copy",
  density = "toolbar",
  className,
  size: sizeProp,
  ...props
}: CopyIconButtonProps) {
  const [feedback, setFeedback] = React.useState<CopyFeedback>("idle");
  // Hover/focus intent from Radix. Confirmation (`copied` / `failed`) keeps the
  // tip open via `open = feedback !== "idle" || intentOpen` after leave.
  // Suppress only the pointerdown dismiss before click. Do not use pointer
  // capture or leave clears mid-press (those remount confirmation on icon-xs
  // slop). End-of-press without click clears intent via window pointerup +
  // setTimeout(0) (runs after click — microtasks flush before click).
  const [intentOpen, setIntentOpen] = React.useState(false);
  const pointerDownRef = React.useRef(false);
  const clickedRef = React.useRef(false);
  const open = feedback !== "idle" || intentOpen;

  React.useEffect(() => {
    if (feedback === "idle") return;
    const timer = window.setTimeout(() => setFeedback("idle"), 2000);
    return () => window.clearTimeout(timer);
  }, [feedback]);

  const label =
    feedback === "copied"
      ? copiedLabel
      : feedback === "failed"
        ? failedLabel
        : tooltip;
  // Chip density is a cell fill, not a toolbar icon-xs square. Null skips
  // Button's default `size` so we do not start from `h-control-default` either.
  const size = sizeProp ?? (density === "chip" ? null : "icon-xs");

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
            "shrink-0",
            density === "chip" &&
              COPY_ICON_CHIP_CLASS,
            className,
          )}
          aria-label={label}
          onPointerDownCapture={(event) => {
            pointerDownRef.current = true;
            clickedRef.current = false;
            const endPress = () => {
              window.removeEventListener("pointerup", endPress, true);
              window.removeEventListener("pointercancel", endPress, true);
              // After click (same task queue): keep confirm via feedback.
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
            clickedRef.current = true;
            void (async () => {
              try {
                await navigator.clipboard.writeText(text);
                setFeedback("copied");
              } catch {
                // Never claim success — keep the copy glyph and surface failure
                // on the same tooltip / aria-label plane (no ambient toast).
                setFeedback("failed");
              }
            })();
          }}
        >
          {feedback === "copied" ? <CheckIcon /> : <CopyIcon />}
          <span className="sr-only" aria-live="polite">
            {feedback === "idle" ? "" : label}
          </span>
        </Button>
      </TooltipTrigger>
      <TooltipContent>{label}</TooltipContent>
    </Tooltip>
  );
}

export { CopyIconButton };
