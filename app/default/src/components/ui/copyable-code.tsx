"use client";

import * as React from "react";
import { cva } from "class-variance-authority";

import { codeVariants } from "@/components/ui/code";
import { CopyIconButton } from "@/components/ui/copy-button";
import {
  TooltipProvider,
} from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";

// Copyable identifier chip — `codeVariants()` paint on a two-cell shell (text +
// action) so the copy affordance is em-scaled and inset, not a toolbar `icon-xs`
// box jammed inside the border.
const copyableCodeVariants = cva(
  cn(
    codeVariants(),
    "inline-flex max-w-full align-baseline whitespace-nowrap px-0 py-0",
  ),
);

const copyableCodeTextVariants = cva(
  "min-w-0 truncate px-[0.25em] py-[0.12em]",
);

const copyableCodeActionVariants = cva(
  "flex shrink-0 items-center self-stretch border-l border-border/50 p-[0.12em]",
);

export type CopyableCodeProps = {
  /** Clipboard payload — may differ from visible children when truncated. */
  value: string;
  children?: React.ReactNode;
  className?: string;
  tooltip?: string;
};

function CopyableCode({
  value,
  children,
  className,
  tooltip = "Copy",
}: CopyableCodeProps) {
  const display = children ?? value;
  const isTruncated =
    children != null &&
    (typeof children === "string" || typeof children === "number") &&
    String(children).trim() !== value.trim();

  return (
    <span
      data-slot="copyable-code"
      className={cn(copyableCodeVariants(), className)}
    >
      <code
        className={copyableCodeTextVariants()}
        title={isTruncated ? value : undefined}
        aria-label={isTruncated ? value : undefined}
      >
        {display}
      </code>
      <span className={copyableCodeActionVariants()}>
        <TooltipProvider delayDuration={0}>
          <CopyIconButton
            density="chip"
            value={value}
            tooltip={isTruncated ? `Copy ${value}` : tooltip}
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
