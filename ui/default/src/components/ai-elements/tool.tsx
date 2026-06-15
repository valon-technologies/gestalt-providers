"use client";

// Adapted from Vercel AI Elements `tool.tsx` —
// https://github.com/vercel/ai-elements, Apache-2.0; see ./LICENSE.
// Divergences: local ToolState union instead of `ai`'s ToolUIPart (the page
// maps gestaltd tool events onto it), local primitives, ShikiCode instead of
// the registry CodeBlock, status colors on the theme's pinned palette and
// status tokens instead of hardcoded Tailwind palette classes.
import ShikiCode from "@/components/ShikiCode";
import { cn } from "@/lib/utils";
import {
  CheckCircleIcon,
  ChevronDownIcon,
  CircleIcon,
  ClockIcon,
  WrenchIcon,
  XCircleIcon,
} from "lucide-react";
import type { ComponentProps, ReactNode } from "react";

import {
  Badge,
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "./primitives";

export type ToolState =
  | "input-streaming"
  | "input-available"
  | "output-available"
  | "output-error"
  | "approval-requested"
  | "approval-responded"
  | "output-denied";

export type ToolProps = ComponentProps<typeof Collapsible>;

export const Tool = ({ className, ...props }: ToolProps) => (
  <Collapsible
    className={cn(
      "group not-prose w-full rounded-md border border-alpha bg-surface/50",
      className,
    )}
    {...props}
  />
);

const statusLabels: Record<ToolState, string> = {
  "approval-requested": "Awaiting approval",
  "approval-responded": "Responded",
  "input-available": "Running",
  "input-streaming": "Pending",
  "output-available": "Completed",
  "output-denied": "Denied",
  "output-error": "Error",
};

const statusIcons: Record<ToolState, ReactNode> = {
  "approval-requested": <ClockIcon className="size-3.5 text-amber-600" />,
  "approval-responded": <CheckCircleIcon className="size-3.5 text-sky-600" />,
  "input-available": <ClockIcon className="size-3.5 animate-pulse text-sky-600" />,
  "input-streaming": <CircleIcon className="size-3.5 text-faint" />,
  "output-available": <CheckCircleIcon className="size-3.5 text-success" />,
  "output-denied": <XCircleIcon className="size-3.5 text-amber-600" />,
  "output-error": <XCircleIcon className="size-3.5 text-danger" />,
};

export const getStatusBadge = (status: ToolState) => (
  <Badge className="gap-1.5 rounded-full" variant="secondary">
    {statusIcons[status]}
    {statusLabels[status]}
  </Badge>
);

export type ToolHeaderProps = ComponentProps<typeof CollapsibleTrigger> & {
  title: string;
  state: ToolState;
};

export const ToolHeader = ({
  className,
  title,
  state,
  ...props
}: ToolHeaderProps) => (
  <CollapsibleTrigger
    className={cn(
      "flex w-full items-center justify-between gap-4 px-3 py-2.5",
      className,
    )}
    {...props}
  >
    <div className="flex min-w-0 items-center gap-2">
      <WrenchIcon className="size-4 shrink-0 text-muted" />
      <span className="truncate font-mono text-sm text-primary">{title}</span>
      {getStatusBadge(state)}
    </div>
    <ChevronDownIcon className="size-4 shrink-0 text-muted transition-transform duration-150 group-data-[state=open]:rotate-180" />
  </CollapsibleTrigger>
);

export type ToolContentProps = ComponentProps<typeof CollapsibleContent>;

export const ToolContent = ({ className, ...props }: ToolContentProps) => (
  <CollapsibleContent
    className={cn("space-y-3 border-t border-alpha p-3", className)}
    {...props}
  />
);

export type ToolInputProps = ComponentProps<"div"> & {
  input: unknown;
};

export const ToolInput = ({ className, input, ...props }: ToolInputProps) => {
  if (input === undefined || input === null || input === "") return null;
  const code =
    typeof input === "string" ? input : JSON.stringify(input, null, 2);
  return (
    <div className={cn("space-y-1.5 overflow-hidden", className)} {...props}>
      <h4 className="label-text">Input</h4>
      <div className="doc-code text-xs">
        <ShikiCode language="json" text={code} />
      </div>
    </div>
  );
};

export type ToolOutputProps = ComponentProps<"div"> & {
  output?: unknown;
  errorText?: string;
};

export const ToolOutput = ({
  className,
  output,
  errorText,
  ...props
}: ToolOutputProps) => {
  if (!output && !errorText) return null;

  let rendered: ReactNode = null;
  if (typeof output === "object" && output !== null) {
    rendered = (
      <div className="doc-code text-xs">
        <ShikiCode language="json" text={JSON.stringify(output, null, 2)} />
      </div>
    );
  } else if (output !== undefined && output !== null && output !== "") {
    rendered = (
      <pre className="max-h-56 overflow-auto whitespace-pre-wrap break-words rounded-md bg-alpha-5 p-2.5 font-mono text-xs leading-5 text-secondary">
        {String(output)}
      </pre>
    );
  }

  return (
    <div className={cn("space-y-1.5", className)} {...props}>
      <h4 className="label-text">{errorText ? "Error" : "Output"}</h4>
      {errorText ? (
        <div className="rounded-md bg-danger/10 p-2.5 text-xs text-danger">
          {errorText}
        </div>
      ) : null}
      {rendered}
    </div>
  );
};
