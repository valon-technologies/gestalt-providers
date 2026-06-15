"use client";

// Slim adaptation of Vercel AI Elements `prompt-input.tsx` —
// https://github.com/vercel/ai-elements, Apache-2.0; see ./LICENSE.
// The upstream component is ~1,400 lines with attachments, model-picker
// scaffolding, and speech input over seven shadcn primitives; this keeps its
// compound API shape (PromptInput / Textarea / Toolbar / Tools / Submit) for
// the subset the agents composer needs. Textarea auto-grows via the
// Tailwind v4 `field-sizing-content` utility.
import { cn } from "@/lib/utils";
import { Loader2Icon, SendIcon, SquareIcon, XIcon } from "lucide-react";
import type { ComponentProps, KeyboardEventHandler } from "react";

import { Button } from "./primitives";

export type PromptInputStatus = "ready" | "submitted" | "streaming" | "error";

export type PromptInputProps = ComponentProps<"form">;

export const PromptInput = ({ className, ...props }: PromptInputProps) => (
  <form
    className={cn(
      "w-full divide-y divide-alpha overflow-hidden rounded-lg border border-alpha bg-background shadow-card",
      "focus-within:border-alpha-strong",
      className,
    )}
    {...props}
  />
);

export type PromptInputTextareaProps = ComponentProps<"textarea">;

export const PromptInputTextarea = ({
  className,
  onKeyDown,
  ...props
}: PromptInputTextareaProps) => {
  const handleKeyDown: KeyboardEventHandler<HTMLTextAreaElement> = (event) => {
    onKeyDown?.(event);
    if (event.defaultPrevented) return;
    // Enter submits; Shift+Enter inserts a newline. Cmd/Ctrl+Enter also
    // submits — muscle memory from the previous composer.
    if (
      event.key === "Enter" &&
      !event.nativeEvent.isComposing &&
      (event.metaKey || event.ctrlKey || !event.shiftKey)
    ) {
      event.preventDefault();
      event.currentTarget.form?.requestSubmit();
    }
  };

  return (
    <textarea
      aria-keyshortcuts="Enter Meta+Enter Control+Enter"
      className={cn(
        "max-h-48 min-h-16 w-full resize-none bg-transparent px-4 py-3 text-sm leading-6 text-primary outline-none [field-sizing:content] placeholder:text-faint",
        className,
      )}
      onKeyDown={handleKeyDown}
      rows={2}
      {...props}
    />
  );
};

export type PromptInputToolbarProps = ComponentProps<"div">;

export const PromptInputToolbar = ({
  className,
  ...props
}: PromptInputToolbarProps) => (
  <div
    className={cn("flex items-center justify-between gap-2 px-2 py-1.5", className)}
    {...props}
  />
);

export type PromptInputToolsProps = ComponentProps<"div">;

export const PromptInputTools = ({
  className,
  ...props
}: PromptInputToolsProps) => (
  <div className={cn("flex min-w-0 items-center gap-2", className)} {...props} />
);

export type PromptInputSubmitProps = ComponentProps<typeof Button> & {
  status?: PromptInputStatus;
};

export const PromptInputSubmit = ({
  className,
  status = "ready",
  children,
  ...props
}: PromptInputSubmitProps) => {
  let icon = <SendIcon className="size-4" />;
  if (status === "submitted") {
    icon = <Loader2Icon className="size-4 animate-spin" />;
  } else if (status === "streaming") {
    icon = <SquareIcon className="size-4" />;
  } else if (status === "error") {
    icon = <XIcon className="size-4" />;
  }

  return (
    <Button
      aria-label="Send message"
      className={cn("rounded-md", className)}
      size="icon"
      type="submit"
      {...props}
    >
      {children ?? icon}
    </Button>
  );
};
