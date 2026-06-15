"use client";

// Adapted from Vercel AI Elements `conversation.tsx` —
// https://github.com/vercel/ai-elements, Apache-2.0; see ./LICENSE.
// Divergences: local Button primitive, no markdown download affordance,
// no AI SDK types.
import { cn } from "@/lib/utils";
import { ArrowDownIcon } from "lucide-react";
import type { ComponentProps } from "react";
import { useCallback, useEffect, useRef } from "react";
import { StickToBottom, useStickToBottomContext } from "use-stick-to-bottom";

import { Button } from "./primitives";

export type ConversationProps = ComponentProps<typeof StickToBottom>;

// Divergence from upstream: initial scroll is instant — a deep-linked long
// transcript should open at the latest message, not animate to it.
export const Conversation = ({ className, ...props }: ConversationProps) => (
  <StickToBottom
    className={cn("relative flex-1 overflow-y-hidden", className)}
    initial="instant"
    resize="smooth"
    role="log"
    {...props}
  />
);

export type ConversationContentProps = ComponentProps<typeof StickToBottom.Content>;

export const ConversationContent = ({
  className,
  ...props
}: ConversationContentProps) => (
  <StickToBottom.Content
    className={cn("flex flex-col gap-6 p-5", className)}
    {...props}
  />
);

export type ConversationEmptyStateProps = ComponentProps<"div"> & {
  title?: string;
  description?: string;
  icon?: React.ReactNode;
};

export const ConversationEmptyState = ({
  className,
  title = "No messages yet",
  description = "Start a conversation to see messages here",
  icon,
  children,
  ...props
}: ConversationEmptyStateProps) => (
  <div
    className={cn(
      "flex size-full flex-col items-center justify-center gap-3 p-8 text-center",
      className,
    )}
    {...props}
  >
    {children ?? (
      <>
        {icon && <div className="text-muted">{icon}</div>}
        <div className="space-y-1">
          <h3 className="text-sm font-medium text-primary">{title}</h3>
        {description && <p className="text-sm text-muted">{description}</p>}
        </div>
      </>
    )}
  </div>
);

// Divergence from upstream: stick-to-bottom handles streaming growth, but a
// transcript that arrives in one commit after an async replay can land with
// the scroller short of the end — and content keeps growing for a few frames
// afterwards (font swap, async highlighting). Pin to the bottom across that
// settling window, once per `ready` cycle (re-arms when ready flips back to
// false, e.g. on session switch). Downward programmatic scrolls re-engage
// the library's sticky state, so streaming behavior is unaffected.
const INITIAL_PIN_FRAMES = 20;

export const ConversationInitialScroll = ({ ready }: { ready: boolean }) => {
  const { scrollRef, scrollToBottom } = useStickToBottomContext();
  const scrolledRef = useRef(false);

  useEffect(() => {
    if (!ready) {
      scrolledRef.current = false;
      return;
    }
    if (scrolledRef.current) return;
    scrolledRef.current = true;

    void scrollToBottom("instant");
    let frames = 0;
    let raf = 0;
    const pin = () => {
      const el = scrollRef.current;
      if (el) {
        el.scrollTop = el.scrollHeight;
      }
      frames += 1;
      if (frames < INITIAL_PIN_FRAMES) {
        raf = requestAnimationFrame(pin);
      }
    };
    raf = requestAnimationFrame(pin);
    return () => cancelAnimationFrame(raf);
  }, [ready, scrollRef, scrollToBottom]);

  return null;
};

export type ConversationScrollButtonProps = ComponentProps<typeof Button>;

export const ConversationScrollButton = ({
  className,
  ...props
}: ConversationScrollButtonProps) => {
  const { isAtBottom, scrollToBottom } = useStickToBottomContext();

  const handleScrollToBottom = useCallback(() => {
    scrollToBottom();
  }, [scrollToBottom]);

  return (
    !isAtBottom && (
      <Button
        aria-label="Scroll to bottom"
        className={cn(
          "absolute bottom-4 left-[50%] translate-x-[-50%] rounded-full shadow-dropdown",
          className,
        )}
        onClick={handleScrollToBottom}
        size="icon"
        type="button"
        variant="outline"
        {...props}
      >
        <ArrowDownIcon className="size-4" />
      </Button>
    )
  );
};
