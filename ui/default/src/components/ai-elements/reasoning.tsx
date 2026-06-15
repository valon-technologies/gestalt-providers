"use client";

// Adapted from Vercel AI Elements `reasoning.tsx` —
// https://github.com/vercel/ai-elements, Apache-2.0; see ./LICENSE.
// Divergences: local Collapsible/Shimmer primitives (no Radix, no motion),
// inline controllable-state logic, Response (ShikiCode-wired Streamdown)
// for the content.
import { cn } from "@/lib/utils";
import { BrainIcon, ChevronDownIcon } from "lucide-react";
import type { ComponentProps, ReactNode } from "react";
import {
  createContext,
  memo,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
  Shimmer,
} from "./primitives";
import { Response } from "./response";

interface ReasoningContextValue {
  isStreaming: boolean;
  isOpen: boolean;
  duration: number | undefined;
}

const ReasoningContext = createContext<ReasoningContextValue | null>(null);

const useReasoning = () => {
  const context = useContext(ReasoningContext);
  if (!context) {
    throw new Error("Reasoning components must be used within Reasoning");
  }
  return context;
};

export type ReasoningProps = ComponentProps<typeof Collapsible> & {
  isStreaming?: boolean;
  duration?: number;
};

const AUTO_CLOSE_DELAY = 1000;
const MS_IN_S = 1000;

export const Reasoning = memo(
  ({
    className,
    isStreaming = false,
    open,
    defaultOpen,
    onOpenChange,
    duration: durationProp,
    children,
    ...props
  }: ReasoningProps) => {
    // Open state auto-follows streaming unless the caller controls it.
    const isExplicitlyClosed = defaultOpen === false;
    const [uncontrolledOpen, setUncontrolledOpen] = useState(
      defaultOpen ?? isStreaming,
    );
    const isOpen = open ?? uncontrolledOpen;
    const setIsOpen = (next: boolean) => {
      if (open === undefined) setUncontrolledOpen(next);
      onOpenChange?.(next);
    };

    const [measuredDuration, setMeasuredDuration] = useState<number>();
    const duration = durationProp ?? measuredDuration;

    const hasEverStreamedRef = useRef(isStreaming);
    const hasAutoClosedRef = useRef(false);
    const startTimeRef = useRef<number | null>(null);

    useEffect(() => {
      if (isStreaming) {
        hasEverStreamedRef.current = true;
        startTimeRef.current ??= Date.now();
      } else if (startTimeRef.current !== null) {
        setMeasuredDuration(
          Math.ceil((Date.now() - startTimeRef.current) / MS_IN_S),
        );
        startTimeRef.current = null;
      }
    }, [isStreaming]);

    // Auto-open while streaming, auto-close shortly after it ends (once).
    useEffect(() => {
      if (isStreaming && !isOpen && !isExplicitlyClosed) {
        setIsOpen(true);
      }
      if (
        hasEverStreamedRef.current &&
        !isStreaming &&
        isOpen &&
        !hasAutoClosedRef.current
      ) {
        const timer = setTimeout(() => {
          setIsOpen(false);
          hasAutoClosedRef.current = true;
        }, AUTO_CLOSE_DELAY);
        return () => clearTimeout(timer);
      }
      // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [isStreaming, isOpen, isExplicitlyClosed]);

    const contextValue = useMemo(
      () => ({ duration, isOpen, isStreaming }),
      [duration, isOpen, isStreaming],
    );

    return (
      <ReasoningContext.Provider value={contextValue}>
        <Collapsible
          className={cn("not-prose", className)}
          onOpenChange={setIsOpen}
          open={isOpen}
          {...props}
        >
          {children}
        </Collapsible>
      </ReasoningContext.Provider>
    );
  },
);

export type ReasoningTriggerProps = ComponentProps<typeof CollapsibleTrigger>;

const thinkingMessage = (isStreaming: boolean, duration?: number): ReactNode => {
  if (isStreaming || duration === 0) {
    return <Shimmer>Thinking…</Shimmer>;
  }
  if (duration === undefined) {
    return <span>Thought for a few seconds</span>;
  }
  return <span>Thought for {duration} second{duration === 1 ? "" : "s"}</span>;
};

export const ReasoningTrigger = memo(
  ({ className, children, ...props }: ReasoningTriggerProps) => {
    const { isStreaming, isOpen, duration } = useReasoning();

    return (
      <CollapsibleTrigger
        className={cn(
          "flex w-full items-center gap-2 text-sm text-muted transition-colors duration-150 hover:text-primary",
          className,
        )}
        {...props}
      >
        {children ?? (
          <>
            <BrainIcon className="size-4" />
            {thinkingMessage(isStreaming, duration)}
            <ChevronDownIcon
              className={cn(
                "size-4 transition-transform duration-150",
                isOpen ? "rotate-180" : "rotate-0",
              )}
            />
          </>
        )}
      </CollapsibleTrigger>
    );
  },
);

export type ReasoningContentProps = ComponentProps<typeof CollapsibleContent> & {
  children: string;
};

export const ReasoningContent = memo(
  ({ className, children, ...props }: ReasoningContentProps) => (
    <CollapsibleContent
      className={cn(
        "mt-3 border-l-2 border-alpha pl-4 text-sm text-muted",
        className,
      )}
      {...props}
    >
      <Response>{children}</Response>
    </CollapsibleContent>
  ),
);

Reasoning.displayName = "Reasoning";
ReasoningTrigger.displayName = "ReasoningTrigger";
ReasoningContent.displayName = "ReasoningContent";
