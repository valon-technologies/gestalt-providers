import { useEffect, useRef, useState } from "react";
import { CheckIcon, CopyIcon, SpinnerIcon } from "@/components/icons";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { cn } from "@/lib/cn";
import type { AppPromptExample } from "@/lib/appPromptExamples";
import {
  createPromptCopyController,
  type PromptCopyController,
  type PromptCopyState,
} from "@/lib/promptCopy";

type AppPromptExamplePromoProps = {
  /** Catalog display name used as the `@…` handle. */
  displayName: string;
  /** One or more prompt bodies stacked inside a single promo card. */
  prompts: AppPromptExample[];
  className?: string;
};

type PromptPillProps = {
  handle: string;
  prompt: AppPromptExample;
  onCopyStateChange: (promptId: string, state: PromptCopyState) => void;
};

function promptSnippet(text: string, max = 48): string {
  const trimmed = text.trim();
  if (trimmed.length <= max) return trimmed;
  return `${trimmed.slice(0, max - 1).trimEnd()}…`;
}

function PromptPill({ handle, prompt, onCopyStateChange }: PromptPillProps) {
  const [copyState, setCopyState] = useState<PromptCopyState>("idle");
  const copyController = useRef<PromptCopyController | null>(null);
  const onCopyStateChangeRef = useRef(onCopyStateChange);
  onCopyStateChangeRef.current = onCopyStateChange;
  const body = prompt.text.trim();
  const fullPrompt = `${handle} ${body}`;
  const snippet = promptSnippet(body);

  useEffect(() => {
    const controller = createPromptCopyController({
      writeText: (value) => {
        const copyPromise = navigator.clipboard?.writeText(value);
        return (
          copyPromise ?? Promise.reject(new Error("Clipboard API unavailable"))
        );
      },
      onStateChange: (state) => {
        setCopyState(state);
        onCopyStateChangeRef.current(prompt.id, state);
      },
    });
    copyController.current = controller;
    return () => {
      controller.dispose();
      copyController.current = null;
    };
  }, [prompt.id]);

  const copyButtonLabel =
    copyState === "copying"
      ? `Copying example prompt: ${snippet}`
      : copyState === "copied"
        ? `Copied example prompt: ${snippet}`
        : copyState === "error"
          ? `Retry copying example prompt: ${snippet}`
          : `Copy example prompt: ${snippet}`;
  const copyTooltip =
    copyState === "copying"
      ? "Copying example prompt"
      : copyState === "copied"
        ? "Copied example prompt"
        : copyState === "error"
          ? "Try copying again"
          : "Copy example prompt";

  return (
    <div
      data-testid={`app-prompt-card-${prompt.id}`}
      className="mx-auto flex w-fit max-w-2xl gap-3 rounded-2xl bg-card px-4 py-2.5 text-sm text-card-foreground shadow-lg sm:px-5"
    >
      <p className="min-w-0 text-pretty leading-relaxed">
        <span className="font-semibold">{handle}</span>
        {body ? ` ${body}` : null}
      </p>
      <div className="flex h-[1.625em] shrink-0 items-center self-start">
        <TooltipProvider delayDuration={0}>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                type="button"
                variant="ghost"
                size="icon-xs"
                onClick={() => void copyController.current?.copy(fullPrompt)}
                disabled={copyState === "copying"}
                aria-label={copyButtonLabel}
                className="text-muted-foreground"
              >
                {copyState === "copying" ? (
                  <SpinnerIcon className="motion-safe:animate-spin" />
                ) : copyState === "copied" ? (
                  <CheckIcon />
                ) : (
                  <CopyIcon />
                )}
              </Button>
            </TooltipTrigger>
            <TooltipContent>{copyTooltip}</TooltipContent>
          </Tooltip>
        </TooltipProvider>
      </div>
    </div>
  );
}

/**
 * ChatGPT plugin–style promo: themed radial stage with one or more stacked
 * prompt pills. Each pill copies its own `@App …` example.
 */
export default function AppPromptExamplePromo({
  displayName,
  prompts,
  className,
}: AppPromptExamplePromoProps) {
  const handle = `@${displayName.trim() || "App"}`;
  const [status, setStatus] = useState<{
    promptId: string | null;
    state: PromptCopyState;
  }>({ promptId: null, state: "idle" });

  if (prompts.length === 0) return null;

  const activePrompt = prompts.find((prompt) => prompt.id === status.promptId);
  const activeSnippet = activePrompt
    ? promptSnippet(activePrompt.text)
    : null;

  const copyMessage =
    status.state === "copying"
      ? activeSnippet
        ? `Copying “${activeSnippet}”…`
        : "Copying example prompt…"
      : status.state === "copied"
        ? activeSnippet
          ? `Copied “${activeSnippet}”. Paste it into your AI client.`
          : "Example prompt copied. Paste it into your AI client."
        : status.state === "error"
          ? activeSnippet
            ? `Couldn’t copy “${activeSnippet}”. Try again.`
            : "Couldn’t copy that example. Try again."
          : "Copy an example and paste it into your AI client.";

  return (
    <Card
      variant="solid"
      data-testid="app-prompt-example"
      className={cn(
        "rounded-2xl bg-promo-stage shadow-none",
        className,
      )}
    >
      <CardContent className="relative flex min-h-40 flex-col items-center justify-center gap-3 px-4 py-10 pb-14 sm:gap-4 sm:px-8 sm:py-12 sm:pb-16">
        {prompts.map((prompt) => (
          <PromptPill
            key={prompt.id}
            handle={handle}
            prompt={prompt}
            onCopyStateChange={(promptId, state) => {
              // Ignore stale idle resets from other pills so the shared live
              // region tracks the latest user interaction only.
              setStatus((current) => {
                if (
                  state === "idle" &&
                  current.promptId !== null &&
                  current.promptId !== promptId
                ) {
                  return current;
                }
                return { promptId, state };
              });
            }}
          />
        ))}
        <p
          role="status"
          aria-live="polite"
          aria-atomic="true"
          className={cn(
            "absolute inset-x-0 bottom-6 mx-auto min-h-5 max-w-2xl px-4 text-center text-xs sm:bottom-8",
            status.state === "error"
              ? "text-destructive"
              : "text-muted-foreground",
          )}
        >
          {copyMessage}
        </p>
      </CardContent>
    </Card>
  );
}
