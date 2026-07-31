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
import {
  createPromptCopyController,
  type PromptCopyController,
  type PromptCopyState,
} from "@/lib/promptCopy";

type AppPromptExamplePromoProps = {
  /** Catalog display name used as the `@…` handle. */
  displayName: string;
  /** Prompt body after the mention. */
  body: string;
  className?: string;
};

/**
 * ChatGPT plugin–style promo: themed radial stage + chat-input pill with an
 * example agent ask. Clicking copies the prompt (Gestalt has no in-page chat).
 */
export default function AppPromptExamplePromo({
  displayName,
  body,
  className,
}: AppPromptExamplePromoProps) {
  const [copyState, setCopyState] = useState<PromptCopyState>("idle");
  const copyController = useRef<PromptCopyController | null>(null);
  const handle = `@${displayName.trim() || "App"}`;
  const prompt = `${handle} ${body.trim()}`;

  useEffect(() => {
    const controller = createPromptCopyController({
      writeText: (value) => {
        const copyPromise = navigator.clipboard?.writeText(value);
        return copyPromise ?? Promise.reject(new Error("Clipboard API unavailable"));
      },
      onStateChange: setCopyState,
    });
    copyController.current = controller;
    return () => {
      controller.dispose();
      copyController.current = null;
    };
  }, []);

  function handleCopy() {
    void copyController.current?.copy(prompt);
  }

  const copyMessage =
    copyState === "copying"
      ? "Copying prompt…"
      : copyState === "copied"
        ? "Prompt copied. Paste it into your AI client."
        : copyState === "error"
          ? "Couldn’t copy the prompt. Try again."
          : "Copy this in your favorite LLM and try it.";
  const copyButtonLabel =
    copyState === "copying"
      ? "Copying prompt"
      : copyState === "copied"
        ? "Copied prompt"
        : copyState === "error"
          ? "Retry copying example prompt"
          : "Copy example prompt";
  const copyTooltip =
    copyState === "copying"
      ? "Copying prompt"
      : copyState === "copied"
        ? "Copied"
        : copyState === "error"
          ? "Try copying again"
          : "Copy prompt";

  return (
    <Card
      variant="solid"
      data-testid="app-prompt-example"
      className={cn(
        "rounded-2xl bg-promo-stage shadow-none",
        className,
      )}
    >
      <CardContent className="relative flex min-h-40 items-center justify-center px-4 py-10 pb-14 sm:px-8 sm:py-12 sm:pb-16">
        <div
          data-testid="app-prompt-card"
          className="mx-auto flex w-fit max-w-2xl gap-3 rounded-2xl bg-card px-4 py-2.5 text-sm shadow-lg sm:px-5"
        >
          <p className="min-w-0 text-pretty leading-relaxed text-foreground">
            <span className="font-semibold">{handle}</span>
            {body.trim() ? ` ${body.trim()}` : null}
          </p>
          <div className="flex h-[1.625em] shrink-0 items-center self-start">
            <TooltipProvider delayDuration={0}>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Button
                    type="button"
                    variant="ghost"
                    size="icon-xs"
                    onClick={handleCopy}
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
        <p
          role="status"
          aria-live="polite"
          aria-atomic="true"
          className={cn(
            "absolute inset-x-0 bottom-6 mx-auto min-h-5 max-w-2xl px-4 text-center text-xs sm:bottom-8",
            copyState === "error"
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
