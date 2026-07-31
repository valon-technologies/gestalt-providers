import { useEffect, useRef, useState } from "react";
import { CheckIcon, CopyIcon, SpinnerIcon } from "@/components/icons";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
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
  const copyButtonText =
    copyState === "copying"
      ? "Copying…"
      : copyState === "copied"
        ? "Copied"
        : copyState === "error"
          ? "Try again"
          : "Copy";

  return (
    <Card
      variant="solid"
      data-testid="app-prompt-example"
      className={cn(
        "rounded-2xl bg-promo-stage shadow-none",
        className,
      )}
    >
      <CardContent className="flex flex-col items-center px-4 py-10 sm:px-8 sm:py-12">
        <div
          data-testid="app-prompt-card"
          className="mx-auto flex w-full max-w-2xl flex-col items-center gap-5 rounded-2xl bg-card px-5 py-6 text-center shadow-lg sm:px-8"
        >
          <p className="max-w-xl text-sm leading-relaxed text-foreground">
            <span className="font-semibold">{handle}</span>
            {body.trim() ? ` ${body.trim()}` : null}
          </p>
          <Button
            type="button"
            variant="secondary"
            size="sm"
            onClick={handleCopy}
            disabled={copyState === "copying"}
            title={copyButtonLabel}
            aria-label={copyButtonLabel}
          >
            {copyState === "copying" ? (
              <SpinnerIcon className="size-4 motion-safe:animate-spin" />
            ) : copyState === "copied" ? (
              <CheckIcon className="size-4" />
            ) : (
              <CopyIcon className="size-4" />
            )}
            {copyButtonText}
          </Button>
        </div>
        <p
          role="status"
          aria-live="polite"
          aria-atomic="true"
          className={cn(
            "mt-3 min-h-5 max-w-2xl text-center text-xs",
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
