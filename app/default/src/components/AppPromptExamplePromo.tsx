import { useEffect, useRef, useState } from "react";
import { CheckIcon, ChevronRightIcon, SpinnerIcon } from "@/components/icons";
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
          : "";
  const copyButtonLabel =
    copyState === "copying"
      ? "Copying prompt"
      : copyState === "copied"
        ? "Copied prompt"
        : copyState === "error"
          ? "Retry copying example prompt"
          : "Copy example prompt";

  return (
    <Card
      variant="solid"
      data-testid="app-prompt-example"
      className={cn(
        "rounded-2xl bg-promo-stage shadow-none",
        className,
      )}
    >
      <CardContent className="px-4 py-10 sm:px-8 sm:py-12">
        <Button
          type="button"
          variant="outline"
          onClick={handleCopy}
          disabled={copyState === "copying"}
          title={copyButtonLabel}
          aria-label={copyButtonLabel}
          className={cn(
            "mx-auto h-auto w-full max-w-2xl justify-start gap-3 whitespace-normal rounded-full",
            "border-border bg-card px-5 py-3.5 text-left text-foreground",
            "hover:bg-neutral-hover",
          )}
        >
          <span className="min-w-0 flex-1 text-sm leading-snug">
            <span className="font-semibold">{handle}</span>
            {body.trim() ? ` ${body.trim()}` : null}
          </span>
          <span
            className={cn(
              "flex size-8 shrink-0 items-center justify-center rounded-full",
              "border border-border bg-card text-foreground",
            )}
            aria-hidden
          >
            {copyState === "copying" ? (
              <SpinnerIcon className="size-4 motion-safe:animate-spin" />
            ) : copyState === "copied" ? (
              <CheckIcon className="size-4" />
            ) : (
              <ChevronRightIcon className="size-4" />
            )}
          </span>
        </Button>
        <p
          role="status"
          aria-live="polite"
          aria-atomic="true"
          className={cn(
            "mx-auto mt-3 min-h-5 max-w-2xl text-center text-xs",
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
