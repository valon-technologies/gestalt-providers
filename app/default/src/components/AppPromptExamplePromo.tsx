import { useState } from "react";
import { CheckIcon, ChevronRightIcon } from "@/components/icons";
import { cn } from "@/lib/cn";

type AppPromptExamplePromoProps = {
  /** Catalog display name used as the `@…` handle. */
  displayName: string;
  /** Prompt body after the mention. */
  body: string;
  className?: string;
};

/**
 * ChatGPT plugin–style promo: Valon Peachy-Copper gradient stage + chat-input
 * pill with an example agent ask. Clicking copies the prompt (Gestalt has no
 * in-page chat).
 */
export default function AppPromptExamplePromo({
  displayName,
  body,
  className,
}: AppPromptExamplePromoProps) {
  const [copied, setCopied] = useState(false);
  const handle = `@${displayName.trim() || "App"}`;
  const prompt = `${handle} ${body.trim()}`;

  function handleCopy() {
    void navigator.clipboard.writeText(prompt).then(() => {
      setCopied(true);
      window.setTimeout(() => setCopied(false), 2000);
    });
  }

  return (
    <div
      data-testid="app-prompt-example"
      className={cn(
        "rounded-2xl px-4 py-10 sm:px-8 sm:py-12",
        // Valon Peachy-Copper — https://www.valon.ai/style
        "bg-[radial-gradient(140%_90%_at_50%_100%,#EACCB8_0%,#FDFCF9_50%,#F8F6F3_80%)]",
        // Dark: same radial geometry, deeper copper → ink surface.
        "dark:bg-[radial-gradient(140%_90%_at_50%_100%,oklch(0.48_0.06_50)_0%,oklch(0.28_0.02_70)_50%,oklch(0.22_0.02_60)_80%)]",
        className,
      )}
    >
      <button
        type="button"
        onClick={handleCopy}
        title={copied ? "Copied" : "Copy prompt"}
        aria-label={copied ? "Copied prompt" : "Copy example prompt"}
        className={cn(
          "mx-auto flex w-full max-w-2xl items-center gap-3 rounded-full",
          "bg-base-white px-5 py-3.5 text-left shadow-sm",
          "dark:bg-surface dark:shadow-none dark:ring-1 dark:ring-border",
          "transition-[background-color,box-shadow] duration-hover-out ease-out-quart",
          "hover:bg-neutral-hover hover:duration-hover-in",
          "focus-visible:outline-hidden focus-visible:ring-2 focus-visible:ring-gold-400 focus-visible:ring-offset-2 focus-visible:ring-offset-transparent",
        )}
      >
        <span className="min-w-0 flex-1 text-sm leading-snug text-foreground">
          <span className="font-semibold">{handle}</span>
          {body.trim() ? ` ${body.trim()}` : null}
        </span>
        <span
          className={cn(
            "flex size-8 shrink-0 items-center justify-center rounded-full",
            "border border-border bg-base-white text-foreground",
            "dark:bg-surface",
          )}
          aria-hidden
        >
          {copied ? (
            <CheckIcon className="size-4" />
          ) : (
            <ChevronRightIcon className="size-4" />
          )}
        </span>
      </button>
    </div>
  );
}
