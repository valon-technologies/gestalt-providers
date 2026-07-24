"use client";


/**
 * Gestalt console vendor of Valon Registry `agent-console`.
 *
 * Ownership: Valon Registry is canonical
 * (`valon-tools/apps/registry/ui/src/ui/agent-console.tsx`).
 * Synced from toolshed origin/main — token adaptation only (`@/lib/cn` path).
 * Do not restyle chrome at call sites; change Registry first.
 */

import * as React from "react";

import { cn } from "@/lib/cn";

// Display-only marketing agent console modeled on Val Town’s Claude Code hero:
// window chrome (traffic lights + title) + a single monospace terminal body.
// Identity, prompt, and “? for shortcuts” are terminal text — not card chrome.
// Not a PTY, not CodeBlock, not an ANSI tool-output Terminal.
//
// Skin colors flow via CSS vars on the root (`--agent-console-*`).

const REDUCED_MOTION_QUERY = "(prefers-reduced-motion: reduce)";

function usePrefersReducedMotion(): boolean {
  const [reduced, setReduced] = React.useState(false);

  React.useEffect(() => {
    if (typeof window === "undefined" || typeof window.matchMedia !== "function") {
      return;
    }
    const media = window.matchMedia(REDUCED_MOTION_QUERY);
    const sync = () => setReduced(media.matches);
    sync();
    media.addEventListener("change", sync);
    return () => media.removeEventListener("change", sync);
  }, []);

  return reduced;
}

/** Claude Code / Val Town palette — default skin. */
export const AGENT_CONSOLE_THEME_CLAUDE = {
  background: "#4b3d35",
  accent: "#ff7250",
  traffic: "#695e57",
  foreground: "#ffffff",
  muted: "rgba(255,255,255,0.4)",
  glyph: "rgba(255,255,255,0.5)",
} as const;

export type AgentConsoleTheme = {
  background: string;
  accent: string;
  traffic: string;
  foreground: string;
  muted: string;
  glyph: string;
};

/** Shared wrap recipe for measure + typed line — must stay identical. */
const AGENT_CONSOLE_PROMPT_WRAP =
  "break-words whitespace-pre-wrap [overflow-wrap:anywhere]";

/** Matches `AgentConsoleCursor` advance so measure height includes the caret. */
const AGENT_CONSOLE_CURSOR_ADVANCE = "inline-block h-[1.1em] w-[1ch] align-baseline";

interface AgentConsoleProps extends React.ComponentProps<"div"> {
  /** Full palette. Defaults to Claude Code / Val Town. */
  theme?: Partial<AgentConsoleTheme>;
  /** Shorthand — sets accent (and divider / cursor). Prefer `theme` for full skins. */
  accent?: string;
}

function AgentConsole({
  className,
  style,
  theme,
  accent,
  ...props
}: AgentConsoleProps) {
  const resolved: AgentConsoleTheme = {
    ...AGENT_CONSOLE_THEME_CLAUDE,
    ...theme,
    ...(accent ? { accent } : null),
  };

  return (
    <div
      data-slot="agent-console"
      role="img"
      aria-label="Agent console demonstration"
      className={cn(
        // Definite preferred width so typed text wraps instead of growing the box.
        "flex w-[32rem] max-w-full flex-col overflow-hidden rounded-xl text-[var(--agent-console-fg)] ring-1 ring-black/10",
        className,
      )}
      style={
        {
          ...style,
          backgroundColor: resolved.background,
          ["--agent-console-bg" as string]: resolved.background,
          ["--agent-console-accent" as string]: resolved.accent,
          ["--agent-console-traffic" as string]: resolved.traffic,
          ["--agent-console-fg" as string]: resolved.foreground,
          ["--agent-console-muted" as string]: resolved.muted,
          ["--agent-console-glyph" as string]: resolved.glyph,
        } as React.CSSProperties
      }
      {...props}
    />
  );
}

/** Title-bar chrome only — traffic lights + centered window title. */
function AgentConsoleChrome({ className, children, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="agent-console-chrome"
      className={cn("relative flex items-center gap-2 px-4 pb-1 pt-4", className)}
      {...props}
    >
      {children}
    </div>
  );
}

function AgentConsoleTrafficLights({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="agent-console-traffic-lights"
      aria-hidden
      className={cn("flex items-center gap-2", className)}
      {...props}
    >
      <span className="size-3 rounded-full bg-[var(--agent-console-traffic)]" />
      <span className="size-3 rounded-full bg-[var(--agent-console-traffic)]" />
      <span className="size-3 rounded-full bg-[var(--agent-console-traffic)]" />
    </div>
  );
}

function AgentConsoleWindowTitle({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="agent-console-window-title"
      className={cn(
        "pointer-events-none absolute inset-x-0 text-center text-xs text-[var(--agent-console-muted)]",
        className,
      )}
      {...props}
    />
  );
}

/** Monospace terminal body — identity, prompt, and hint live here. */
function AgentConsoleBody({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="agent-console-body"
      className={cn(
        "flex flex-col gap-3 p-4 font-mono font-normal text-xs sm:p-6 sm:text-sm",
        className,
      )}
      {...props}
    />
  );
}

/** Agent lockup as terminal text (icon + name / model / cwd) — not window chrome. */
function AgentConsoleIdentity({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="agent-console-identity"
      className={cn("flex min-w-0 items-center gap-5 text-[var(--agent-console-fg)]", className)}
      {...props}
    />
  );
}

function AgentConsoleMedia({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="agent-console-media"
      className={cn(
        "flex size-16 shrink-0 items-center justify-center text-[var(--agent-console-accent)]",
        className,
      )}
      {...props}
    />
  );
}

function AgentConsoleHeading({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div data-slot="agent-console-heading" className={cn("min-w-0 flex-1", className)} {...props} />
  );
}

function AgentConsoleProduct({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <p
      data-slot="agent-console-product"
      className={cn("font-medium text-[var(--agent-console-fg)]", className)}
      {...props}
    />
  );
}

function AgentConsoleSubtitle({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <p
      data-slot="agent-console-subtitle"
      className={cn("text-[var(--agent-console-fg)]", className)}
      {...props}
    />
  );
}

function AgentConsolePath({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <p
      data-slot="agent-console-path"
      className={cn("text-[var(--agent-console-fg)]", className)}
      {...props}
    />
  );
}

/**
 * Prompt row with accent hairlines (Val Town `border-y border-accent/50`).
 * Children: Glyph + Input (Typing + Cursor).
 */
function AgentConsolePrompt({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="agent-console-prompt"
      className={cn(
        "flex min-w-0 items-start gap-2 border-y border-[color-mix(in_oklab,var(--agent-console-accent)_50%,transparent)] py-2.5",
        className,
      )}
      {...props}
    />
  );
}

/**
 * Wrapping line for typed text + block cursor.
 * Reserves full-prompt height (invisible measure) so the box doesn’t jump while typing.
 * Measure and visible layers share `AGENT_CONSOLE_PROMPT_WRAP`; measure also reserves
 * the cursor’s 1ch advance so wrap boundaries match typed text + caret.
 */
function AgentConsoleInput({
  className,
  children,
  measureText,
  ...props
}: React.ComponentProps<"div"> & {
  /** Full prompt used to reserve height while characters type in. */
  measureText?: string;
}) {
  return (
    <div
      data-slot="agent-console-input"
      className={cn("relative block min-w-0 flex-1 basis-0", className)}
      {...props}
    >
      {measureText ? (
        <span aria-hidden className={cn("invisible", AGENT_CONSOLE_PROMPT_WRAP)}>
          {measureText}
          <span className={AGENT_CONSOLE_CURSOR_ADVANCE} />
        </span>
      ) : null}
      <span
        className={cn(AGENT_CONSOLE_PROMPT_WRAP, measureText ? "absolute inset-0" : null)}
      >
        {children}
      </span>
    </div>
  );
}

function AgentConsoleGlyph({
  className,
  children = "❯",
  ...props
}: React.ComponentProps<"span">) {
  return (
    <span
      data-slot="agent-console-glyph"
      aria-hidden
      className={cn("shrink-0 select-none text-[var(--agent-console-glyph)]", className)}
      {...props}
    >
      {children}
    </span>
  );
}

interface AgentConsoleTypingProps extends Omit<React.ComponentProps<"span">, "children"> {
  text: string;
  /** Milliseconds per character. Ignored when reduced motion is preferred. */
  durationMs?: number;
  /** Delay before typing starts (ms). */
  delayMs?: number;
  /** Called once the full string has been revealed. */
  onComplete?: () => void;
}

function AgentConsoleTyping({
  className,
  text,
  durationMs = 45,
  delayMs = 200,
  onComplete,
  ...props
}: AgentConsoleTypingProps) {
  const reducedMotion = usePrefersReducedMotion();
  const [displayed, setDisplayed] = React.useState(reducedMotion ? text : "");
  const onCompleteRef = React.useRef(onComplete);
  React.useEffect(() => {
    onCompleteRef.current = onComplete;
  }, [onComplete]);

  React.useEffect(() => {
    if (reducedMotion) {
      setDisplayed(text);
      onCompleteRef.current?.();
      return;
    }

    setDisplayed("");
    let index = 0;
    let intervalId: ReturnType<typeof setInterval> | null = null;
    const startId = setTimeout(() => {
      intervalId = setInterval(() => {
        index += 1;
        setDisplayed(text.slice(0, index));
        if (index >= text.length) {
          if (intervalId) clearInterval(intervalId);
          onCompleteRef.current?.();
        }
      }, durationMs);
    }, delayMs);

    return () => {
      clearTimeout(startId);
      if (intervalId) clearInterval(intervalId);
    };
  }, [text, durationMs, delayMs, reducedMotion]);

  return (
    <span data-slot="agent-console-typing" className={cn(className)} {...props}>
      <span className="sr-only">{text}</span>
      <span aria-hidden>{displayed}</span>
    </span>
  );
}

function AgentConsoleCursor({ className, ...props }: React.ComponentProps<"span">) {
  return (
    <span
      data-slot="agent-console-cursor"
      aria-hidden
      className={cn(
        AGENT_CONSOLE_CURSOR_ADVANCE,
        "ml-px translate-y-[0.2em] bg-[var(--agent-console-accent)] motion-safe:animate-pulse",
        className,
      )}
      {...props}
    />
  );
}

/** Dim terminal line (e.g. “? for shortcuts”) — not a window footer bar. */
function AgentConsoleHint({ className, ...props }: React.ComponentProps<"p">) {
  return (
    <p
      data-slot="agent-console-hint"
      className={cn("text-[var(--agent-console-muted)]", className)}
      {...props}
    />
  );
}

/** Optional bordered terminal panel (Codex status box). */
function AgentConsolePanel({ className, ...props }: React.ComponentProps<"div">) {
  return (
    <div
      data-slot="agent-console-panel"
      className={cn(
        "rounded-sm border border-[color-mix(in_oklab,var(--agent-console-fg)_22%,transparent)] px-3 py-2 text-[var(--agent-console-fg)]",
        className,
      )}
      {...props}
    />
  );
}

export {
  AgentConsole,
  AgentConsoleChrome,
  AgentConsoleTrafficLights,
  AgentConsoleWindowTitle,
  AgentConsoleBody,
  AgentConsoleIdentity,
  AgentConsoleMedia,
  AgentConsoleHeading,
  AgentConsoleProduct,
  AgentConsoleSubtitle,
  AgentConsolePath,
  AgentConsolePrompt,
  AgentConsoleInput,
  AgentConsoleGlyph,
  AgentConsoleTyping,
  AgentConsoleCursor,
  AgentConsoleHint,
  AgentConsolePanel,
};
