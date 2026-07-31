export type PromptCopyState = "idle" | "copying" | "copied" | "error";

export const PROMPT_COPY_RESET_DELAY_MS = {
  copied: 2_000,
  error: 3_000,
} as const;

export type PromptCopyScheduler = {
  setTimeout(callback: () => void, delay: number): number;
  clearTimeout(handle: number): void;
};

type PromptCopyControllerOptions = {
  writeText(text: string): Promise<void>;
  onStateChange(state: PromptCopyState): void;
  scheduler?: PromptCopyScheduler;
};

export type PromptCopyController = {
  copy(prompt: string): Promise<void>;
  dispose(): void;
};

/**
 * Owns clipboard request ordering and reset timers independently of React.
 * A stale request can never overwrite a newer attempt, and disposal silences
 * all future state changes.
 */
export function createPromptCopyController({
  writeText,
  onStateChange,
  scheduler,
}: PromptCopyControllerOptions): PromptCopyController {
  const timers =
    scheduler ??
    ({
      setTimeout: (callback, delay) => window.setTimeout(callback, delay),
      clearTimeout: (handle) => window.clearTimeout(handle),
    } satisfies PromptCopyScheduler);
  let disposed = false;
  let requestGeneration = 0;
  let resetTimer: number | undefined;

  function clearResetTimer() {
    if (resetTimer === undefined) return;
    timers.clearTimeout(resetTimer);
    resetTimer = undefined;
  }

  function transition(state: PromptCopyState) {
    if (!disposed) onStateChange(state);
  }

  function scheduleReset(
    generation: number,
    state: keyof typeof PROMPT_COPY_RESET_DELAY_MS,
  ) {
    resetTimer = timers.setTimeout(() => {
      resetTimer = undefined;
      if (disposed || generation !== requestGeneration) return;
      transition("idle");
    }, PROMPT_COPY_RESET_DELAY_MS[state]);
  }

  return {
    async copy(prompt) {
      if (disposed) return;
      requestGeneration += 1;
      const generation = requestGeneration;
      clearResetTimer();
      transition("copying");

      try {
        await writeText(prompt);
        if (disposed || generation !== requestGeneration) return;
        transition("copied");
        scheduleReset(generation, "copied");
      } catch {
        if (disposed || generation !== requestGeneration) return;
        transition("error");
        scheduleReset(generation, "error");
      }
    },
    dispose() {
      if (disposed) return;
      disposed = true;
      requestGeneration += 1;
      clearResetTimer();
    },
  };
}
