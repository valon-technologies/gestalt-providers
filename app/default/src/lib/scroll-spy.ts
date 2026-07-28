/**
 * Gestalt console vendor of Valon Registry `scroll-spy`.
 *
 * Ownership: Valon Registry (`valon-tools/apps/registry/ui/src/lib/scroll-spy.ts`).
 * Synced from toolshed origin/main — import-path adaptation only.
 */

/** A section candidate for scroll-spy activation (document/viewport tops). */
export type ScrollSpyEntry = {
  id: string;
  /** `getBoundingClientRect().top` of the section heading / anchor. */
  top: number;
};

/**
 * Coalesced rAF scheduler whose cancel fully resets the pending gate.
 *
 * Invariant: after `cancel()`, the next `schedule()` always enqueues a new
 * frame. Owning this in the scroll-binding effect (not a hook-lifetime ref)
 * prevents StrictMode / sectionsKey remounts from permanently no-op'ing spy
 * updates when cleanup cancels a frame but leaves a stale "pending" id.
 */
export type FrameScheduler = {
  schedule: () => void;
  cancel: () => void;
};

export type FrameSchedulerHost = {
  request: (cb: FrameRequestCallback) => number;
  cancel: (id: number) => void;
};

const defaultFrameHost: FrameSchedulerHost = {
  request: (cb) => requestAnimationFrame(cb),
  cancel: (id) => cancelAnimationFrame(id),
};

export function createFrameScheduler(
  run: () => void,
  host: FrameSchedulerHost = defaultFrameHost,
): FrameScheduler {
  let pendingId: number | null = null;
  return {
    schedule() {
      if (pendingId !== null) return;
      pendingId = host.request(() => {
        pendingId = null;
        run();
      });
    },
    cancel() {
      if (pendingId === null) return;
      host.cancel(pendingId);
      pendingId = null;
    },
  };
}

export type PickActiveSectionOptions = {
  /** Distance below the scroll-root top that acts as the activation line (px). */
  activationOffset: number;
  /**
   * When true (default), the last section wins once the scroll root is at (or
   * within `bottomThreshold` of) the bottom — so short trailing sections still
   * activate. Matches Bootstrap ScrollSpy's bottom contract.
   */
  forceLastAtBottom?: boolean;
  /**
   * When true, return null while the first section's top is still below the
   * activation line (scrolled above all content). Default false keeps the
   * first section selected (docs / editor outline default).
   */
  clearAboveFirst?: boolean;
  /** Pixels from the bottom that count as "at bottom". Default 1. */
  bottomThreshold?: number;
  scrollTop: number;
  scrollHeight: number;
  clientHeight: number;
};

/**
 * Pick the active section id from geometry.
 *
 * Contract (peer docs TOCs / Bootstrap ScrollSpy rewrite):
 * - Active = deepest section whose top has crossed the activation line.
 * - At bottom → last section (when `forceLastAtBottom`).
 * - Above first → null when `clearAboveFirst`, else first section.
 */
export function pickActiveSection(
  entries: ScrollSpyEntry[],
  containerTop: number,
  options: PickActiveSectionOptions,
): string | null {
  if (entries.length === 0) return null;

  const {
    activationOffset,
    forceLastAtBottom = true,
    clearAboveFirst = false,
    bottomThreshold = 1,
    scrollTop,
    scrollHeight,
    clientHeight,
  } = options;

  const maxScroll = Math.max(0, scrollHeight - clientHeight);
  if (forceLastAtBottom && maxScroll > 0 && scrollTop >= maxScroll - bottomThreshold) {
    return entries[entries.length - 1]!.id;
  }

  const first = entries[0]!;
  if (clearAboveFirst && first.top - containerTop > activationOffset) {
    return null;
  }

  let activeId = first.id;
  for (const entry of entries) {
    if (entry.top - containerTop <= activationOffset) activeId = entry.id;
    else break;
  }
  return activeId;
}
