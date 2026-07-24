"use client";

import {
  type RefObject,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";

import {
  createFrameScheduler,
  pickActiveSection,
  type ScrollSpyEntry,
} from "@/lib/scroll-spy";

/**
 * Gestalt console vendor of Valon Registry `use-scroll-spy`.
 *
 * Ownership: Valon Registry
 * (`valon-tools/apps/registry/ui/src/hooks/use-scroll-spy.ts`).
 */

const DEFAULT_ACTIVATION_OFFSET = 80;
/** Fallback pin after TOC click when `scrollend` never fires. */
const DEFAULT_SCROLL_SUPPRESS_MS = 400;
/** Minimum pin while a smooth scroll may still be in flight. */
const MIN_CLICK_SUPPRESS_MS = 1000;

export type UseScrollSpyOptions = {
  /** Scrollport to observe. When null/unattached, activeId stays null. */
  scrollRootRef: RefObject<HTMLElement | null>;
  /**
   * Resolve current section geometry. Called on scroll / resize / when
   * `sectionsKey` changes. Return tops from `getBoundingClientRect()`.
   */
  getEntries: () => ScrollSpyEntry[];
  /** Rebind when section inventory or layout identity changes. */
  sectionsKey?: string | number;
  activationOffset?: number;
  forceLastAtBottom?: boolean;
  clearAboveFirst?: boolean;
  bottomThreshold?: number;
  enabled?: boolean;
  /** How long `activate` pins the selection after a click jump. */
  suppressMs?: number;
  /**
   * When true, also listen on `window` (document / viewport scroll). Needed
   * because scrolling the page does not reliably fire `scroll` on
   * `document.documentElement`.
   */
  observeWindow?: boolean;
};

export type UseScrollSpyResult = {
  activeId: string | null;
  /** Pin `id` as active and suppress spy updates briefly (TOC click). */
  activate: (id: string) => void;
  /** Recompute active id from current geometry (after content mutations). */
  recompute: () => void;
};

export function useScrollSpy({
  scrollRootRef,
  getEntries,
  sectionsKey = "",
  activationOffset = DEFAULT_ACTIVATION_OFFSET,
  forceLastAtBottom = true,
  clearAboveFirst = false,
  bottomThreshold = 1,
  enabled = true,
  suppressMs = DEFAULT_SCROLL_SUPPRESS_MS,
  observeWindow = false,
}: UseScrollSpyOptions): UseScrollSpyResult {
  const [activeId, setActiveId] = useState<string | null>(null);
  const suppressUntilRef = useRef(0);
  const activateGenRef = useRef(0);
  const getEntriesRef = useRef(getEntries);
  getEntriesRef.current = getEntries;

  const compute = useCallback(() => {
    if (!enabled) return;
    if (Date.now() < suppressUntilRef.current) return;

    const container = scrollRootRef.current;
    if (!container) {
      setActiveId(null);
      return;
    }

    const entries = getEntriesRef.current();
    // Document / body scroll: section tops from getBoundingClientRect() are
    // viewport-relative, so the activation line is `activationOffset` below the
    // viewport top (0). Using documentElement.getBoundingClientRect().top is
    // wrong — that value tracks -scrollY and shifts every section by scrollY.
    const containerTop =
      container === document.documentElement || container === document.body
        ? 0
        : container.getBoundingClientRect().top;
    const nextId = pickActiveSection(entries, containerTop, {
      activationOffset,
      forceLastAtBottom,
      clearAboveFirst,
      bottomThreshold,
      scrollTop:
        container === document.documentElement || container === document.body
          ? window.scrollY
          : container.scrollTop,
      scrollHeight: container.scrollHeight,
      clientHeight: container.clientHeight,
    });
    setActiveId(nextId);
  }, [
    activationOffset,
    bottomThreshold,
    clearAboveFirst,
    enabled,
    forceLastAtBottom,
    scrollRootRef,
  ]);

  useEffect(() => {
    suppressUntilRef.current = 0;
  }, [sectionsKey]);

  useEffect(() => {
    if (!enabled) {
      setActiveId(null);
      return;
    }

    // Effect owns the coalescer: cancel discards pending state with the effect.
    const frames = createFrameScheduler(compute);
    let container: HTMLElement | null = null;
    const onScroll = () => frames.schedule();

    const bind = () => {
      const next = scrollRootRef.current;
      if (next === container) {
        frames.schedule();
        return;
      }
      container?.removeEventListener("scroll", onScroll);
      container = next;
      container?.addEventListener("scroll", onScroll, { passive: true });
      frames.schedule();
    };

    bind();
    const bindFrame = requestAnimationFrame(bind);

    const resizeObserver = new ResizeObserver(() => bind());
    const root = scrollRootRef.current;
    if (root) resizeObserver.observe(root);

    if (observeWindow) {
      window.addEventListener("scroll", onScroll, { passive: true });
      window.addEventListener("resize", onScroll);
    }

    return () => {
      cancelAnimationFrame(bindFrame);
      container?.removeEventListener("scroll", onScroll);
      resizeObserver.disconnect();
      frames.cancel();
      if (observeWindow) {
        window.removeEventListener("scroll", onScroll);
        window.removeEventListener("resize", onScroll);
      }
    };
  }, [compute, enabled, observeWindow, scrollRootRef, sectionsKey]);

  const activate = useCallback(
    (id: string) => {
      const gen = ++activateGenRef.current;
      setActiveId(id);
      // Smooth scroll often outlives the default 400ms pin; keep the click
      // target selected until `scrollend` (or a floor timeout).
      const pinMs = Math.max(suppressMs, MIN_CLICK_SUPPRESS_MS);
      suppressUntilRef.current = Date.now() + pinMs;

      const finish = () => {
        if (activateGenRef.current !== gen) return;
        suppressUntilRef.current = 0;
        compute();
      };

      const scrollTarget: EventTarget = observeWindow
        ? window
        : (scrollRootRef.current ?? window);

      const onScrollEnd = () => {
        window.clearTimeout(fallbackId);
        finish();
      };

      scrollTarget.addEventListener("scrollend", onScrollEnd, {
        once: true,
      } as AddEventListenerOptions);
      const fallbackId = window.setTimeout(() => {
        scrollTarget.removeEventListener("scrollend", onScrollEnd);
        finish();
      }, pinMs);
    },
    [compute, observeWindow, scrollRootRef, suppressMs],
  );

  return { activeId, activate, recompute: compute };
}
