import { type RefObject, useLayoutEffect } from "react";

const STICKY_CHROME_HEIGHT_VAR = "--app-sticky-chrome-height";

/**
 * Publishes the sticky app chrome stack height (banner + top bar) as
 * `--app-sticky-chrome-height` so `--page-layout-mobile-nav-top` (flush Menu)
 * and `--page-layout-pane-top` (rails = chrome + gap) stay correct whether or
 * not DevWorktreeBanner is mounted.
 */
export function useSyncStickyAppChromeHeight(
  chromeRef: RefObject<HTMLElement | null>,
): void {
  useLayoutEffect(() => {
    const el = chromeRef.current;
    if (!el) return;

    const publish = () => {
      const height = el.getBoundingClientRect().height;
      document.documentElement.style.setProperty(
        STICKY_CHROME_HEIGHT_VAR,
        `${height}px`,
      );
    };

    publish();

    if (typeof ResizeObserver === "undefined") {
      window.addEventListener("resize", publish);
      return () => {
        window.removeEventListener("resize", publish);
        document.documentElement.style.removeProperty(STICKY_CHROME_HEIGHT_VAR);
      };
    }

    const ro = new ResizeObserver(publish);
    ro.observe(el);
    return () => {
      ro.disconnect();
      document.documentElement.style.removeProperty(STICKY_CHROME_HEIGHT_VAR);
    };
  }, [chromeRef]);
}
