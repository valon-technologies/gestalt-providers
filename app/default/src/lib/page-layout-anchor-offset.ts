/**
 * Resolve `--page-layout-anchor-offset` to used pixels.
 *
 * The custom property is a `calc(...)` string; `parseFloat` cannot evaluate it —
 * probe a temporary element's computed height instead. One source of truth for
 * docs, catalog, and any future scroll-spy that must clear sticky page chrome.
 */

import { useLayoutEffect, useState } from "react";

/** Fallback before CSS vars resolve — chrome + Menu + gap ≈ 172px. */
export const PAGE_LAYOUT_ANCHOR_OFFSET_FALLBACK_PX = 172;

export function readPageLayoutAnchorOffsetPx(
  fallbackPx: number = PAGE_LAYOUT_ANCHOR_OFFSET_FALLBACK_PX,
): number {
  if (typeof document === "undefined") return fallbackPx;
  const probe = document.createElement("div");
  probe.setAttribute("aria-hidden", "true");
  probe.style.cssText =
    "position:absolute;visibility:hidden;pointer-events:none;height:var(--page-layout-anchor-offset)";
  document.documentElement.appendChild(probe);
  const px = Number.parseFloat(getComputedStyle(probe).height);
  probe.remove();
  return Number.isFinite(px) ? px : fallbackPx;
}

/**
 * Live `--page-layout-anchor-offset` for scroll-spy activation. Re-syncs on
 * resize and when measured app sticky chrome changes height.
 */
export function usePageLayoutAnchorOffsetPx(
  fallbackPx: number = PAGE_LAYOUT_ANCHOR_OFFSET_FALLBACK_PX,
): number {
  const [offset, setOffset] = useState(fallbackPx);

  useLayoutEffect(() => {
    const syncOffset = () => setOffset(readPageLayoutAnchorOffsetPx(fallbackPx));
    syncOffset();
    window.addEventListener("resize", syncOffset);
    const ro =
      typeof ResizeObserver !== "undefined"
        ? new ResizeObserver(syncOffset)
        : null;
    const chrome = document.querySelector("[data-slot='app-sticky-chrome']");
    if (chrome && ro) ro.observe(chrome);
    return () => {
      window.removeEventListener("resize", syncOffset);
      ro?.disconnect();
    };
  }, [fallbackPx]);

  return offset;
}
