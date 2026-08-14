import { type RefObject, useLayoutEffect, useState } from "react";

const STUCK_EPSILON_PX = 1;

/** True when a `position: sticky` node has docked at its computed `top`. */
export function isElementStuck(
  el: HTMLElement,
  epsilonPx = STUCK_EPSILON_PX,
): boolean {
  const stickyTop = Number.parseFloat(getComputedStyle(el).top);
  if (!Number.isFinite(stickyTop)) return false;
  return el.getBoundingClientRect().top <= stickyTop + epsilonPx;
}

export function useStickyStuck(
  ref: RefObject<HTMLElement | null>,
  enabled = true,
): boolean {
  const [stuck, setStuck] = useState(false);

  useLayoutEffect(() => {
    if (!enabled) {
      setStuck(false);
      return;
    }

    const el = ref.current;
    if (!el) return;

    let frame = 0;
    const update = () => {
      frame = 0;
      setStuck(isElementStuck(el));
    };
    const schedule = () => {
      if (frame) return;
      frame = requestAnimationFrame(update);
    };

    update();
    window.addEventListener("scroll", schedule, { passive: true });
    window.addEventListener("resize", schedule);
    const observer = new ResizeObserver(schedule);
    observer.observe(el);

    return () => {
      if (frame) cancelAnimationFrame(frame);
      window.removeEventListener("scroll", schedule);
      window.removeEventListener("resize", schedule);
      observer.disconnect();
    };
  }, [enabled, ref]);

  return enabled && stuck;
}
