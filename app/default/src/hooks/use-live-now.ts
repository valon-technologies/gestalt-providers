import { useEffect, useState } from "react";

/** Ticks every second while enabled so in-flight publish labels update between registry polls. */
export function useLiveNow({
  enabled,
  intervalMs = 1_000,
}: {
  enabled: boolean;
  intervalMs?: number;
}): number {
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    if (!enabled) {
      return undefined;
    }

    const tick = () => {
      if (document.visibilityState !== "hidden") {
        setNow(Date.now());
      }
    };

    tick();
    const timer = window.setInterval(tick, intervalMs);
    document.addEventListener("visibilitychange", tick);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", tick);
    };
  }, [enabled, intervalMs]);

  return now;
}
