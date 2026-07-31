import { useCallback, useSyncExternalStore } from "react";

const DEFAULT_INTERVAL_MS = 1_000;

let liveNowSnapshot = Date.now();
let liveNowInterval: number | undefined;
let liveNowSubscriberCount = 0;
const liveNowSubscribers = new Set<() => void>();

function onVisibilityChange() {
  if (document.visibilityState !== "hidden") {
    emitLiveNow();
  }
}

function emitLiveNow() {
  liveNowSnapshot = Date.now();
  for (const notify of liveNowSubscribers) {
    notify();
  }
}

function startLiveNowInterval() {
  if (liveNowInterval !== undefined) {
    return;
  }
  emitLiveNow();
  liveNowInterval = window.setInterval(() => {
    if (document.visibilityState !== "hidden") {
      emitLiveNow();
    }
  }, DEFAULT_INTERVAL_MS);
  document.addEventListener("visibilitychange", onVisibilityChange);
}

function stopLiveNowInterval() {
  if (liveNowInterval === undefined) {
    return;
  }
  window.clearInterval(liveNowInterval);
  liveNowInterval = undefined;
  document.removeEventListener("visibilitychange", onVisibilityChange);
}

function subscribeLiveNow(onStoreChange: () => void) {
  liveNowSubscribers.add(onStoreChange);
  liveNowSubscriberCount += 1;
  startLiveNowInterval();
  return () => {
    liveNowSubscribers.delete(onStoreChange);
    liveNowSubscriberCount -= 1;
    if (liveNowSubscriberCount === 0) {
      stopLiveNowInterval();
    }
  };
}

/** Ticks every second while enabled so in-flight publish labels update between registry polls. */
export function useLiveNow({
  enabled,
}: {
  enabled: boolean;
  /** Reserved for future use; only the default 1s cadence is supported today. */
  intervalMs?: number;
}): number {
  const subscribe = useCallback(
    (onStoreChange: () => void) =>
      enabled ? subscribeLiveNow(onStoreChange) : () => {},
    [enabled],
  );

  return useSyncExternalStore(
    subscribe,
    () => liveNowSnapshot,
    () => liveNowSnapshot,
  );
}
