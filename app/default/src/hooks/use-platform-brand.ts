import { useSyncExternalStore } from "react";

import {
  defaultPlatformBrand,
  fetchPlatformBrand,
  getPlatformBrand,
  type PlatformBrand,
} from "@/lib/platform-brand";

type Listener = () => void;

let snapshot: PlatformBrand = defaultPlatformBrand();
let hydrated = false;
const listeners = new Set<Listener>();

function emit() {
  for (const listener of listeners) listener();
}

function subscribe(listener: Listener): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

function getSnapshot(): PlatformBrand {
  return snapshot;
}

function getServerSnapshot(): PlatformBrand {
  return defaultPlatformBrand();
}

function ensureHydrated() {
  if (hydrated) return;
  hydrated = true;
  snapshot = getPlatformBrand();
  void fetchPlatformBrand().then((brand) => {
    if (
      brand.name === snapshot.name &&
      brand.markSrc === snapshot.markSrc
    ) {
      return;
    }
    snapshot = brand;
    emit();
  });
}

/** Live platform brand for chrome (top bar, document title suffix). */
export function usePlatformBrand(): PlatformBrand {
  ensureHydrated();
  return useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);
}
