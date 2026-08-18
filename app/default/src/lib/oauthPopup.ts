const OAUTH_POPUP_NAME = "gestalt-oauth";
const OAUTH_POPUP_FEATURES = "popup=yes,width=560,height=720";
const OAUTH_POPUP_POLL_MS = 400;

export type OAuthPopup = Pick<Window, "closed" | "close"> & {
  location: Pick<Location, "assign">;
};

/**
 * Google always returns to the API host (valon.tools). That callback page
 * closes itself — but only if this tab opened it with window.open. Navigating
 * this tab away replaces Setup with the callback, and close() is a no-op.
 */
export function openOAuthPopup(): OAuthPopup | null {
  if (typeof window === "undefined") return null;
  return window.open("about:blank", OAUTH_POPUP_NAME, OAUTH_POPUP_FEATURES);
}

export function navigateOAuthPopup(popup: OAuthPopup, url: string): void {
  popup.location.assign(url);
}

export function closeOAuthPopup(popup: OAuthPopup | null): void {
  if (!popup || popup.closed) return;
  popup.close();
}

/** Resolves when the sign-in window closes, then the opener can refresh. */
export function watchOAuthPopup(
  popup: OAuthPopup,
  onReturned: () => void,
): () => void {
  let finished = false;
  let sawOpen = false;

  const finish = () => {
    if (finished) return;
    finished = true;
    stop();
    onReturned();
  };

  const poll = window.setInterval(() => {
    if (!popup.closed) {
      sawOpen = true;
      return;
    }
    if (sawOpen) finish();
  }, OAUTH_POPUP_POLL_MS);

  const onFocus = () => {
    if (popup.closed) finish();
  };
  window.addEventListener("focus", onFocus);

  function stop() {
    window.clearInterval(poll);
    window.removeEventListener("focus", onFocus);
  }

  return stop;
}
