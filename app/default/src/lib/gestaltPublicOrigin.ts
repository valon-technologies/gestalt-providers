/** Placeholder when the running origin is local or unknown. */
export const GESTALT_PUBLIC_ORIGIN_PLACEHOLDER = "https://your-gestalt-host";

/**
 * Public origin for MCP URLs shown to users.
 *
 * Prefer the deployment env, then the browser origin, except localhost —
 * local Vite must not tell people to paste 127.0.0.1 into an assistant.
 */
export function resolveGestaltPublicOrigin(): string {
  const configured = import.meta.env.VITE_GESTALT_PUBLIC_ORIGIN?.trim();
  if (configured) return configured.replace(/\/$/, "");
  if (typeof window === "undefined") return GESTALT_PUBLIC_ORIGIN_PLACEHOLDER;
  const { origin, hostname } = window.location;
  if (hostname === "localhost" || hostname === "127.0.0.1") {
    return GESTALT_PUBLIC_ORIGIN_PLACEHOLDER;
  }
  return origin;
}
