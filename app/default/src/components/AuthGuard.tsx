import { useEffect, useState } from "react";
import { getAuthSession } from "@/lib/api";
import {
  clearSession,
  getCachedSession,
  setCachedSession,
} from "@/lib/auth";
import { serverLoginURL } from "@/lib/authReturn";

/**
 * App-shell session gate. Owns authentication for the whole console once —
 * mount at the root layout, not per page.
 *
 * Cached session is provisional identity: if localStorage already has a
 * subject, render children immediately and revalidate in the background.
 * That keeps chrome stable across client navigations and avoids a blank
 * full-screen flash on every route change.
 */
export default function AuthGuard({ children }: { children: React.ReactNode }) {
  const [gate, setGate] = useState(() => {
    const hasCache = !!getCachedSession();
    return { checked: hasCache, authenticated: hasCache };
  });

  useEffect(() => {
    getAuthSession()
      .then((session) => {
        setCachedSession(session);
        setGate({ checked: true, authenticated: true });
      })
      .catch(() => {
        // fetchAPI usually navigates to /api/v1/auth/login on 401; if that
        // did not happen (network error / invalid cache), do not sit on an
        // empty shell.
        clearSession();
        setGate({ checked: true, authenticated: false });
        window.location.href = serverLoginURL();
      });
  }, []);

  if (!gate.checked || !gate.authenticated) {
    return (
      <div className="flex min-h-screen items-center justify-center text-sm text-muted-foreground">
        Checking session…
      </div>
    );
  }

  return <>{children}</>;
}
