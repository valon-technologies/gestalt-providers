
import { useEffect, useState } from "react";
import { getAuthSession } from "@/lib/api";
import { setCachedSession } from "@/lib/auth";
import { serverLoginURL } from "@/lib/authReturn";

export default function AuthGuard({ children }: { children: React.ReactNode }) {
  const [checked, setChecked] = useState(false);
  const [authenticated, setAuthenticated] = useState(false);

  useEffect(() => {
    getAuthSession()
      .then((session) => {
        setCachedSession(session);
        setAuthenticated(true);
        setChecked(true);
      })
      .catch(() => {
        // fetchAPI usually navigates to /api/v1/auth/login on 401; if that
        // did not happen (network error), do not sit on an empty #root.
        setAuthenticated(false);
        setChecked(true);
        window.location.href = serverLoginURL();
      });
  }, []);

  if (!checked || !authenticated) {
    return (
      <div className="flex min-h-screen items-center justify-center text-sm text-muted">
        Checking session…
      </div>
    );
  }

  return <>{children}</>;
}
