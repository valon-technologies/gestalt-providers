
import { useEffect, useState } from "react";
import { getAuthSession } from "@/lib/api";
import { setCachedSession } from "@/lib/auth";

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
        setAuthenticated(false);
        setChecked(true);
      });
  }, []);

  if (!checked || !authenticated) return null;

  return <>{children}</>;
}
