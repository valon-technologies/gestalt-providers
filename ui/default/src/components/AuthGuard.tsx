"use client";

import { useEffect, useState } from "react";
import { isAuthenticated } from "@/lib/auth";
import { loginPathForCurrentLocation } from "@/lib/authReturn";

export default function AuthGuard({ children }: { children: React.ReactNode }) {
  const [checked, setChecked] = useState(false);
  const [authenticated, setAuthenticated] = useState(false);

  useEffect(() => {
    const authed = isAuthenticated();
    setAuthenticated(authed);
    setChecked(true);
    if (!authed) {
      window.location.replace(loginPathForCurrentLocation());
    }
  }, []);

  if (!checked || !authenticated) return null;

  return <>{children}</>;
}
