<<<<<<< HEAD
import { useAuthSessionQuery } from "@/lib/queries";
=======

import { useEffect, useState } from "react";
import { getAuthSession, isAPIErrorStatus, redirectToLogin } from "@/lib/api";
import { HTTP_UNAUTHORIZED } from "@/lib/constants";
import { setCachedSession } from "@/lib/auth";
>>>>>>> 4837101c (Fix console home redirect tests and centralize login redirect on 401.)

export default function AuthGuard({ children }: { children: React.ReactNode }) {
  const { isPending, isError } = useAuthSessionQuery();

<<<<<<< HEAD
  if (isPending || isError) return null;
=======
  useEffect(() => {
    getAuthSession()
      .then((session) => {
        setCachedSession(session);
        setAuthenticated(true);
        setChecked(true);
      })
      .catch((error: unknown) => {
        if (isAPIErrorStatus(error, HTTP_UNAUTHORIZED)) {
          redirectToLogin();
          return;
        }
        setAuthenticated(false);
        setChecked(true);
      });
  }, []);

  if (!checked || !authenticated) return null;
>>>>>>> 4837101c (Fix console home redirect tests and centralize login redirect on 401.)

  return <>{children}</>;
}
