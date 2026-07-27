import { useQuery } from "@tanstack/react-query";
import { getAuthInfo, getAuthSession } from "@/lib/api";
import { getCachedSession, setCachedSession } from "@/lib/auth";
import { queryKeys } from "@/lib/query-keys";

export function useAuthSessionQuery() {
  return useQuery({
    queryKey: queryKeys.auth.session(),
    queryFn: async () => {
      const session = await getAuthSession();
      setCachedSession(session);
      return session;
    },
    initialData: () => getCachedSession() ?? undefined,
    retry: false,
  });
}

export function useAuthInfoQuery(enabled = true) {
  return useQuery({
    queryKey: queryKeys.auth.info(),
    queryFn: getAuthInfo,
    enabled,
  });
}
