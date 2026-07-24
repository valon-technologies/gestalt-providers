import { QueryClient } from "@tanstack/react-query";

/** Canonical server-state keys for the Gestalt console. */
export const queryKeys = {
  integrations: ["integrations"] as const,
  tokens: ["tokens"] as const,
  integrationOperations: (appName: string) =>
    ["integrations", appName, "operations"] as const,
};

/**
 * Shared QueryClient defaults: warm navigations show cached data immediately;
 * background refetch keeps connect/token state fresh without a loading flash.
 */
export function createQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: 60_000,
        gcTime: 30 * 60_000,
        refetchOnWindowFocus: true,
        retry: 1,
      },
    },
  });
}
