import { useQuery } from "@tanstack/react-query";
import {
  getAgentInteractions,
  getAgentProviders,
  getAgentSession,
  getAgentSessions,
  getAgentTurn,
  getAgentTurns,
  isAPIErrorStatus,
} from "@/lib/api";
import { queryKeys } from "@/lib/query-keys";

export function useAgentProvidersQuery() {
  return useQuery({
    queryKey: queryKeys.agents.providers(),
    queryFn: getAgentProviders,
  });
}

export function useAgentSessionsQuery(
  opts?: { view?: "full" | "summary"; limit?: number },
  enabled = true,
) {
  return useQuery({
    queryKey: queryKeys.agents.sessions(opts),
    queryFn: () => getAgentSessions(opts),
    enabled,
    retry: (failureCount, error) =>
      !isAPIErrorStatus(error, 412) && failureCount < 1,
  });
}

export function useAgentSessionQuery(
  sessionId: string | null,
  provider: string | null,
) {
  return useQuery({
    queryKey: queryKeys.agents.session(sessionId ?? "", provider ?? ""),
    queryFn: () => getAgentSession(sessionId!, provider!),
    enabled: !!sessionId && !!provider,
  });
}

export function useAgentTurnsQuery(
  sessionId: string | null,
  provider: string | null,
  status?: string,
) {
  return useQuery({
    queryKey: queryKeys.agents.turns(sessionId ?? "", provider ?? "", status),
    queryFn: () =>
      getAgentTurns(sessionId!, provider!, {
        status: status === "all" ? undefined : status,
        limit: 100,
      }),
    enabled: !!sessionId && !!provider,
  });
}

export function useAgentTurnQuery(
  turnId: string | null,
  provider: string | null,
) {
  return useQuery({
    queryKey: queryKeys.agents.turn(turnId ?? "", provider ?? ""),
    queryFn: () => getAgentTurn(turnId!, provider!),
    enabled: !!turnId && !!provider,
  });
}

export function useAgentInteractionsQuery(
  turnId: string | null,
  provider: string | null,
) {
  return useQuery({
    queryKey: queryKeys.agents.interactions(turnId ?? "", provider ?? ""),
    queryFn: () => getAgentInteractions(turnId!, provider!),
    enabled: !!turnId && !!provider,
  });
}
