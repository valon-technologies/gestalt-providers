export const AGENT_SESSION_ROUTE = "/agents";
export const AGENT_SESSION_QUERY_PARAM = "session";
export const AGENT_TURN_QUERY_PARAM = "turn";
export const AGENT_PROVIDER_QUERY_PARAM = "provider";

export interface AgentSessionHrefOptions {
  sessionID?: string | null;
  turnID?: string | null;
  /**
   * Agent provider that owns the session. Session ids are provider-minted, so
   * deep links must carry the provider for the console to load the session.
   */
  provider?: string | null;
}

/** Builds the canonical relative UI href for an agent session or turn. */
export function agentSessionHref(
  sessionIDOrOptions?: string | AgentSessionHrefOptions | null,
  turnID?: string | null,
): string {
  const options =
    typeof sessionIDOrOptions === "object" && sessionIDOrOptions !== null
      ? sessionIDOrOptions
      : { sessionID: sessionIDOrOptions, turnID };
  const params = new URLSearchParams();
  if (options.sessionID) {
    params.set(AGENT_SESSION_QUERY_PARAM, options.sessionID);
  }
  if (options.turnID) {
    params.set(AGENT_TURN_QUERY_PARAM, options.turnID);
  }
  if (options.sessionID && options.provider) {
    params.set(AGENT_PROVIDER_QUERY_PARAM, options.provider);
  }
  const query = params.toString();
  return query ? `${AGENT_SESSION_ROUTE}?${query}` : AGENT_SESSION_ROUTE;
}
