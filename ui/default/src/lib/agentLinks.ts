export function agentSessionHref(
  sessionID?: string | null,
  turnID?: string | null,
): string {
  const params = new URLSearchParams();
  if (sessionID) params.set("session", sessionID);
  if (turnID) params.set("turn", turnID);
  const query = params.toString();
  return query ? `/agents?${query}` : "/agents";
}
