import { getTokens, type APIToken } from "@/lib/api";

/**
 * Personal token listing for the console — Identity v2 grants
 * (`GET /api/v2/identity/grants`), not the removed `GET /api/v1/tokens`.
 */
export async function fetchPersonalTokenList(): Promise<APIToken[]> {
  return getTokens();
}
