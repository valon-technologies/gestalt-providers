import { getTokens, isAPIErrorStatus, type APIToken } from "@/lib/api";

/**
 * Personal token **listing** for the console.
 *
 * Some deployments expose create/revoke but not GET /api/v1/tokens (405).
 * Build authorize already treats an in-session plaintext secret as complete in
 * that case (`BuildWorkspaceSnapshot.apiToken`). Mapping 405 → [] keeps the
 * shared TanStack Query cache in a settled success state so authorize does not
 * spin on retries or block on a known product gap.
 *
 * Other errors still throw so Settings/Build can surface real failures.
 */
export async function fetchPersonalTokenList(): Promise<APIToken[]> {
  try {
    return await getTokens();
  } catch (error) {
    if (isAPIErrorStatus(error, 405)) {
      return [];
    }
    throw error;
  }
}
