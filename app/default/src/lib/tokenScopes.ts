/**
 * Encodes Create Token UI state into the personal-token `scopes` string.
 *
 * Grammar (authorization/indexeddb/scope.go):
 * - empty → full identity (all apps)
 * - `app` → all operations for that app
 * - `app:operation` → one operation
 * - space-separated for multiples
 *
 * UI owns structured selection; this is the sole encode chokepoint before createToken.
 */

export type TokenScopeMode = "all" | "select";

/** Per selected app: which operations are granted. */
export type AppOperationSelection =
  | { kind: "all" }
  | { kind: "ops"; operationIds: readonly string[] };

export type AppScopeSelection = {
  appId: string;
  operations: AppOperationSelection;
};

/**
 * Encode structured app/operation selections into the API scopes string.
 * Returns "" for full-identity (all apps).
 */
export function encodeTokenScopes(
  mode: TokenScopeMode,
  selections: readonly AppScopeSelection[],
): string {
  if (mode === "all") {
    return "";
  }

  const tokens: string[] = [];
  for (const selection of selections) {
    const appId = selection.appId.trim();
    if (!appId) continue;

    if (
      selection.operations.kind === "all" ||
      selection.operations.operationIds.length === 0
    ) {
      tokens.push(appId);
      continue;
    }

    for (const opId of selection.operations.operationIds) {
      const op = opId.trim();
      if (!op) continue;
      tokens.push(`${appId}:${op}`);
    }
  }

  return tokens.join(" ");
}

export function hasEffectiveScopes(
  mode: TokenScopeMode,
  selections: readonly AppScopeSelection[],
): boolean {
  if (mode === "all") return true;
  return encodeTokenScopes(mode, selections).length > 0;
}

const DAY_SECONDS = 24 * 60 * 60;

export type ExpirationChoice =
  | { kind: "days"; days: 7 | 30 | 60 | 90 }
  | { kind: "custom"; date: string }
  | { kind: "none" };

export function formatExpirationDayLabel(days: number, from: Date = new Date()): string {
  const end = new Date(from);
  end.setDate(end.getDate() + days);
  const dateLabel = end.toLocaleDateString(undefined, {
    weekday: "short",
    month: "short",
    day: "numeric",
    year: "numeric",
  });
  return `${days} days (${dateLabel})`;
}

/**
 * Seconds until end of the chosen local calendar day (inclusive), or undefined
 * when the token should not expire.
 */
export function expiresInFromChoice(
  choice: ExpirationChoice,
  now: Date = new Date(),
): number | undefined {
  if (choice.kind === "none") {
    return undefined;
  }

  if (choice.kind === "days") {
    return choice.days * DAY_SECONDS;
  }

  const trimmed = choice.date.trim();
  if (!trimmed) {
    return undefined;
  }

  // Interpret YYYY-MM-DD as local calendar day; expire at end of that day.
  const [y, m, d] = trimmed.split("-").map((part) => Number(part));
  if (!y || !m || !d) {
    return undefined;
  }
  const endOfDay = new Date(y, m - 1, d, 23, 59, 59, 999);
  const seconds = Math.floor((endOfDay.getTime() - now.getTime()) / 1000);
  return seconds > 0 ? seconds : undefined;
}
