export const LOGIN_PATH = "/login";
export const DOCS_PATH = "/docs";
export const HTTP_UNAUTHORIZED = 401;
export const DEFAULT_LOCAL_EMAIL = "anonymous@gestalt";
export const AUTH_RETURN_PATH_STORAGE_KEY = "gestalt.auth.returnPath";
export const CONNECTION_RETURN_PATH_STORAGE_KEY =
  "gestalt.connection.returnPath";

export const INPUT_CLASSES =
  "rounded-sm border border-alpha bg-base-white px-3 py-2 text-sm text-primary placeholder:text-faint focus:border-base-950 focus:outline-hidden focus:ring-2 focus:ring-base-950/10 dark:bg-surface dark:focus:border-base-200 dark:focus:ring-base-200/10";

export const SECONDS_PER_DAY = 24 * 60 * 60;

// TOKEN_TTL_NEVER is reserved for a future non-expiring option. It is not
// exposed in TOKEN_TTL_PRESETS yet; 0 currently maps to the provider default.
export const TOKEN_TTL_NEVER = 0;

export const TOKEN_TTL_PRESETS: { label: string; seconds: number | null }[] = [
  { label: "7 days", seconds: 7 * SECONDS_PER_DAY },
  { label: "30 days", seconds: 30 * SECONDS_PER_DAY },
  { label: "90 days", seconds: 90 * SECONDS_PER_DAY },
  { label: "1 year", seconds: 365 * SECONDS_PER_DAY },
  { label: "Default", seconds: null },
];

export const DEFAULT_TOKEN_TTL_PRESET_SECONDS = 30 * SECONDS_PER_DAY;
