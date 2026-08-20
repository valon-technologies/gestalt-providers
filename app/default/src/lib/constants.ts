/** Canonical Setup journey. Welcome is the index (`/setup`). */
export const SETUP_PATH = "/setup" as const;
/** Legacy assistant-journey URL. Redirects onto {@link SETUP_PATH}. */
export const CONNECT_PATH = "/connect" as const;
/** @deprecated Use {@link SETUP_PATH}. Kept for `/build` redirects. */
export const BUILD_PATH = SETUP_PATH;
/** Workspace Admin — who can use which apps. */
export const ADMIN_PATH = "/admin";
export const DOCS_PATH = "/docs";
export const HTTP_UNAUTHORIZED = 401;
export const CONNECTION_RETURN_PATH_STORAGE_KEY =
  "gestalt.connection.returnPath";

export const INPUT_CLASSES =
  "rounded-sm border border-border bg-background px-3 py-2 text-sm text-foreground placeholder:text-muted-foreground/70 focus:border-ring focus:outline-hidden focus:ring-2 focus:ring-ring";
