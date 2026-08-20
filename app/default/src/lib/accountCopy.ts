/**
 * App connection family: wire this app into the workspace.
 *
 * Verbs are Connect and Disconnect, matching ChatGPT plugins and Claude
 * connectors. Setup (top nav) is a different job: wire an assistant in.
 * Do not call Gestalt apps Plugins or Connectors. Those names are what
 * ChatGPT and Claude call Gestalt itself.
 *
 * Account stays the noun for one named login in the list (work vs personal).
 */

export const CONNECTION_SURFACE_TITLE = "Connection" as const;

export const CONNECTION_NAV_LABEL = "Connection" as const;

export const APP_CONNECTED_LABEL = "Connected" as const;

export const APP_NOT_CONNECTED_LABEL = "Not connected" as const;

export const SIGN_IN_AGAIN_LABEL = "Sign in again" as const;

export const SIGNING_IN_LABEL = "Signing in..." as const;

export const NEEDS_SIGN_IN_LABEL = "Needs sign-in" as const;

export const OTHER_SIGN_IN_METHODS_LABEL = "Other sign-in methods" as const;

export const MANAGE_CONNECTION_LABEL = "Manage connection" as const;

export const IDENTITY_CONNECTED_LABEL = "Identity connected" as const;

export const IDENTITY_CONNECTION_REQUIRED_LABEL =
  "Identity connection required" as const;

export const SIGN_IN_WITH_OAUTH_LABEL = "Sign in with OAuth" as const;

export const CONNECTION_STATUS_UNAVAILABLE =
  "Couldn't load connection status. Try again." as const;

/** Human label when the API ships a `default` method/account slug. */
export const ACCOUNT_NAME_FALLBACK = "Account" as const;

/** First-time Connect CTA and dialog title: Connect GitHub, Connect Slack. */
export function connectAppActionLabel(displayName: string): string {
  return `Connect ${displayName}`;
}

/** Dialog title uses the same Connect {app} string as the CTA. */
export const connectAppDialogTitle = connectAppActionLabel;

export function signInAgainActionAriaLabel(displayName: string): string {
  return `${SIGN_IN_AGAIN_LABEL} for ${displayName}`;
}

export function appConnectedCopy(displayName: string): string {
  return `${displayName} is connected.`;
}

export function confirmAppConnectedFallback(displayName: string): string {
  return `Couldn't confirm ${displayName} is connected. Try again.`;
}

export function connectAppFailedCopy(displayName: string): string {
  return `Couldn't connect ${displayName}. Try again.`;
}

export function appDisconnectedCopy(displayName: string): string {
  return `${displayName} is disconnected.`;
}

export const APPS_CATALOG_DESCRIPTION =
  "Browse installed apps by category. Open a web app from its card when one is available." as const;

export const APPS_CONNECTED_BUCKET_DESCRIPTION =
  "Apps that are connected. Use Open app when available, or the card menu to manage the app." as const;

export const SIGN_IN_DETAILS_HEADING = "Sign-in details" as const;

export function connectionForAppAriaLabel(displayName: string): string {
  return `Connection for ${displayName}`;
}
