import type { AppAdminIdentity } from "@/lib/api";
import { serviceAccountLocalId } from "./app-workspace-shared";

/**
 * User-facing copy for the App Admin service-accounts roster.
 * Wire route remains `/admin/agent-identities`; product language is "Service accounts".
 */
export const SERVICE_ACCOUNTS_COPY = {
  navLabel: "Service accounts",
  title: "Service accounts",
  /** Page blurb before the docs handoff link. */
  descriptionBeforeLink:
    "Automation accounts that can use this app (for scheduled jobs and bots). This list is view-only — see",
  /** Link label → `/docs/authorization#authz-service-accounts`. */
  docsLinkLabel: "How to create a service account",
  /** Page blurb after the docs handoff link. */
  descriptionAfterLink: "to add one.",
  empty: "No service accounts have access to this app yet.",
  forbidden: "You need admin access on this app to view this list.",
  loading: "Loading service accounts…",
  loadErrorFallback:
    "Couldn’t load service accounts. Check your connection and refresh the page.",
  sectionAriaLabel: "Service accounts",
  listTestId: "app-agent-identities-list",
} as const;

/** User-facing load error — never surface bare transport text alone. */
export function serviceAccountsLoadErrorMessage(error: unknown): string {
  const detail =
    error instanceof Error && error.message.trim()
      ? error.message.trim()
      : null;
  if (detail) {
    return `${detail} Try refreshing the page.`;
  }
  return SERVICE_ACCOUNTS_COPY.loadErrorFallback;
}

const ROLE_LABELS: Record<string, string> = {
  admin: "Admin",
  viewer: "Viewer",
  editor: "Editor",
};

export type AgentIdentityException = {
  /** Short badge label for the exception state */
  label: string;
  /** Plain-English explanation under the title */
  detail: string;
};

/**
 * Presentation row for one service-account grant.
 * API/wire fields stay on AppAdminIdentity; this type is what the roster may show.
 */
export type AgentIdentityRowView = {
  /** Stable key — full wire subject id */
  key: string;
  title: string;
  /** Local account id without `service_account:` prefix */
  accountId: string;
  /** Show accountId under the title only when it differs from title */
  showAccountId: boolean;
  roleLabel: string;
  /** Absent on healthy/effective grants — primary layer is name + role only */
  exception: AgentIdentityException | null;
};

function roleLabel(role: string | undefined): string {
  const value = role?.trim() || "viewer";
  return (
    ROLE_LABELS[value.toLowerCase()] ??
    value.charAt(0).toUpperCase() + value.slice(1)
  );
}

function exceptionFor(
  identity: AppAdminIdentity,
): AgentIdentityException | null {
  if (identity.effective !== false) return null;
  // Do not echo opaque wire `shadowedBy` prose (e.g. "static viewer grant").
  return {
    label: "Overridden",
    detail: "Not used — another grant takes priority",
  };
}

/** Project a wire identity into the user-facing roster row. */
export function toAgentIdentityRowView(
  identity: AppAdminIdentity,
  index: number,
): AgentIdentityRowView {
  const subjectId = identity.subjectId?.trim() || `identity-${index}`;
  const accountId = serviceAccountLocalId(subjectId);
  const title = identity.displayName?.trim() || accountId;
  return {
    key: `${subjectId}:${identity.role}:${identity.source}:${index}`,
    title,
    accountId,
    showAccountId: title !== accountId,
    roleLabel: roleLabel(identity.role),
    exception: exceptionFor(identity),
  };
}
