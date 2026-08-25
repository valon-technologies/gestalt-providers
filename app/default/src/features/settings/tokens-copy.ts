/**
 * Settings API-tokens surface copy — inventory vs create task.
 * Build / tutorial framing does not live here; pass Build-specific strings
 * into TokenCreateForm from the Build page when needed.
 */

import { MCP_SETUP_DOCS_LINK_LABEL } from "@/lib/assistantConnectionCopy";

export const SETTINGS_TOKENS_LIST_TITLE = "API tokens";

export const SETTINGS_TOKENS_LIST_DESCRIPTION =
  "Personal tokens for scripts, local tooling, and integrations. They authenticate as you.";

export const SETTINGS_TOKENS_CREATE_CTA = "Create token";

export const SETTINGS_TOKENS_EMPTY_TITLE = "No API tokens yet.";

export const SETTINGS_TOKENS_EMPTY_DESCRIPTION =
  "Use Create token for scripts, MCP clients, and other tools.";

/** Inventory table: grant has no stored display name. */
export const SETTINGS_TOKENS_UNNAMED_LABEL = "No name";

/** Inventory table: token is not scoped to specific apps. */
export const SETTINGS_TOKENS_SCOPES_ALL_LABEL = "all";

export const SETTINGS_TOKENS_SCOPES_SHOW_LESS = "Show less";

export function settingsTokensScopesMoreLabel(hiddenCount: number): string {
  return hiddenCount === 1
    ? "Show 1 more scope"
    : `Show ${hiddenCount} more scopes`;
}

/** Inventory table: token does not expire. */
export const SETTINGS_TOKENS_EXPIRES_NEVER_LABEL = "Never";

export const SETTINGS_TOKEN_CREATE_TITLE = "Create token";

export const SETTINGS_TOKEN_CREATE_DESCRIPTION =
  "Name the token, choose an expiration, and limit which apps it can access.";

export const SETTINGS_TOKEN_CREATED_TITLE = "Token created";

export const SETTINGS_TOKEN_CREATED_DESCRIPTION =
  "Your token is ready. Return to your tokens or choose your assistant in MCP Clients.";

/** One-time secret alert after mint — settings-native, not Build tutorial. */
export const SETTINGS_TOKEN_PLAINTEXT_DESCRIPTION =
  "Copy this token now. We won't show the full value again.";

export const SETTINGS_TOKEN_CREATE_CANCEL = "Cancel";

export const SETTINGS_TOKEN_CREATE_CONTINUE = MCP_SETUP_DOCS_LINK_LABEL;

export const SETTINGS_TOKEN_CREATE_DONE = "Back to tokens";

export const SETTINGS_TOKENS_DOCUMENT_TITLE = "API tokens";

export const SETTINGS_TOKEN_CREATE_DOCUMENT_TITLE = "Create token";
