import { APIError } from "@/lib/api";
import { WorkflowProviderConfigurationError } from "@/lib/workflowProvider";

export const APPS_CATALOG_UNAVAILABLE = "Couldn't load apps. Try again.";
export const TOKENS_UNAVAILABLE = "Couldn't load tokens. Try again.";

const WORKFLOW_PROVIDER_UNAVAILABLE =
  /workflow provider .+ is not available/i;
const WORKFLOW_CANCEL_ALREADY_STARTED =
  /cannot be canceled once it has started/i;

/**
 * Product action that failed — owns how HTTP status maps to UI copy.
 * Generic 404 resource jargon must not override disconnect/connect meaning.
 */
export type UserFacingErrorKind =
  | "generic"
  | "disconnect"
  | "connect"
  | "select_instance"
  | "sign_in";

export type UserFacingErrorOptions = {
  /** Narrow copy for a specific mutating action when status codes are overloaded. */
  action?: "cancel";
  /** Connection/account flows that share status codes with different meaning. */
  kind?: UserFacingErrorKind;
};

function resolveOptions(
  kindOrOpts: UserFacingErrorKind | UserFacingErrorOptions = "generic",
): Required<Pick<UserFacingErrorOptions, "kind">> &
  Omit<UserFacingErrorOptions, "kind"> {
  if (typeof kindOrOpts === "string") {
    return { kind: kindOrOpts };
  }
  return { kind: kindOrOpts.kind ?? "generic", action: kindOrOpts.action };
}

/** Convert transport failures into stable, product-level UI copy. */
export function userFacingError(
  error: unknown,
  fallback: string,
  kindOrOpts: UserFacingErrorKind | UserFacingErrorOptions = "generic",
): string {
  const opts = resolveOptions(kindOrOpts);

  if (error instanceof WorkflowProviderConfigurationError) {
    return "Workflows are not configured on this deployment. Ask an admin to enable a workflow provider, or inspect runs with the gestalt CLI.";
  }

  if (error instanceof APIError) {
    if (error.status === 401) {
      return "Your session expired. Sign in again to continue.";
    }
    if (error.status === 403) {
      return "You do not have permission to perform this action.";
    }
    if (error.status === 404) {
      switch (opts.kind) {
        case "disconnect":
        case "connect":
        case "sign_in":
          // Caller supplies action-specific fallback; do not invent "resource".
          return fallback;
        case "select_instance":
          return "That account isn't available anymore. Refresh and choose again.";
        case "generic":
          return "The requested resource is no longer available.";
      }
    }
    if (WORKFLOW_PROVIDER_UNAVAILABLE.test(error.message)) {
      return "Workflows are not available on this deployment. Ask an admin to configure a workflow provider.";
    }
    if (
      opts.action === "cancel" &&
      (error.status === 412 ||
        WORKFLOW_CANCEL_ALREADY_STARTED.test(error.message))
    ) {
      return "This run can't be canceled because it already started. Refresh to see the latest status.";
    }
  }

  if (
    error instanceof Error &&
    error.message === "This workflow run is not available in this app."
  ) {
    return error.message;
  }

  return fallback;
}
