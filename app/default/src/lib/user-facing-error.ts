import { APIError } from "@/lib/api";

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

/** Convert transport failures into stable, product-level UI copy. */
export function userFacingError(
  error: unknown,
  fallback: string,
  kind: UserFacingErrorKind = "generic",
): string {
  if (error instanceof APIError) {
    if (error.status === 401) {
      return "Your session expired. Sign in again to continue.";
    }
    if (error.status === 403) {
      return "You do not have permission to perform this action.";
    }
    if (error.status === 404) {
      switch (kind) {
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
  }
  return fallback;
}
