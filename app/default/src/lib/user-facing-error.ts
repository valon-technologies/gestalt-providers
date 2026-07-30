import { APIError } from "@/lib/api";

/** Convert transport failures into stable, product-level UI copy. */
export function userFacingError(error: unknown, fallback: string): string {
  if (error instanceof APIError) {
    if (error.status === 401) return "Your session expired. Sign in again to continue.";
    if (error.status === 403) return "You do not have permission to perform this action.";
    if (error.status === 404) return "The requested resource is no longer available.";
  }
  return fallback;
}
