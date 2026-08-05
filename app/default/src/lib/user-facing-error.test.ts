import { describe, expect, test } from "vitest";

import { APIError } from "@/lib/api";
import { userFacingError } from "./user-facing-error";

describe("userFacingError", () => {
  test("keeps disconnect fallback on 404 instead of resource jargon", () => {
    expect(
      userFacingError(
        new APIError(404, "no connection found"),
        "Couldn't disconnect Gmail. Try again.",
        "disconnect",
      ),
    ).toBe("Couldn't disconnect Gmail. Try again.");
  });

  test("maps select-instance 404 to account-unavailable copy", () => {
    expect(
      userFacingError(
        new APIError(404, "gone"),
        "Couldn't choose that account. Try again.",
        "select_instance",
      ),
    ).toMatch(/account isn't available/i);
  });

  test("generic 404 still uses resource copy", () => {
    expect(
      userFacingError(new APIError(404, "missing"), "fallback", "generic"),
    ).toBe("The requested resource is no longer available.");
  });

  test("session and permission copy", () => {
    expect(userFacingError(new APIError(401, "x"), "fallback")).toMatch(/session/i);
    expect(userFacingError(new APIError(403, "x"), "fallback")).toMatch(/permission/i);
  });
});
