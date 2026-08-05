import { describe, expect, it, test } from "vitest";

import { APIError } from "@/lib/api";
import { userFacingError } from "@/lib/user-facing-error";
import { WorkflowProviderConfigurationError } from "@/lib/workflowProvider";

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

  it("maps workflow provider configuration failures without offering a useless retry", () => {
    expect(
      userFacingError(
        new WorkflowProviderConfigurationError(),
        "Unable to load workflow activity. Try again.",
      ),
    ).toMatch(/not configured on this deployment/i);
  });

  it("maps unavailable workflow providers from API errors", () => {
    expect(
      userFacingError(
        new APIError(400, 'workflow provider "temporal" is not available'),
        "Unable to load workflow activity. Try again.",
      ),
    ).toMatch(/not available on this deployment/i);
  });

  it("maps cancel-after-start failures when cancel action is declared", () => {
    expect(
      userFacingError(
        new APIError(412, "workflow run cannot be canceled once it has started"),
        "Unable to cancel this workflow run. Try again.",
        { action: "cancel" },
      ),
    ).toMatch(/already started/i);
  });
});
