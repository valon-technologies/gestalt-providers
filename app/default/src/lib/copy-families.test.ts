import { describe, expect, test } from "vitest";
import {
  ACCOUNT_NAME_FALLBACK,
  APP_CONNECTED_LABEL,
  APP_NOT_CONNECTED_LABEL,
  APPS_CATALOG_DESCRIPTION,
  APPS_CONNECTED_BUCKET_DESCRIPTION,
  CONNECTION_NAV_LABEL,
  CONNECTION_STATUS_UNAVAILABLE,
  CONNECTION_SURFACE_TITLE,
  IDENTITY_CONNECTED_LABEL,
  IDENTITY_CONNECTION_REQUIRED_LABEL,
  MANAGE_CONNECTION_LABEL,
  NEEDS_SIGN_IN_LABEL,
  OTHER_SIGN_IN_METHODS_LABEL,
  SIGN_IN_AGAIN_LABEL,
  SIGN_IN_DETAILS_HEADING,
  SIGN_IN_WITH_OAUTH_LABEL,
  SIGNING_IN_LABEL,
  appConnectedCopy,
  appDisconnectedCopy,
  confirmAppConnectedFallback,
  connectAppActionLabel,
  connectAppDialogTitle,
  connectAppFailedCopy,
  connectionForAppAriaLabel,
  signInAgainActionAriaLabel,
} from "./accountCopy";
import {
  AFTER_SETUP_ARIA_LABEL,
  DISMISS_SETUP_REMINDER_LABEL,
  FINISH_SETUP_LABEL,
  LOADING_SETUP_LABEL,
  RESUME_SETUP_LABEL,
  SETUP_JOURNEY_LABEL,
  SETUP_STEPS_ARIA_LABEL,
  SETUP_STEP_NAV_ARIA_LABEL,
  SWITCH_ASSISTANTS_NAV_HINT,
  SETUP_MISSING_APPS_ADMIN_HINT,
} from "./setupJourneyCopy";

const SETUP_FAMILY_STRINGS = [
  SETUP_JOURNEY_LABEL,
  FINISH_SETUP_LABEL,
  RESUME_SETUP_LABEL,
  SWITCH_ASSISTANTS_NAV_HINT,
  SETUP_STEPS_ARIA_LABEL,
  SETUP_STEP_NAV_ARIA_LABEL,
  AFTER_SETUP_ARIA_LABEL,
  LOADING_SETUP_LABEL,
  DISMISS_SETUP_REMINDER_LABEL,
  SETUP_MISSING_APPS_ADMIN_HINT,
];

const APP_CONNECTION_STRINGS = [
  CONNECTION_SURFACE_TITLE,
  CONNECTION_NAV_LABEL,
  APP_CONNECTED_LABEL,
  APP_NOT_CONNECTED_LABEL,
  SIGN_IN_AGAIN_LABEL,
  SIGNING_IN_LABEL,
  NEEDS_SIGN_IN_LABEL,
  OTHER_SIGN_IN_METHODS_LABEL,
  MANAGE_CONNECTION_LABEL,
  IDENTITY_CONNECTED_LABEL,
  IDENTITY_CONNECTION_REQUIRED_LABEL,
  ACCOUNT_NAME_FALLBACK,
  SIGN_IN_WITH_OAUTH_LABEL,
  CONNECTION_STATUS_UNAVAILABLE,
  connectAppActionLabel("GitHub"),
  connectAppDialogTitle("Notion"),
  signInAgainActionAriaLabel("Slack"),
  appConnectedCopy("BigQuery"),
  confirmAppConnectedFallback("Notion"),
  connectAppFailedCopy("Slack"),
  appDisconnectedCopy("Gmail"),
  APPS_CATALOG_DESCRIPTION,
  APPS_CONNECTED_BUCKET_DESCRIPTION,
  SIGN_IN_DETAILS_HEADING,
  connectionForAppAriaLabel("Notion"),
];

describe("copy families", () => {
  test("Setup chrome is Setup, not Connect", () => {
    expect(SETUP_JOURNEY_LABEL).toBe("Setup");
    expect(FINISH_SETUP_LABEL).toBe("Finish setup");
    expect(RESUME_SETUP_LABEL).toBe("Resume setup");
    for (const value of SETUP_FAMILY_STRINGS) {
      expect(value.toLowerCase()).not.toMatch(/connect/);
    }
  });

  test("first-time app CTA is Connect {app}", () => {
    expect(connectAppActionLabel("GitHub")).toBe("Connect GitHub");
    expect(connectAppActionLabel("GitHub")).toBe(connectAppDialogTitle("GitHub"));
    expect(connectAppActionLabel("GitHub")).not.toBe(SETUP_JOURNEY_LABEL);
    expect(SIGN_IN_AGAIN_LABEL).not.toBe(SETUP_JOURNEY_LABEL);
  });

  test("app connection status is Connected / Not connected", () => {
    expect(APP_CONNECTED_LABEL).toBe("Connected");
    expect(APP_NOT_CONNECTED_LABEL).toBe("Not connected");
    expect(CONNECTION_SURFACE_TITLE).toBe("Connection");
    expect(CONNECTION_NAV_LABEL).toBe("Connection");
    expect(appConnectedCopy("Slack")).toBe("Slack is connected.");
    expect(appDisconnectedCopy("Slack")).toBe("Slack is disconnected.");
    expect(CONNECTION_STATUS_UNAVAILABLE).toBe(
      "Couldn't load connection status. Try again.",
    );
  });

  test("Setup chrome does not collide with app Connect copy", () => {
    expect(APP_CONNECTION_STRINGS).toContain("Connect GitHub");
    expect(SETUP_JOURNEY_LABEL).not.toBe("Connect");
  });
});
