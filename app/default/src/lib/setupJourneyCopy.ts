/**
 * Setup family: wire this assistant into this workspace.
 *
 * Connect / Disconnect belong to apps (`accountCopy.ts`).
 * Do not use Connect as the Setup place name.
 */

export const SETUP_JOURNEY_LABEL = "Setup" as const;

export const FINISH_SETUP_LABEL = "Finish setup" as const;

export const RESUME_SETUP_LABEL = "Resume setup" as const;

export const SWITCH_ASSISTANTS_NAV_HINT =
  "Switch assistants anytime from Setup in the top nav" as const;

export const SETUP_STEPS_ARIA_LABEL = "Setup steps" as const;

export const SETUP_STEP_NAV_ARIA_LABEL = "Setup step navigation" as const;

export const AFTER_SETUP_ARIA_LABEL = "After setup" as const;

export const LOADING_SETUP_LABEL = "Loading Setup…" as const;

export const DISMISS_SETUP_REMINDER_LABEL = "Dismiss setup reminder" as const;
