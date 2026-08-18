/**
 * Setup create-token layout. The form is the width container: name and
 * expiration cap at half of it; the actions divider and Create token row
 * span the full form. Owned by Setup — TokenCreateForm only accepts width
 * class props.
 */
export const SETUP_TOKEN_CREATE_TRACK = {
  form: "w-full @container",
  controls: "w-full max-w-[50cqi] min-w-0",
  actions: "w-full",
} as const;

/** Clears TimelineStepsContent’s description pull-up so the field group sits below the title. */
export const SETUP_TOKEN_CREATE_CONTENT_CLASS = "mt-5";
