/**
 * Settings create-token layout. The page sits in the shared reading column
 * (`PAGE_LAYOUT_READING_COLUMN_CLASS`). The form is the width container:
 * name and expiration cap at half of it; the app picker, actions, and
 * one-time secret callout span the form.
 *
 * Owned by Settings. TokenCreateForm only accepts width class props.
 */
export const SETTINGS_TOKEN_CREATE_TRACK = {
  form: "w-full @container",
  controls: "w-full max-w-[50cqi] min-w-0",
  appAccessPanel: "w-full min-w-0",
  actions: "w-full",
} as const;
