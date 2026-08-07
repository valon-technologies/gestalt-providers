/**
 * Settings create-token layout tracks on an 8-column content grid:
 * form shell = 8, token/expiration controls = 2, app-access panel = 3,
 * actions divider = 4.
 *
 * Owned by Settings — TokenCreateForm only accepts width class props.
 */
export const SETTINGS_TOKEN_CREATE_TRACK = {
  form: "w-full",
  controls: "w-[calc(2/8*100%)] min-w-0",
  appAccessPanel: "w-[calc(3/8*100%)] min-w-0",
  actions: "w-[calc(4/8*100%)] min-w-0",
} as const;
