/** Product copy for Admin. Keep engineer words off the page. */

export const ADMIN_NAV_LABEL = "Admin";
export const ADMIN_SECTIONS_NAV_LABEL = "Admin sections";

export const APP_ACCESS_NAV_LABEL = "App access";
export const APP_ACCESS_PAGE_TITLE = "App access";
export const APP_ACCESS_PAGE_DESCRIPTION =
  "Set who can use each app: everyone, specific people and groups, or no one.";

export const PLATFORM_ADMINS_NAV_LABEL = "Platform admins";
export const PLATFORM_ADMINS_PAGE_TITLE = "Platform admins";
export const PLATFORM_ADMINS_PAGE_DESCRIPTION =
  "People and groups who can open Admin.";
export const PLATFORM_ADMINS_EMPTY_GROUPS =
  "No groups yet. Add a group so a whole team can open Admin.";
export const PLATFORM_ADMINS_EMPTY_PEOPLE =
  "No individual people. Add someone only if they are not already in a group.";
export const PLATFORM_ADMINS_SAVED_PERSON = (label: string) =>
  `${label} can open Admin.`;
export const PLATFORM_ADMINS_SAVED_GROUP = (label: string) =>
  `${label} can open Admin.`;
export const PLATFORM_ADMINS_UNAVAILABLE =
  "Couldn't load platform admins. Authorization is unavailable on this server.";
export const PLATFORM_ADMINS_LOAD_ERROR =
  "Couldn't load platform admins. Try again.";
export const PLATFORM_ADMINS_FORBIDDEN =
  "You do not have permission to manage platform admins.";
export const PLATFORM_ADMINS_ADD_GROUP_DESCRIPTION =
  "Add a group so a whole team can open Admin.";
export const PLATFORM_ADMINS_ADD_PERSON_DESCRIPTION =
  "Add someone only if they are not already in a group.";

export const APP_VERSIONS_NAV_LABEL = "App versions";
export const APP_VERSIONS_PAGE_TITLE = "App versions";
export const APP_VERSIONS_PAGE_DESCRIPTION =
  "Current fleet health, desired versions, and rollout progress.";
export const APP_VERSIONS_EMPTY_TITLE = "No registry apps";
export const APP_VERSIONS_EMPTY_DESCRIPTION =
  "No registry-only apps are configured in this workspace.";
export const APP_VERSIONS_SEARCH_EMPTY = "No apps match that search.";
export const APP_VERSIONS_LOAD_ERROR = "Couldn't load app versions. Try again.";
export const APP_VERSIONS_NOT_INSTALLED = "Not installed";
export const APP_VERSIONS_NO_DATA = "No data";
export const APP_VERSIONS_NOT_FOUND = "This registry app was not found.";
export const APP_VERSIONS_LIVE_ON_TARGET = (count: number) =>
  `${count} live on target`;
export const APP_VERSIONS_LIVE_ON_TARGET_PARTIAL = (
  onTarget: number,
  live: number,
) => `${onTarget} of ${live} live on target`;
export const APP_VERSIONS_COHORT = (reloaded: number, acknowledged: number) =>
  `${reloaded} of ${acknowledged} reloaded`;
export const APP_VERSIONS_LAST_ROLLOUT_RELOADED = (
  reloaded: number,
  acknowledged: number,
) => `last rollout ${reloaded} of ${acknowledged} reloaded`;
export const APP_VERSIONS_ROLLOUT_RELOADED = (
  reloaded: number,
  acknowledged: number,
) => `rollout ${reloaded} of ${acknowledged} reloaded`;
export const APP_VERSIONS_ROW_META_SEP = " · ";

export const ADMIN_METRICS_NAV_LABEL = "Metrics";
export const ADMIN_METRICS_PAGE_TITLE = "Metrics";
export const ADMIN_METRICS_PAGE_DESCRIPTION =
  "Live telemetry from this server since it started. This is not a 24 hour history.";
export const ADMIN_METRICS_UNAVAILABLE =
  "Metrics are unavailable on this server.";
export const ADMIN_METRICS_LOAD_ERROR = "Couldn't load metrics. Try again.";
export const ADMIN_METRICS_REFRESH = "Refresh";
export const ADMIN_METRICS_EMPTY = "Click refresh to load metrics.";
export const ADMIN_METRICS_LOADING = "Loading metrics…";
export const ADMIN_METRICS_LAST_REFRESHED = (time: string) =>
  `Last refreshed ${time}`;

/** @deprecated Use APP_ACCESS_PAGE_TITLE */
export const ADMIN_PAGE_TITLE = APP_ACCESS_PAGE_TITLE;
/** @deprecated Use APP_ACCESS_PAGE_DESCRIPTION */
export const ADMIN_PAGE_DESCRIPTION = APP_ACCESS_PAGE_DESCRIPTION;

/** One-phrase list status. Do not pair a count with a second chip. */
export const ACCESS_LIST_STATUS = {
  everyone: "Everyone",
  noOne: "No one",
  nobodyYet: "Nobody yet",
} as const;

export const ACCESS_RULE_HEADING = (appLabel: string) =>
  `Who can use ${appLabel}`;

export const ACCESS_RULE_CHOICES = {
  everyone: {
    label: "Everyone in the workspace",
    description:
      "Anyone signed in can use this app. They may still need to connect an account.",
  },
  specific: {
    label: "Specific people and groups",
    description: "Only the groups and people below can use this app.",
  },
  noOne: {
    label: "No one",
    description:
      "This app stays in the workspace, but nobody can use it until you change this.",
  },
} as const;

export const ADD_GROUP_LABEL = "Add group";
export const ADD_PERSON_LABEL = "Add person";
export const REMOVE_ACCESS_LABEL = "Remove access";

export const EMPTY_GROUPS =
  "No groups yet. Add a group so a whole team can use this app.";
export const EMPTY_PEOPLE =
  "No individual people. Add someone only if they are not already in a group.";

export const LOCKED_FROM_CONFIG =
  "Set in workspace config. Can’t change here.";
export const EVERYONE_BLOCKS_REMOVE =
  "This app is on for everyone. To limit it, choose Specific people and groups first.";

export const ACCESS_SECTIONS_NAV_LABEL = "Access sections";
export const ACCESS_WHO_NAV_LABEL = "Who can use";
export const GROUPS_SECTION_TITLE = "Groups";
export const PEOPLE_SECTION_TITLE = "People";

export const ACCESS_SECTION_IDS = {
  who: "admin-access-who",
  groups: "admin-access-groups",
  people: "admin-access-people",
} as const;

export const ADD_GROUP_DIALOG_TITLE = "Add group";
export const ADD_GROUP_FIELD_LABEL = "Group";
export const ADD_GROUP_FIELD_HINT =
  "Group id from your directory, for example eng or group:eng#member.";
export const ADD_PERSON_DIALOG_TITLE = "Add person";
export const ADD_PERSON_FIELD_LABEL = "Email";
export const ADD_PERSON_FIELD_HINT =
  "Work email for someone who is not already in a group.";
export const DIALOG_CANCEL = "Cancel";

export function savedOnForGroup(appLabel: string, groupLabel: string): string {
  return `${appLabel} is on for ${groupLabel}.`;
}

export function savedOnForPerson(appLabel: string, personLabel: string): string {
  return `${appLabel} is on for ${personLabel}.`;
}

export function savedOffForEveryone(appLabel: string): string {
  return `${appLabel} is off for everyone.`;
}

export function accessCountLabel(groups: number, people: number): string | null {
  const groupPart =
    groups <= 0 ? null : groups === 1 ? "1 group" : `${groups} groups`;
  const peoplePart =
    people <= 0 ? null : people === 1 ? "1 person" : `${people} people`;
  if (groupPart && peoplePart) return `${groupPart} and ${peoplePart}`;
  return groupPart ?? peoplePart;
}

export function accessListStatus(args: {
  rule: "everyone" | "specific" | "no_one";
  groups: number;
  people: number;
}): string {
  if (args.rule === "everyone") return ACCESS_LIST_STATUS.everyone;
  if (args.rule === "no_one") return ACCESS_LIST_STATUS.noOne;
  return accessCountLabel(args.groups, args.people) ?? ACCESS_LIST_STATUS.nobodyYet;
}
