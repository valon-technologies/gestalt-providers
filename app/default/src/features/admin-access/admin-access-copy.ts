/** Product copy for Admin — who can use which apps. Keep engineer words off the page. */

export const ADMIN_NAV_LABEL = "Admin";
export const ADMIN_PAGE_TITLE = "Admin";
export const ADMIN_PAGE_DESCRIPTION = "Choose who can use each app.";

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

export const LOCKED_FROM_CONFIG = "Set in workspace config — can’t change here";
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
