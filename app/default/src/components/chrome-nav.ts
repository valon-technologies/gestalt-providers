import { SETUP_JOURNEY_LABEL } from "@/lib/setupJourneyCopy";
import { ADMIN_PATH, SETUP_PATH, DOCS_PATH } from "@/lib/constants";

/**
 * App-shell destinations. Product links belong in the top nav; utility links
 * belong in the signed-in account menu. A path must not appear in both.
 */
export const CHROME_NAV = [
  { to: "/apps", label: "Apps", kind: "product" },
  { to: SETUP_PATH, label: SETUP_JOURNEY_LABEL, kind: "product" },
  {
    to: ADMIN_PATH,
    label: "Admin",
    kind: "product",
    when: "gestaltAdmin",
  },
  { to: DOCS_PATH, label: "Docs", kind: "utility" },
  { to: "/settings", label: "Settings", kind: "utility" },
] as const;

export type ChromeNavDestination = (typeof CHROME_NAV)[number];

function isGatedAdmin(item: ChromeNavDestination): boolean {
  return "when" in item && item.when === "gestaltAdmin";
}

/** Top-nav product destinations. Admin is included only when the gate is open. */
export function chromeProductNav(showAdmin: boolean) {
  return CHROME_NAV.filter((item) => {
    if (item.kind !== "product") return false;
    if (isGatedAdmin(item)) return showAdmin;
    return true;
  });
}

/** Signed-in account-menu utilities (Docs, Settings). */
export function chromeUtilityNav() {
  return CHROME_NAV.filter((item) => item.kind === "utility");
}
