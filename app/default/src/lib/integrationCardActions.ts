import type { ConnectionContext } from "@/lib/integrationStatus";

/**
 * Jobs this App Card instance offers. Install state still decides whether a
 * job can appear (Open app only when mounted, More only when connected).
 *
 * - `manage` — Apps store: Connect, Open app, More (Remove)
 * - `connect` — Setup Connect: Connect / connected mark
 * - `launch` — Setup Try: Open app, no More, no Add
 */
export type IntegrationCardActions = "manage" | "connect" | "launch";

export type IntegrationCardActionPolicy = {
  density: "default" | "compact";
  connectionEntry: "app-detail" | "modal";
  allowConnect: boolean;
  allowOpenApp: boolean;
  /** Ellipsis: Remove app / Manage connection. */
  allowOverflow: boolean;
  allowConnectedMark: boolean;
};

export function integrationCardActionPolicy(
  actions: IntegrationCardActions,
  connectionContext: ConnectionContext = "current_user",
): IntegrationCardActionPolicy {
  switch (actions) {
    case "connect":
      return {
        density: "compact",
        connectionEntry: "modal",
        allowConnect: true,
        allowOpenApp: false,
        allowOverflow: false,
        allowConnectedMark: true,
      };
    case "launch":
      return {
        density: "default",
        connectionEntry: "app-detail",
        allowConnect: false,
        allowOpenApp: true,
        allowOverflow: false,
        allowConnectedMark: true,
      };
    case "manage":
      return {
        density: "default",
        connectionEntry:
          connectionContext === "current_user" ? "app-detail" : "modal",
        allowConnect: true,
        allowOpenApp: true,
        allowOverflow: true,
        allowConnectedMark: true,
      };
  }
}
