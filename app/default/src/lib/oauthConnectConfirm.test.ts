import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import type { Integration } from "@/lib/api";
import {
  appIsConnectedCopy,
  isConnectedInCatalog,
  oauthConnectedToastMessage,
} from "./oauthConnectConfirm";

function stub(partial: Partial<Integration> & Pick<Integration, "name">): Integration {
  return {
    displayName: partial.name,
    description: "",
    ...partial,
  };
}

const disconnected: Integration = stub({
  name: "bigquery",
  credentialState: "missing",
  status: "needs_user_connection",
  connections: [
    {
      name: "default",
      connected: false,
      credentialState: "missing",
      status: "needs_user_connection",
      authTypes: ["oauth"],
    },
  ],
});

const connected: Integration = stub({
  name: "bigquery",
  credentialState: "connected",
  status: "ready",
  connections: [
    {
      name: "default",
      connected: true,
      credentialState: "connected",
      status: "ready",
      authTypes: ["oauth"],
    },
  ],
});

describe("isConnectedInCatalog", () => {
  test("is true only after this app is connected", () => {
    expect(isConnectedInCatalog([disconnected], "bigquery")).toBe(false);
    expect(isConnectedInCatalog([connected], "bigquery")).toBe(true);
  });

  test("ignores other apps and a missing catalog", () => {
    expect(isConnectedInCatalog([connected], "slack")).toBe(false);
    expect(isConnectedInCatalog(undefined, "bigquery")).toBe(false);
    expect(isConnectedInCatalog([], "bigquery")).toBe(false);
  });
});

describe("appIsConnectedCopy", () => {
  test("names the app without filler", () => {
    expect(appIsConnectedCopy("BigQuery")).toBe("BigQuery is connected.");
  });
});

describe("oauthConnectedToastMessage", () => {
  test("toasts only after the catalog shows the app connected", () => {
    expect(oauthConnectedToastMessage(true, "Slack")).toBe("Slack is connected.");
    expect(oauthConnectedToastMessage(false, "Slack")).toBeNull();
  });
});

describe("refetchIntegrationConnected", () => {
  test("reloads the catalog with the same AbortSignal query as the apps list", () => {
    const source = readFileSync(
      join(dirname(fileURLToPath(import.meta.url)), "oauthConnectConfirm.ts"),
      "utf8",
    );
    expect(source).toContain("queryFn: ({ signal }) => getIntegrations(signal)");
    expect(source).not.toContain("queryFn: getIntegrations,");
  });
});
