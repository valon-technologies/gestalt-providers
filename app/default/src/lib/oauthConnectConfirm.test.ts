import { describe, expect, test } from "vitest";
import type { Integration } from "@/lib/api";
import { isConnectedInCatalog, appIsConnectedCopy } from "./oauthConnectConfirm";

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
