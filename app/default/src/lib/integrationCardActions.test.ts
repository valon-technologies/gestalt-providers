import { describe, expect, test } from "vitest";
import { integrationCardActionPolicy } from "./integrationCardActions";

describe("integrationCardActionPolicy", () => {
  test("store manage keeps overflow, connect, and Open app", () => {
    expect(integrationCardActionPolicy("manage")).toMatchObject({
      density: "default",
      connectionEntry: "app-detail",
      allowConnect: true,
      allowOpenApp: true,
      allowOverflow: true,
    });
  });

  test("Setup Connect is compact modal connect, never overflow", () => {
    expect(integrationCardActionPolicy("connect")).toEqual({
      density: "compact",
      connectionEntry: "modal",
      allowConnect: true,
      allowOpenApp: false,
      allowOverflow: false,
      allowConnectedMark: true,
    });
  });

  test("Setup Try launches the app and hides overflow and Add", () => {
    expect(integrationCardActionPolicy("launch")).toEqual({
      density: "default",
      connectionEntry: "app-detail",
      allowConnect: false,
      allowOpenApp: true,
      allowOverflow: false,
      allowConnectedMark: true,
    });
  });
});
