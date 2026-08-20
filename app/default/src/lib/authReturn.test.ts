// @vitest-environment happy-dom
import { afterEach, describe, expect, test } from "vitest";
import { CONNECTION_RETURN_PATH_STORAGE_KEY } from "./constants";
import {
  clearConnectionReturnPath,
  connectionReturnRedirectHref,
  consumeConnectionReturnPath,
  rememberConnectionReturnPath,
  sanitizeAuthReturnPath,
} from "./authReturn";

describe("sanitizeAuthReturnPath", () => {
  test("keeps Setup connect-apps return", () => {
    expect(sanitizeAuthReturnPath("/setup/apps")).toBe("/setup/apps");
  });

  test("rejects open redirects", () => {
    expect(sanitizeAuthReturnPath("https://evil.example/apps")).toBe("/apps");
    expect(sanitizeAuthReturnPath("//evil.example/apps")).toBe("/apps");
  });
});

describe("connectionReturnRedirectHref", () => {
  test("stays on the catalog when nothing was remembered", () => {
    expect(connectionReturnRedirectHref(null, "/apps")).toBeNull();
  });

  test("stays on the catalog when the remembered page is the catalog", () => {
    expect(connectionReturnRedirectHref("/apps", "/apps")).toBeNull();
  });

  test("leaves the catalog for Setup connect apps", () => {
    expect(connectionReturnRedirectHref("/setup/apps", "/apps")).toBe(
      "/setup/apps",
    );
  });

  test("sanitizes a hostile stored path to a same-page stay", () => {
    expect(
      connectionReturnRedirectHref("https://evil.example/phish", "/apps"),
    ).toBeNull();
  });
});

describe("remember and consume connection return path", () => {
  afterEach(() => {
    sessionStorage.clear();
  });

  test("stores a sanitized path and consumes it once", () => {
    rememberConnectionReturnPath("/setup/apps");
    expect(sessionStorage.getItem(CONNECTION_RETURN_PATH_STORAGE_KEY)).toBe(
      "/setup/apps",
    );
    expect(consumeConnectionReturnPath()).toBe("/setup/apps");
    expect(consumeConnectionReturnPath()).toBeNull();
  });

  test("clear drops a leftover path without returning it", () => {
    rememberConnectionReturnPath("/setup/apps");
    clearConnectionReturnPath();
    expect(consumeConnectionReturnPath()).toBeNull();
  });
});
