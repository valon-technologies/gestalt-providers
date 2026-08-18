// @vitest-environment happy-dom
import { afterEach, describe, expect, test } from "vitest";
import {
  BUILD_API_TOKEN_GRANT_ID_STORAGE_KEY,
  BUILD_CREATE_NEW_TOKEN_ID,
  BUILD_SELECTED_TOKEN_ID_STORAGE_KEY,
  readStoredApiTokenGrantId,
  writeStoredApiTokenGrantId,
} from "./buildPaths";

describe("readStoredApiTokenGrantId", () => {
  afterEach(() => {
    sessionStorage.clear();
  });

  test("copies a leftover selected grant id into the grant key", () => {
    sessionStorage.setItem(BUILD_SELECTED_TOKEN_ID_STORAGE_KEY, "tok_legacy");
    expect(readStoredApiTokenGrantId()).toBe("tok_legacy");
    expect(sessionStorage.getItem(BUILD_API_TOKEN_GRANT_ID_STORAGE_KEY)).toBe(
      "tok_legacy",
    );
    expect(sessionStorage.getItem(BUILD_SELECTED_TOKEN_ID_STORAGE_KEY)).toBeNull();
  });

  test("prefers the grant key over a leftover selected id", () => {
    sessionStorage.setItem(BUILD_API_TOKEN_GRANT_ID_STORAGE_KEY, "tok_grant");
    sessionStorage.setItem(BUILD_SELECTED_TOKEN_ID_STORAGE_KEY, "tok_old");
    expect(readStoredApiTokenGrantId()).toBe("tok_grant");
    expect(sessionStorage.getItem(BUILD_SELECTED_TOKEN_ID_STORAGE_KEY)).toBeNull();
  });

  test("ignores leftover radio sentinels", () => {
    sessionStorage.setItem(
      BUILD_SELECTED_TOKEN_ID_STORAGE_KEY,
      BUILD_CREATE_NEW_TOKEN_ID,
    );
    expect(readStoredApiTokenGrantId()).toBe("");
  });

  test("writeStoredApiTokenGrantId drops the leftover selected key", () => {
    sessionStorage.setItem(BUILD_SELECTED_TOKEN_ID_STORAGE_KEY, "tok_old");
    writeStoredApiTokenGrantId("tok_new");
    expect(sessionStorage.getItem(BUILD_API_TOKEN_GRANT_ID_STORAGE_KEY)).toBe(
      "tok_new",
    );
    expect(sessionStorage.getItem(BUILD_SELECTED_TOKEN_ID_STORAGE_KEY)).toBeNull();
  });
});
