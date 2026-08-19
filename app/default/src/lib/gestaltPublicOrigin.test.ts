import { describe, expect, test } from "vitest";

import {
  GESTALT_PUBLIC_ORIGIN_PLACEHOLDER,
  resolveGestaltPublicOrigin,
} from "./gestaltPublicOrigin";

describe("resolveGestaltPublicOrigin", () => {
  test("does not tell users to paste localhost", () => {
    expect(resolveGestaltPublicOrigin()).toBe(
      GESTALT_PUBLIC_ORIGIN_PLACEHOLDER,
    );
  });
});
