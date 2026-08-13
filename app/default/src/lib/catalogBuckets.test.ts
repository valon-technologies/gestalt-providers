import { describe, expect, test } from "vitest";
import type { Integration } from "@/lib/api";
import {
  catalogBucketIdFor,
  catalogBucketsPresentIn,
} from "./catalogBuckets";

function stub(
  name: string,
  extra: Partial<Integration> = {},
): Integration {
  return {
    name,
    displayName: extra.displayName ?? name,
    description: extra.description ?? name,
    ...extra,
  };
}

describe("catalogBucketsPresentIn", () => {
  test("returns catalog buckets that have at least one app, in browse order", () => {
    const buckets = catalogBucketsPresentIn([
      stub("gmail"),
      stub("github"),
      stub("ashby"),
      stub("gmail"),
    ]);
    expect(buckets.map((bucket) => bucket.id)).toEqual([
      "communication",
      "developer-tools",
      "people",
    ]);
  });

  test("omits empty buckets", () => {
    expect(catalogBucketsPresentIn([stub("slack")]).map((bucket) => bucket.id)).toEqual([
      "communication",
    ]);
  });
});

describe("catalogBucketIdFor", () => {
  test("maps known apps and falls back to other", () => {
    expect(catalogBucketIdFor(stub("slack"))).toBe("communication");
    expect(catalogBucketIdFor(stub("unknown-source"))).toBe("other");
  });
});
