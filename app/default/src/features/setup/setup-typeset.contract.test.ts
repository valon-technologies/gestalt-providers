import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  SETUP_TYPESET_CHROME_CLASS,
  SETUP_TYPESET_CLASS,
  SETUP_TYPESET_NESTED_CHROME_CLASS,
} from "./setup-typeset";

const TYPESET = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "../../styles/typeset-reading.css"),
  "utf8",
);

describe("setup typeset contract", () => {
  test("host is Registry docs typeset; chrome opts out", () => {
    expect(SETUP_TYPESET_CLASS).toBe("typeset typeset-docs");
    expect(SETUP_TYPESET_CHROME_CLASS).toBe("not-typeset");
    expect(SETUP_TYPESET_NESTED_CHROME_CLASS).toContain("not-typeset");
    expect(SETUP_TYPESET_NESTED_CHROME_CLASS).toContain("--typeset-flow");
  });

  test("typeset-reading paints list markers from accent", () => {
    expect(TYPESET).toContain("--typeset-marker");
    expect(TYPESET).toContain("&:where(ol > li)::marker");
  });
});
