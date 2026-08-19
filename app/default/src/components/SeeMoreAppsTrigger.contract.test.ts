import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "SeeMoreAppsTrigger.tsx"),
  "utf8",
);

describe("SeeMoreAppsTrigger", () => {
  test("uses List Item Neutral hover and press, not ghost ink", () => {
    expect(SOURCE).toContain("listItemInteraction");
    expect(SOURCE).toContain('pointer: "css"');
    expect(SOURCE).not.toContain("ghostQuietChromePaintClassName");
    expect(SOURCE).toContain("group-hover/see-more:ring-neutral-hover");
    expect(SOURCE).toContain("group-active/see-more:ring-neutral-pressed");
  });
});
