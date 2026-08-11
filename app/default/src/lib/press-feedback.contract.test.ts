import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  ghostQuietChromeClassName,
  ghostQuietChromePaintClassName,
  pressFeedbackScrimClassName,
} from "@/lib/press-feedback";

const SOURCE = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "press-feedback.ts"),
  "utf8",
);

describe("press-feedback quiet chrome (toolshed#4081 / #4191)", () => {
  test("exports scrim + ghost paint + secondary surface roles", () => {
    expect(SOURCE).toContain("export const pressFeedbackScrimClassName");
    expect(SOURCE).toContain("export const ghostQuietChromePaintClassName");
    expect(SOURCE).toContain("export const ghostQuietChromeClassName");
    expect(SOURCE).toContain("export const secondarySurfaceFillClassName");
    expect(SOURCE).toContain('bg-foreground/[0.06]');
  });

  test("ghost paint never uses accent hover wash", () => {
    expect(ghostQuietChromePaintClassName).toBe(
      "bg-transparent text-muted-foreground hover:text-foreground",
    );
    expect(ghostQuietChromePaintClassName).not.toContain("hover:bg-accent");
    expect(ghostQuietChromeClassName).toContain(pressFeedbackScrimClassName);
  });

  test("group activate drives scrim from a parent .group", () => {
    expect(SOURCE).toContain("export const ghostQuietChromeGroupActivateClassName");
    expect(SOURCE).toContain("group-hover:after:opacity-");
  });
});
