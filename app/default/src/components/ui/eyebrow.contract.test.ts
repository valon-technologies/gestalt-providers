import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const DIR = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(DIR, "eyebrow.tsx"), "utf8");

describe("Eyebrow tone contract", () => {
  test("defaults to denser muted-foreground ink, not the soft caption step", () => {
    expect(SOURCE).toContain('default: "text-muted-foreground"');
    expect(SOURCE).toContain('muted: "text-muted-foreground-soft"');
    expect(SOURCE).toContain('accent: "text-accent-strong"');
    expect(SOURCE).toContain('tone: "default"');
    expect(SOURCE).not.toContain('tone: "secondary"');
    expect(SOURCE).not.toContain('brand: "text-brand"');
  });
});
