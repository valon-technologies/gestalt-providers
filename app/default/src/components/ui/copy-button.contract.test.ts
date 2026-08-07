import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const DIR = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(DIR, "copy-button.tsx"), "utf8");

describe("CopyIconButton copied tooltip", () => {
  test("ignores pointerdown dismiss so Copied does not unmount/flicker", () => {
    expect(SOURCE).toContain("pointerDownRef");
    expect(SOURCE).toContain("const open = copied || intentOpen");
    expect(SOURCE).toContain(
      "if (!next && (copied || pointerDownRef.current)) return",
    );
    expect(SOURCE).toContain("onPointerDownCapture");
    expect(SOURCE).toContain("setIntentOpen(true)");
    expect(SOURCE).toContain('copiedLabel = "Copied"');
  });

  test("only confirms Copied after clipboard.writeText resolves", () => {
    expect(SOURCE).toContain("navigator.clipboard.writeText(text).then(");
    expect(SOURCE).toContain("setCopied(true)");
    expect(SOURCE).toMatch(
      /writeText\(text\)\.then\(\s*\(\)\s*=>\s*\{[\s\S]*?setCopied\(true\)/,
    );
  });
});
