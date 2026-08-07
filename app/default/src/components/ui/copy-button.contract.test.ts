import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const DIR = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(DIR, "copy-button.tsx"), "utf8");

describe("CopyIconButton copied tooltip (toolshed#4111)", () => {
  test("press-dismiss only; end-press without click clears intent", () => {
    expect(SOURCE).toContain("pointerDownRef");
    expect(SOURCE).toContain("clickedRef");
    expect(SOURCE).toContain("const open = copied || intentOpen");
    expect(SOURCE).toContain("if (!next && pointerDownRef.current) return");
    expect(SOURCE).not.toContain(
      "if (!next && (copied || pointerDownRef.current)) return",
    );
    expect(SOURCE).toContain('addEventListener("pointerup", endPress, true)');
    expect(SOURCE).toContain("setTimeout");
    expect(SOURCE).not.toContain("queueMicrotask");
    expect(SOURCE).toContain("if (!clickedRef.current) setIntentOpen(false)");
    expect(SOURCE).toContain("clickedRef.current = true");
    expect(SOURCE).toContain("setCopied(true)");
    expect(SOURCE).not.toContain("setPointerCapture");
    expect(SOURCE).not.toContain("onPointerLeave");
    expect(SOURCE).not.toMatch(/onClick=\{\(\) => \{[^}]*setIntentOpen\(true\)/s);
    // Registry fire-and-forget confirm — do not reintroduce #1250 .then gating.
    expect(SOURCE).toContain("void navigator.clipboard.writeText(text)");
    expect(SOURCE).not.toMatch(
      /writeText\([^)]*\)\.then\([\s\S]*setCopied\(true\)/,
    );
  });
});
