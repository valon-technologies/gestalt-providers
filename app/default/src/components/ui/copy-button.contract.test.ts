import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const DIR = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(join(DIR, "copy-button.tsx"), "utf8");

describe("CopyIconButton feedback (toolshed#4296)", () => {
  test("press-dismiss only; end-press without click clears intent", () => {
    expect(SOURCE).toContain("pointerDownRef");
    expect(SOURCE).toContain("clickedRef");
    expect(SOURCE).toContain(
      'const open = feedback !== "idle" || intentOpen',
    );
    expect(SOURCE).toContain("if (!next && pointerDownRef.current) return");
    expect(SOURCE).not.toContain(
      "if (!next && (copied || pointerDownRef.current)) return",
    );
    expect(SOURCE).toContain('addEventListener("pointerup", endPress, true)');
    expect(SOURCE).toContain("setTimeout");
    expect(SOURCE).not.toContain("queueMicrotask");
    expect(SOURCE).toContain("if (!clickedRef.current) setIntentOpen(false)");
    expect(SOURCE).toContain("clickedRef.current = true");
    expect(SOURCE).toContain('setFeedback("copied")');
    expect(SOURCE).toContain('setFeedback("failed")');
    expect(SOURCE).not.toContain("setPointerCapture");
    expect(SOURCE).not.toContain("onPointerLeave");
    expect(SOURCE).not.toMatch(/onClick=\{\(\) => \{[^}]*setIntentOpen\(true\)/s);
    expect(SOURCE).toContain("await navigator.clipboard.writeText(text)");
  });

  test("chip density fills the action cell and uses before for hit slop", () => {
    expect(SOURCE).toContain("COPY_ICON_CHIP_CLASS");
    expect(SOURCE).toContain('density === "chip" &&');
    expect(SOURCE).toContain("self-stretch");
    expect(SOURCE).toContain("before:-top-1.5");
    expect(SOURCE).toContain("before:-right-1.5");
    expect(SOURCE).toContain("before:-bottom-1.5");
    expect(SOURCE).toContain("before:left-0");
    expect(SOURCE).not.toContain("after:-inset-1.5");
    expect(SOURCE).toContain("size-auto");
    expect(SOURCE).toContain('density === "chip" ? null : "icon-xs"');
    expect(SOURCE).not.toContain('const size = sizeProp ?? "icon-xs"');
  });
});
