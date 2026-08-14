import { describe, expect, test } from "vitest";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const SOURCE = readFileSync(
  join(HERE, "outcome-status-indicator.tsx"),
  "utf8",
);

describe("OutcomeStatusIndicator", () => {
  test("maps Registry mid-dark ramps onto status-indicator tokens (not palette literals)", () => {
    expect(SOURCE).toContain(
      'success: "bg-status-indicator-success text-white"',
    );
    expect(SOURCE).toContain(
      'failure: "bg-status-indicator-danger text-white"',
    );
    expect(SOURCE).toContain(
      'warning: "bg-status-indicator-warning text-white"',
    );
    expect(SOURCE).toContain('info: "bg-status-indicator-info text-white"');
    expect(SOURCE).toContain(
      'in_progress: "bg-status-indicator-warning text-white"',
    );
    expect(SOURCE).not.toContain("bg-green-500");
    expect(SOURCE).not.toContain("bg-red-500");
    expect(SOURCE).not.toContain("bg-yellow-500");
    expect(SOURCE).not.toContain("bg-blue-500");
    expect(SOURCE).not.toContain("--valon-green");
  });

  test("respects reduced motion on the in-progress spinner", () => {
    expect(SOURCE).toContain("animate-spin motion-reduce:animate-none");
  });

  test("pins the glyph to the label first-line rail", () => {
    expect(SOURCE).toContain("inline-flex h-5 shrink-0 items-center");
    expect(SOURCE).toContain("text-sm leading-5 text-foreground");
    expect(SOURCE).toContain('visibleLabel ? "items-start" : "items-center"');
    expect(SOURCE).toContain('"inline-flex gap-2"');
  });

  test("maps statuses to Badge variants (failure → destructive until error exists)", () => {
    expect(SOURCE).toContain("outcomeStatusIndicatorBadgeVariant");
    expect(SOURCE).toContain('return "success"');
    expect(SOURCE).toContain('return "destructive"');
    expect(SOURCE).toContain('return "warning"');
    expect(SOURCE).toContain('return "info"');
    expect(SOURCE).toContain('return "muted"');
    expect(SOURCE).not.toContain('return "error"');
  });

  test("info is a filled notice glyph, not pending", () => {
    expect(SOURCE).toContain("info: { icon: Info");
    expect(SOURCE).toContain('info: "bg-status-indicator-info text-white"');
  });

  test("pending is a single hollow Circle (no shell border)", () => {
    expect(SOURCE).toContain(
      'pending: "bg-transparent text-muted-foreground"',
    );
    expect(SOURCE).not.toContain("border border-muted-foreground/40");
    expect(SOURCE).toContain("pending: { icon: Circle");
  });

  test("labeled mode clamps glyph size to the first-line rail", () => {
    expect(SOURCE).toContain(
      'const glyphSize = visibleLabel ? "md" : (size ?? "md")',
    );
  });

  test("iconOnly falls back when label is an empty string", () => {
    expect(SOURCE).toContain("label != null && label.length > 0");
    expect(SOURCE).toContain("customLabel ?? defaultLabel");
  });

  test("empty label falls back to the default caption (not nameless)", () => {
    expect(SOURCE).toContain(
      "const visibleLabel = iconOnly ? undefined : (customLabel ?? defaultLabel)",
    );
  });

  test("pins glyph size on the Icon so ancestor menus cannot enlarge it", () => {
    expect(SOURCE).toContain("GLYPH_SIZE_CLASS");
    expect(SOURCE).toContain("overflow-hidden");
    expect(SOURCE).toContain('sm: "size-4"');
    expect(SOURCE).toContain('sm: "size-2.5"');
    expect(SOURCE).not.toContain('sm: "size-4 [&>svg]:size-2.5"');
  });
});
