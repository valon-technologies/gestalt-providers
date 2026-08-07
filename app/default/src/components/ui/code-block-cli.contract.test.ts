import { describe, expect, test } from "vitest";
import { isValidElement, type ReactNode } from "react";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import {
  highlightCodeToLines,
  resolveLanguage,
} from "@/components/ui/code-block";
import {
  codeLineEmphasisEdgeClass,
  codeLineEmphasisRowClass,
  codeLineEmphasisRowClassName,
  codeLineEmphasisWashClass,
  codeLineRowBleedClass,
} from "@/components/ui/code-fence";

const DIR = dirname(fileURLToPath(import.meta.url));
const CLI_SOURCE = readFileSync(join(DIR, "code-block-cli-language.ts"), "utf8");

function collectHljsClasses(node: ReactNode): string[] {
  const out: string[] = [];
  const walk = (value: unknown) => {
    if (value == null || typeof value === "boolean") return;
    if (Array.isArray(value)) {
      for (const child of value) walk(child);
      return;
    }
    if (!isValidElement(value)) return;
    const className = (value.props as { className?: string }).className;
    if (typeof className === "string" && className.includes("hljs-")) {
      out.push(className);
    }
    walk((value.props as { children?: unknown }).children);
  };
  walk(node);
  return out;
}

describe("code-block language aliases (PR #4089)", () => {
  test("maps sh/shell onto cli; keeps bash as bash", () => {
    expect(resolveLanguage("sh")).toBe("cli");
    expect(resolveLanguage("shell")).toBe("cli");
    expect(resolveLanguage("cli")).toBe("cli");
    expect(resolveLanguage("bash")).toBe("bash");
  });
});

describe("code-block cli grammar (PR #4089)", () => {
  test("commands map to keyword, not built_in", () => {
    expect(CLI_SOURCE).toContain('className: "keyword"');
    expect(CLI_SOURCE).not.toContain('className: "built_in"');
  });

  test("tokenizes plain CLI commands that bash leaves uncolored", () => {
    const classes = highlightCodeToLines(
      "gestalt apps list --role viewer",
      "cli",
    ).flatMap(collectHljsClasses);
    expect(classes).toContain("hljs-keyword");
    expect(classes).toContain("hljs-attr");
  });

  test("does not paint hyphens inside positional args as flags", () => {
    const classes = highlightCodeToLines(
      "gestalt apps get my-app-name us-east-1",
      "cli",
    ).flatMap(collectHljsClasses);
    expect(classes).toContain("hljs-keyword");
    expect(classes).not.toContain("hljs-attr");
  });

  test("does not split on bare ampersands in redirects", () => {
    const classes = highlightCodeToLines("curl -o out 2>&1", "cli").flatMap(
      collectHljsClasses,
    );
    expect(classes).toContain("hljs-keyword");
    expect(classes).toContain("hljs-attr");
    expect(classes.filter((c) => c === "hljs-keyword")).toEqual(["hljs-keyword"]);
  });
});

describe("code-fence line emphasis (PR #4089)", () => {
  test("uses inset accent-solid edge and shared bleed, not border-l shift", () => {
    expect(codeLineEmphasisWashClass).toBe("bg-code-line-emphasis");
    expect(codeLineRowBleedClass).toBe("-mx-4 px-4");
    expect(codeLineEmphasisEdgeClass).toContain("inset_2px");
    expect(codeLineEmphasisEdgeClass).toContain("accent-solid");
    expect(codeLineEmphasisRowClassName).toBe(
      `${codeLineEmphasisEdgeClass} ${codeLineEmphasisWashClass}`,
    );
    expect(codeLineEmphasisRowClassName).not.toContain("border-l");
    expect(codeLineEmphasisRowClass()).toBe(
      `${codeLineEmphasisRowClassName} ${codeLineRowBleedClass}`,
    );
  });
});
